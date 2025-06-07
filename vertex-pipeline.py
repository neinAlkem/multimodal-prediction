from kfp import dsl
from kfp.dsl import importer
from kfp.compiler import Compiler
from google_cloud_pipeline_components.v1.dataproc import DataprocPySparkBatchOp
from google_cloud_pipeline_components.v1.custom_job import CustomTrainingJobOp
from google_cloud_pipeline_components.v1.model import ModelUploadOp
from google_cloud_pipeline_components.types import artifact_types
from google.cloud import aiplatform
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------- #
#                                 Cloud Config                                 #
# ---------------------------------------------------------------------------- #
PROJECT_ID=os.getenv("PROJECT_ID")
REGION = "asia-southeast2" 
GCS_BUCKET_NAME = "project-abd"
SERVICE_ACCOUNT=os.getenv("SERVICE_ACCOUNT")

# ---------------------------------------------------------------------------- #
#                                  Path Config                                 #
# ---------------------------------------------------------------------------- #
GCS_BUCKET="project-abd"
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
PIPELINE_ROOT = f"gs://{GCS_BUCKET}/pipeline_root/pipeline_{TIMESTAMP}"
PIPELINE_NAME = "ABD_PIPELINE"

# ---------------------------------------------------------------------------- #
#                               Data Fusion Phase                              #
# ---------------------------------------------------------------------------- #
FUSION_SCRIPT_GCS = f"gs://{GCS_BUCKET}/scripts/data_fusion_pyspark.py" 
PARTICIPANT_INFO_GCS_DIR=f"gs://{GCS_BUCKET}/raw/participant_class_info/"
SURVEY_GCS_DIR=f"gs://{GCS_BUCKET}/raw/survey/"
CLASS_TABLE_GCS_PATH=f"gs://{GCS_BUCKET}/raw/participant_class_info/"
WEARABLE_BASE_GCS_PATH=f"gs://{GCS_BUCKET}/raw/class_wearable_data/"
OUTPUT_GCS_PATH_TEST=f"gs://{GCS_BUCKET}/test-pipeline/fusion_data_{TIMESTAMP}.csv"

# ---------------------------------------------------------------------------- #
#                              Data Pre-processing                             #
# ---------------------------------------------------------------------------- #
PREP_SCRIPT_GCS = f"gs://{GCS_BUCKET}/scripts/data_prep.py"
PREP_DATA_GCS_DIR = OUTPUT_GCS_PATH_TEST
TRAIN_DATA_GCS = f"gs://{GCS_BUCKET}/test-pipeline/train_data.csv"
TEST_DATA_GCS = f"gs://{GCS_BUCKET}/test-pipeline/test_data.csv"

# ---------------------------------------------------------------------------- #
#                                   Modeling                                   #
# ---------------------------------------------------------------------------- #
MODEL_OUTPUT = f"gs://{GCS_BUCKET}/test-pipeline/"
TRAINING_IMAGE_URI = "asia-southeast1-docker.pkg.dev/project-big-data-461104/model-images/engagement-classifier-trainer:v1.0"

# ---------------------------------------------------------------------------- #
#                                Model Registry                                #
# ---------------------------------------------------------------------------- #
DISPLAY_NAME = f"engagement-classifier-{TIMESTAMP}"
MODEL_OUTPUT_URI = f"gs://{GCS_BUCKET}/test-pipeline/"
SERVING_CONTAINER_IMAGE_URI="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest"


@dsl.pipeline(
    name=PIPELINE_NAME,
    description="ABD Project Pipeline: Data Prep (Dataproc), Training (CatBoost+PSO), Register, Deploy.",
    pipeline_root=PIPELINE_ROOT,
)
def pipeline(
    project_id:str = PROJECT_ID,
    pipeline_region:str = REGION,
    dataproc_fusion:str = FUSION_SCRIPT_GCS,
    dataproc_prep:str = PREP_SCRIPT_GCS,
    input_participant_info_dir: str = PARTICIPANT_INFO_GCS_DIR,
    input_survey_dir: str = SURVEY_GCS_DIR,
    input_class_table_path: str = CLASS_TABLE_GCS_PATH,
    input_wearable_base_dir: str = WEARABLE_BASE_GCS_PATH,
    output_fused_data_uri: str = OUTPUT_GCS_PATH_TEST,
    service_account: str = SERVICE_ACCOUNT,
    train_data_dir:str = TRAIN_DATA_GCS,
    test_data_dir:str = TEST_DATA_GCS,
    model_output_dir:str = MODEL_OUTPUT,
    training_image_uri:str = TRAINING_IMAGE_URI,
    display_name:str = DISPLAY_NAME,
    model_output_uri:str = MODEL_OUTPUT_URI,
    serving_container_uri:str = SERVING_CONTAINER_IMAGE_URI
):
    # ----------------------- Dataset Fusion With Dataproc ----------------------- #
    dataproc_fusion_job = DataprocPySparkBatchOp(
        project=project_id,
        location=pipeline_region,
        main_python_file_uri=dataproc_fusion,
        args=[
            project_id,
            input_participant_info_dir,
            input_survey_dir,
            input_class_table_path,
            input_wearable_base_dir,
            output_fused_data_uri
        ],
        service_account=service_account
    ).set_display_name('Dataproc - Data Fusioning')

    # ----------------------- Dataset Preparation With Dataproc ----------------------- #
    dataproc_prep_job = DataprocPySparkBatchOp(
        project=project_id,
        location=pipeline_region,
        main_python_file_uri=dataproc_prep,
        args=[
            project_id,
            output_fused_data_uri,
            train_data_dir,
            test_data_dir,
        ],
        service_account=service_account
    ).after(dataproc_fusion_job)\
     .set_display_name('Dataproc - Data Preparation')
    
    # ----------------------- Model Training ----------------------- #
    training_op = CustomTrainingJobOp(
        project=project_id,
        location=pipeline_region,
        display_name=f"Model_{TIMESTAMP}",
        worker_pool_specs=[{
                "machine_spec": {
                    "machine_type": "n1-standard-8",
                },
                "replica_count": 1,
                "container_spec": {
                    "image_uri": training_image_uri,
                    "args": [
                        train_data_dir,
                        test_data_dir,
                        model_output_dir
                    ]
                }
            }],
        service_account=service_account
    ).after(dataproc_prep_job)

    # ------------------------ Import Model Artifact ------------------------ #
    # Import the model as UnmanagedContainerModel
    model_importer = importer(
        artifact_uri=model_output_uri,
        artifact_class=artifact_types.UnmanagedContainerModel,
        metadata={
            'containerSpec': {
                'imageUri': serving_container_uri
            }
        }
    ).after(training_op)

    # ----------------------- Model Registry ----------------------- #
    upload_op = ModelUploadOp(
        project=project_id,
        location=pipeline_region,
        display_name=f"Model_{TIMESTAMP}",
        unmanaged_container_model=model_importer.outputs['artifact']
    ).after(model_importer)

# ----------------------------- Compile Pipeline ----------------------------- #
PIPELINE_JSON_FILE = f"{PIPELINE_NAME}_spec.json"

if __name__ == '__main__':
    Compiler().compile(
        pipeline_func=pipeline,
        package_path=PIPELINE_JSON_FILE,
    )
    
    aiplatform.init(
        project=PROJECT_ID, 
        location=REGION, 
        staging_bucket=f"gs://{GCS_BUCKET}/staging_vertex_sdk_full"
    )

    pipeline_run = aiplatform.PipelineJob(
        display_name=PIPELINE_NAME + "_run_" + TIMESTAMP,
        template_path=PIPELINE_JSON_FILE,
        pipeline_root=PIPELINE_ROOT,
        enable_caching=False, 
    )
    pipeline_run.submit()
    print(f"Submitting Vertex AI Pipeline job: {pipeline_run.display_name}...")
    print(f"Pipeline job submitted. Please check the Vertex AI Pipelines UI.")