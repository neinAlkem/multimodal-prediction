from kfp.v2 import dsl
from kfp.v2.compiler import Compiler
from google_cloud_pipeline_components.v1.dataproc import DataprocPySparkBatchOp
from google.cloud import aiplatform
import os
from datetime import datetime
import os
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
PYSPARK_SCRIPT_GCS = f"{GCS_BUCKET}/scripts/data_fusion_pyspark.py" # Pastikan ini ada
PARTICIPANT_INFO_GCS_DIR="gs://${GCP_BUCKET}/raw/participant_class_info/"
SURVEY_GCS_DIR="gs://${GCP_BUCKET}/raw/survey/"
CLASS_TABLE_GCS_PATH="gs://${GCP_BUCKET}/raw/participant_class_info/"
WEARABLE_BASE_GCS_PATH="gs://${GCP_BUCKET}/raw/class_wearable_data/"
OUTPUT_GCS_PATH_TEST="gs://${GCP_BUCKET}/test-pipeline/fusion_data_{TIMESTAMP}.csv"

# ---------------------------------------------------------------------------- #
#                              Data Pre-processing                             #
# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                   Modeling                                   #
# ---------------------------------------------------------------------------- #

# ---------------------------------------------------------------------------- #
#                                Model Registry                                #
# ---------------------------------------------------------------------------- #

@dsl.pipeline(
    name=PIPELINE_NAME,
    description="ABD Project Pipeline: Data Prep (Dataproc), Training (CatBoost+PSO), Register, Deploy.",
    pipeline_root=PIPELINE_ROOT,
)

def pipeline(
    project_id:str = PROJECT_ID,
    pipeline_region:str = REGION,
    dataproc_region:str = REGION,
    pyspark_uri:str = PYSPARK_SCRIPT_GCS,
    input_participant_info_dir: str = PARTICIPANT_INFO_GCS_DIR,
    input_survey_dir: str = SURVEY_GCS_DIR,
    input_class_table_path: str = CLASS_TABLE_GCS_PATH,
    input_wearable_base_dir: str = WEARABLE_BASE_GCS_PATH,
    output_fused_data_uri: str = OUTPUT_GCS_PATH_TEST,
    dataproc_service_account: str = SERVICE_ACCOUNT,
):
# ----------------------- Dataset Fusion With Dataproc ----------------------- #
    dataproc_fusion_job = DataprocPySparkBatchOp(
        project=project_id,
        location=dataproc_region,
        main_python_file_uri=pyspark_uri,
        args=[
            project_id,
            input_participant_info_dir,
            input_survey_dir,
            input_class_table_path,
            input_wearable_base_dir,
            output_fused_data_uri
        ],
        service_account=dataproc_service_account
    ).set_display_name('Dataproc - Data Fusioning')
    
# ----------------------------- Compile Pipeline ----------------------------- #
PIPELINE_JSON_FILE = f"{PIPELINE_NAME}_spec.json"
if __name__ == '__main__':
    Compiler().compile(
        pipeline_func=pipeline,
        package_path=PIPELINE_JSON_FILE,
    )
    
    aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=f"g://{GCS_BUCKET}/staging_vertex_sdk_full")

    pipeline_run = aiplatform.PipelineJob(
        display_name=PIPELINE_NAME + "_run_" + TIMESTAMP,
        template_path=PIPELINE_JSON_FILE,
        pipeline_root=PIPELINE_ROOT,
        enable_caching=False, 
        parameter_values={
            # Anda bisa override parameter di sini, contoh:
            # "n_trials_hpo_param": 10, 
            # "deploy_model": False 
        }
    )
    
    print(f"Submitting Vertex AI Pipeline job: {pipeline_run.display_name}...")
    pipeline_run.submit()
    print(f"Pipeline job submitted. Please check the Vertex AI Pipelines UI.")
