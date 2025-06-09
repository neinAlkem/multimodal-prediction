PROJECT_ID = project-big-data-461104

PARTICIPANT_INFO_GCS = gs://project-abd/raw/participant_class_info/
SURVEY_GCS = gs://project-abd/raw/survey/student_survey.csv
CLASS_TABLE_GCS = gs://project-abd/raw/participant_class_info/
WEARABLE_GCS = gs://project-abd/raw/participant_class_info/
FUSION_OUTPUT_GCS = gs://project-abd/pipeline/fused_data.gcs

FUSION_LOCAL_OUTPUT = fused_data.csv

TRAIN_OUTPUT_GCS = gs://project-abd/pipeline/train_data.csv
TEST_OUTPUT_GCS = gs://project-abd/pipeline/test_data.csv

MODEL_OUTPUT_DIR_GCS = gs://project-abd/pipeline/
METRICS_OUTPUT_GCS = gs://your-bucket/pipeline/

# Targets
.PHONY: all inital fusion data_prep train_model clean down

all: initial fusion data_prep train_model down

initial:
	@echo "Preparing Envirovment..."
	docker compose build --no-cache
	make down && docker compose up --scale spark-worker=3
	pip install -r requirements.txt --break-system-packages
	pip install -r model/requirements.txt --break-system-packages

fusion:
	@echo "Running fusion step..."
	pip install -r requirements.txt --break-system-packages
	python3 scripts/data_fusion.py $(PROJECT_ID) $(PARTICIPANT_INFO_GCS) $(SURVEY_GCS) $(CLASS_TABLE_GCS) $(WEARABLE_GCS) $(FUSION_OUTPUT_GCS)

data_prep:
	@echo "Running data preparation step..."
	python3 scripts/data_prep.py $(PROJECT_ID) $(FUSION_OUTPUT_GCS) $(TRAIN_OUTPUT_GCS) $(TEST_OUTPUT_GCS)

train_model:
	@echo "Running model training and evaluation..."
	python3 model/scripts.py \
		--model_output_dir $(MODEL_OUTPUT_DIR_GCS) \
		--metrics_output_path $(METRICS_OUTPUT_GCS) \
		--train_data_path $(TRAIN_OUTPUT_GCS) \
		--test_data_path $(TEST_OUTPUT_GCS)

clean:
	@echo "Cleaning local files..."
	rm -f fused_data.csv model.cbm evaluation_metrics.json

down:
	docker compose down --volumes
