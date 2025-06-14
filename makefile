PROJECT_ID = project-big-data-461104

PARTICIPANT_INFO_GCS = gs://project-abd/raw/participant_class_info/
SURVEY_GCS = gs://project-abd/raw/survey/
CLASS_TABLE_GCS = gs://project-abd/raw/participant_class_info/
WEARABLE_GCS = gs://project-abd/raw/class_wearable_data/
FUSION_OUTPUT_GCS = gs://project-abd/pipeline/fused_data.csv

TRAIN_OUTPUT_GCS = gs://project-abd/pipeline/train_data.csv
TEST_OUTPUT_GCS = gs://project-abd/pipeline/test_data.csv

MODEL_OUTPUT_DIR_GCS = gs://project-abd/pipeline/
METRICS_OUTPUT_GCS = gs://project-abd/pipeline/

.PHONY: all initial fusion data_prep train_model clean down

all: initial fusion data_prep train_model down

initial:
	@echo "Preparing Environment..."
	@set -e; \
	docker compose build; \
	make down; \
	docker compose up -d --scale spark-worker=3; \
	pip install -r requirements.txt --break-system-packages; \
	pip install -r model/requirements.txt --break-system-packages;

fusion:
	@echo "Running fusion step..."
	@set -e; \
	python3 scripts/data_fusion.py $(PROJECT_ID) $(PARTICIPANT_INFO_GCS) $(SURVEY_GCS) $(CLASS_TABLE_GCS) $(WEARABLE_GCS) $(FUSION_OUTPUT_GCS);

data_prep:
	@echo "Running data preparation step..."
	@set -e; \
	python3 scripts/data_prep.py $(PROJECT_ID) $(FUSION_OUTPUT_GCS) $(TRAIN_OUTPUT_GCS) $(TEST_OUTPUT_GCS);

train_model:
	@echo "Running model training and evaluation..."
	@set -e; \
	python3 model/script.py \
  	--train-data $(TRAIN_OUTPUT_GCS) \
  	--test-data $(TEST_OUTPUT_GCS) \
  	--model-output-dir $(MODEL_OUTPUT_DIR_GCS) \
  	--metric-output-dir $(METRICS_OUTPUT_GCS)

clean:
	@echo "Cleaning local files..."
	rm -f fused_data.csv model.cbm evaluation_metrics.json

down:
	@echo "Stopping and removing Docker containers and volumes..."
	docker compose down --volumes
