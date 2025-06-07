export PROJECT_ID="project-big-data-461104"
export REGION="asia-southeast1"
export REPO_NAME="model-images"
export IMAGE_NAME="engagement-classifier-trainer"
export IMAGE_TAG="v1.0"
export IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}"

gcloud builds submit --tag ${IMAGE_URI} .
