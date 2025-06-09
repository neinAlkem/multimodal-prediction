#!/bin/sh

export PROJECT_ID=$(gcloud config get-value project)
docker build -t gcr.io/$PROJECT_ID/spark-runner:latest -f build/Dockerfile.spark .

docker build -t gcr.io/$PROJECT_ID/train-runner:latest -f build/Dockerfile.train .
docker push gcr.io/$PROJECT_ID/spark-runner:latest
docker push gcr.io/$PROJECT_ID/train-runner:latest