export CLUSTER_NAME="kfp-lean-cluster"
export CLUSTER_ZONE="asia-southeast1-b"
echo "Creating cluster GKE, ..."

gcloud container clusters create $CLUSTER_NAME \
  --zone $CLUSTER_ZONE \
  --machine-type "e2-standard-2" \
  --num-nodes "1" \
  --scopes "https://www.googleapis.com/auth/cloud-platform"

echo "Cluster GKE Created!"

gcloud container clusters get-credentials $CLUSTER_NAME --zone $CLUSTER_ZONE
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=2.0.5"
kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic-pns?ref=2.0.5"

echo "Installing KFP..."
kubectl wait --for=condition=ready --timeout=300s -n kubeflow --all pods

echo "Cluster Ready to use"