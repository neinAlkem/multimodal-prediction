# A Multimodal Predictive Model of Students’ Affective Responses: Integrating Wearable Sensor Data and Psychological Questionnaires


## Prasyarat
Sebelum memulai, pastikan Anda memiliki:
1.  Akun Google Cloud dengan proyek yang sudah dibuat dan **billing diaktifkan** (diperlukan bahkan untuk akun trial).
2.  Google Cloud SDK (`gcloud`) terinstal dan terautentikasi di komputer lokal atau Cloud Shell.
3.  Docker terinstal dan berjalan di lingkungan tempat Anda akan membangun image.
4.  Sebuah GCS Bucket yang kosong untuk menyimpan data mentah dan output pipeline.

## Langkah-langkah Penyiapan dan Eksekusi

Ikuti langkah-langkah ini secara berurutan untuk menjalankan pipeline dari awal hingga akhir.

### Fase 1: Konfigurasi Proyek

1.  **Kloning Repositori**:
    ```bash
    git clone [URL_REPOSITORI_ANDA]
    cd student-engagement-pipeline
    ```

2.  **Edit File Konfigurasi**:
    Buka file-file berikut dan ganti placeholder `project_id` dan `bucket_name` dengan nilai yang sesuai:
    * `pipelines/main_pipeline.py`
    * Semua file `component.yaml` di dalam direktori `components/`

### Fase 2: Membangun Infrastruktur Cloud

Jalankan perintah ini dari **Cloud Shell** atau **terminal lokal Anda**.

1.  **Buat Cluster GKE**:
    ```bash
    export CLUSTER_NAME="kfp-lean-cluster"
    export CLUSTER_ZONE="asia-southeast1-b"
    gcloud container clusters create $CLUSTER_NAME \
      --zone $CLUSTER_ZONE \
      --machine-type "e2-standard-2" \
      --num-nodes "1" \
      --scopes "[https://www.googleapis.com/auth/cloud-platform](https://www.googleapis.com/auth/cloud-platform)"
    ```
    *(Proses ini memakan waktu 5-10 menit)*

2.  **Instal Kubeflow Pipelines (KFP)**:
    ```bash
    gcloud container clusters get-credentials $CLUSTER_NAME --zone $CLUSTER_ZONE
    kubectl apply -k "[github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=2.0.5](https://github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=2.0.5)"
    kubectl wait --for condition=established --timeout=60s crd/applications.app.k8s.io
    kubectl apply -k "[github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic-pns?ref=2.0.5](https://github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic-pns?ref=2.0.5)"
    kubectl wait --for=condition=ready --timeout=300s -n kubeflow --all pods
    ```
    *(Proses ini juga memakan waktu sekitar 5 menit)*

### Fase 3: Menyiapkan Data & Docker Images

1.  **Unggah Data Mentah**: Unggah semua data mentah Anda ke GCS Bucket sesuai dengan path yang Anda tentukan di `main_pipeline.py` (misalnya: `gs://NAMA_BUCKET_ANDA/pipeline-root/raw/...`).

2.  **Berikan Izin IAM**: Pastikan akun yang akan menjalankan `docker push` memiliki peran IAM berikut:
    * `Artifact Registry Writer` (`roles/artifactregistry.writer`)
    * `Artifact Registry Repository Administrator` (`roles/artifactregistry.repoAdmin`) - atau buat repositori `gcr.io` secara manual.

3.  **Build dan Push Docker Images**: Jalankan skrip bantuan yang sudah disediakan.
    ```bash
    chmod +x scripts/build_images.sh
    ./scripts/build_images.sh
    ```

### Fase 4: Mengompilasi Pipeline

Jalankan perintah ini untuk mengubah skrip Python pipeline Anda menjadi file `YAML` yang bisa dibaca oleh Kubeflow.
```bash
python3 pipelines/main_pipeline.py