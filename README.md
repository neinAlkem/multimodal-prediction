# A Multimodal Predictive Model of Students’ Affective Responses: Integrating Wearable Sensor Data and Psychological Questionnaires
Mengintegrasikan data dari sensor wearable dan kuesioner psikologis untuk memprediksi respons afektif siswa secara multimodal menggunakan pipeline berbasis Docker, Spark, dan Google Cloud Platform.

## Prasyarat
Sebelum menjalankan pipeline, harap diperhatikan beberapa hal dibawah ini :
1. **Docker** : Terinstal dan berjalan di mesin lokal atau server.
2. **Google Cloud SDK (gcloud)** : Terinstal dan sudah diautentikasi di mesin lokal atau Cloud Shell.
3. **Service Account GCP** : File JSON kredensial sudah tersedia di folder credentials/ dan dikonfigurasi di pipeline. ( Catatan: Akun ini akan dihapus setelah presentasi selesai. )
4. **Google Cloud Storage** : Struktur folder dan bucket sudah disiapkan dan sesuai dengan konfigurasi pipeline.

## Detail Direktori
- **conf/** : konfig untuk spark
- **credentials/** : berisikan file services account GCP
- **data/** : penyimpanan mounted dari spark image
- **model/** : file untuk membangun model registry pada GCP
- **notebook/** : gabungan notebook untuk proses EDA serta tes model
- **spark_apps/** : penyimpanan mounted untuk script spark
- **terraform/** : optional untuk membangun infrastruktur GCP

## Langkah Memulai
`Buat virtual envirovment untuk model depedencies`
```Bash
python -m venv myenv
```

`Eksekusi Pipeline`
```Bash
make all
```
