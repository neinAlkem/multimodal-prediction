# A Multimodal Predictive Model of Students’ Affective Responses: Integrating Wearable Sensor Data and Psychological Questionnaires

## Prasyarat
Sebelum menjalankan pipeline, harap diperhatikan beberapa hal dibawah ini :
1.  Docker terinstal dan berjalan di lingkungan tempat Anda akan membangun image.
2.  Google Cloud SDK (`gcloud`) terinstal dan terautentikasi di komputer lokal atau Cloud Shell.
3.  Akun Google Cloud Service akan menggunakan Service Account yang sudah di atur pada pipeline (Setelah presentasi selesai, Service Account akan dihapus.)
4.  Path penyimpanan telah diatur sedemikian lupa pada layanan Google Cloud Storage, sehingga tidak perlu melakukan proses tambahan apa-apa.

## Detail Direktori
- conf : konfig untuk spark
- credentials : berisikan file services account GCP
- data : penyimpanan mounted dari spark image
- model : file untuk membangun model registry pada GCP
- notebook : gabungan notebook untuk proses EDA serta tes model
- spark_apps : penyimpanan mounted untuk script spark
- terraform : optional untuk membangun infrastruktur GCP

## Langkah Memulai
`Buat virtual envirovment untuk model depedencies`
```Bash
python -m venv myenv
```

`Eksekusi Pipeline`
```Bash
make all
```
