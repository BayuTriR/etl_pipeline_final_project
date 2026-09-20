import os
from datetime import datetime, timezone
from pyspark.sql.functions import col
from google.cloud import storage
from google.cloud import bigquery

def upload_citibike_to_gcs_and_bq(df_transformed, unique_dates, cfg, batch_month):
    os.makedirs(cfg["output_dir"], exist_ok=True)

    storage_client = storage.Client()
    bucket = storage_client.bucket(cfg["gcs_bucket_name"])

    bq_client = bigquery.Client(project=cfg["project_id"])

    print(f"Ditemukan {len(unique_dates)} hari unik. Mulai mengekspor, mengunggah ke GCS, dan memuat ke BigQuery...")

    ingestion_ts = datetime.now(timezone.utc)

    for d_str in unique_dates:
        df_daily = df_transformed.filter(col("date_str") == d_str) \
            .drop("date_str")
        
        pdf_daily = df_daily.toPandas()
        pdf_daily.insert(0, "ingestion_at", ingestion_ts)

        batch_output_dir = os.path.join(cfg["output_dir"], batch_month)
        os.makedirs(batch_output_dir, exist_ok=True)

        file_name = f"citibike_trips_{d_str}.csv"
        local_file_path = os.path.join(batch_output_dir, file_name)
        pdf_daily.to_csv(local_file_path, index=False)

        # Upload ke GCS
        gcs_blob_path = f"{cfg['gcs_folder_prefix']}/{batch_month}/{file_name}"
        blob = bucket.blob(gcs_blob_path)
        blob.upload_from_filename(local_file_path)
        print(f"Berhasil mengunggah ke GCS: gs://{cfg['gcs_bucket_name']}/{gcs_blob_path}")

        # Load ke BigQuery
        table_name = "citibike_trips"
        table_id = f"{cfg['project_id']}.{cfg['dataset_staging']}.{table_name}${d_str}"
        gcs_uri = f"gs://{cfg['gcs_bucket_name']}/{gcs_blob_path}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="started_at",
            ),
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        print(f"Berhasil memuat data ke tabel BigQuery: {table_id}")

def upload_gbfs_to_gcs_and_bq(df_transformed, cfg):
    os.makedirs(cfg["output_station_info"], exist_ok=True)

    storage_client = storage.Client()
    bucket = storage_client.bucket(cfg["gcs_bucket_name"])
    bq_client = bigquery.Client(project=cfg["project_id"])

    local_file_path = os.path.join(cfg["output_station_info"], "station_information.csv")
    
    pdf = df_transformed.toPandas()
    pdf.to_csv(local_file_path, index=False)

    # Upload ke GCS
    gcs_blob_path = f"{cfg['gcs_folder_prefix']}/gbfs/station_information.csv"
    blob = bucket.blob(gcs_blob_path)
    blob.upload_from_filename(local_file_path)
    print(f"Berhasil mengunggah GBFS ke GCS: gs://{cfg['gcs_bucket_name']}/{gcs_blob_path}")

    # Load ke BigQuery 
    table_name = "gbfs_station_information"
    table_id = f"{cfg['project_id']}.{cfg['dataset_staging']}.{table_name}"
    gcs_uri = f"gs://{cfg['gcs_bucket_name']}/{gcs_blob_path}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()
    print(f"Berhasil memuat data GBFS ke tabel BigQuery: {table_id}")