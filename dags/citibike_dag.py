import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

# Mengambil konfigurasi dari .env
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER_PREFIX = os.getenv("GCS_FOLDER_PREFIX", "citibike_output")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_STAGING = os.getenv("DATASET_STAGING")
DATASET_INTERMEDIATE = os.getenv("DATASET_INTERMEDIATE")
DATASET_WAREHOUSE = os.getenv("DATASET_WAREHOUSE")
LOCATION = "asia-southeast2"

DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt_runner"

def task_failure_alert(context):
    """Callback custom: kirim pesan ke Slack pas task gagal."""
    task_instance = context.get('task_instance')
    exception = context.get('exception')

    message = (
        f":red_circle: *Task Gagal*\n"
        f"*DAG:* {task_instance.dag_id}\n"
        f"*Task:* {task_instance.task_id}\n"
        f"*Data Period:* {context.get('data_interval_start')} — {context.get('data_interval_end')}\n"
        f"*Failed At:* {task_instance.end_date}\n"
        f"*Try:* {task_instance.try_number}\n"
        f"*Alasan Gagal:* ```{exception}```\n"
        f"*Log:* {task_instance.log_url}"
    )

    hook = SlackWebhookHook(slack_webhook_conn_id="slack_default")
    hook.send(text=message)

# Konfigurasi default arguments untuk DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_alert,
}

# Mendefinisikan DAG
with DAG(
    'citibike_pipeline_dag',
    default_args=default_args,
    description='Pipeline CitiBike Batch Bulanan',
    schedule_interval='@monthly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
        
    # Task 01: Memastikan GCS Bucket
    create_gcs_bucket = GCSCreateBucketOperator(
        task_id='create_gcs_bucket',
        bucket_name=GCS_BUCKET_NAME,
        project_id=PROJECT_ID,
        storage_class="STANDARD",
        location=LOCATION,
    )

    # Task 02: Memastikan Dataset Staging di BigQuery
    create_dataset_staging = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset_staging',
        dataset_id=DATASET_STAGING,
        project_id=PROJECT_ID,
        location=LOCATION,
        if_exists="ignore",
    )

    # Task 03: Memastikan Dataset Intermediate di BigQuery
    create_dataset_intermediate = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset_intermediate',
        dataset_id=DATASET_INTERMEDIATE,
        project_id=PROJECT_ID,
        location=LOCATION,
        if_exists="ignore",
    )

    # Task 04: Memastikan Dataset Warehouse di BigQuery
    create_dataset_warehouse = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset_warehouse',
        dataset_id=DATASET_WAREHOUSE,
        project_id=PROJECT_ID,
        location=LOCATION,
        if_exists="ignore",
    )    
    # Task 05: Menjalankan script Extract ke Local dan Load sampai ke Bigquery
    citibike_to_bigquery = BashOperator(
        task_id='citibike_to_bigquery',
        bash_command='python /opt/airflow/scripts/batch/pipeline_batch.py',
    )

    # Task 06: Menjalankan dbt run untuk membersihkan data di staging
    dbt_run_stg_citibike_trips = BashOperator(
        task_id="dbt_run_stg_citibike_trips",
        bash_command=(
            f"{DBT_BIN} run --select stg_citibike_trips "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    # Task 07: Menjalankan dbt run untuk memproses data ke dataset intermediate    
    dbt_run_int_citibike_trips = BashOperator(
        task_id="dbt_run_int_citibike_trips",
        bash_command=(
            f"{DBT_BIN} run --select int_citibike_trips "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    # Task 08: Menjalankan dbt run untuk memproses data ke dataset warehouse
    dbt_run_fct_citibike_trips_enriched = BashOperator(
        task_id="dbt_run_fct_citibike_trips_enriched",
        bash_command=(
            f"{DBT_BIN} run --select fct_citibike_trips_enriched "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    dbt_run_fct_station_hourly_traffic = BashOperator(
        task_id="dbt_run_fct_station_hourly_traffic",
        bash_command=(
            f"{DBT_BIN} run --select fct_station_hourly_traffic "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    # Task 09: Menjalankan dbt test pada stg_citibike_trips di staging
    dbt_test_stg_citibike_trips = BashOperator(
        task_id="dbt_test_stg_citibike_trips",
        bash_command=(
            f"{DBT_BIN} test --select stg_citibike_trips "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    # Task 10: Menjalankan dbt test pada int_citibike_trips di intermediate
    dbt_test_int_citibike_trips = BashOperator(
        task_id="dbt_test_int_citibike_trips",
        bash_command=(
            f"{DBT_BIN} test --select int_citibike_trips "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    # Task 11: Menjalankan dbt test pada view di warehouse
    dbt_test_fct_citibike_trips_enriched = BashOperator(
        task_id="dbt_test_fct_citibike_trips_enriched",
        bash_command=(
            f"{DBT_BIN} test --select fct_citibike_trips_enriched "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test_fct_station_hourly_traffic = BashOperator(
        task_id="dbt_test_fct_station_hourly_traffic",
        bash_command=(
            f"{DBT_BIN} test --select fct_station_hourly_traffic "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    # Task 12: Task untuk test fail dan alerting ke Slack
    test_fail_task = BashOperator(
        task_id='test_fail_task',
        bash_command='exit 1',
    )
    
    [create_gcs_bucket, create_dataset_staging, create_dataset_intermediate, create_dataset_warehouse] >> citibike_to_bigquery
    citibike_to_bigquery >> dbt_run_stg_citibike_trips >> dbt_run_int_citibike_trips
    dbt_run_int_citibike_trips >> dbt_run_fct_citibike_trips_enriched >> dbt_run_fct_station_hourly_traffic
    dbt_run_fct_station_hourly_traffic >> dbt_test_stg_citibike_trips >> dbt_test_int_citibike_trips
    dbt_test_int_citibike_trips >> dbt_test_fct_citibike_trips_enriched >> dbt_test_fct_station_hourly_traffic
    dbt_test_fct_station_hourly_traffic >> test_fail_task