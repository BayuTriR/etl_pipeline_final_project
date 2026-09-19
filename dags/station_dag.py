import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

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
    'station_dag',
    default_args=default_args,
    description='Pipeline GBFS Station Information & Station Status Harian',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    # Task 01: Menjalankan script Extract ke Local dan Load sampai ke Bigquery
    gbfs_to_bigquery = BashOperator(
        task_id='gbfs_to_bigquery',
        bash_command='python /opt/airflow/scripts/batch/pipeline_gbfs.py',
    )

    # Task 02: Menjalankan dbt run untuk membersihkan data di staging
    dbt_run_stg_station_information = BashOperator(
        task_id="dbt_run_stg_station_information",
        bash_command=(
            f"{DBT_BIN} run --select stg_station_information "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    dbt_run_stg_station_status = BashOperator(
        task_id="dbt_run_stg_station_status",
        bash_command=(
            f"{DBT_BIN} run --select stg_station_status_streaming "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    # Task 03: Menjalankan dbt run untuk memproses data ke dataset warehouse
    dbt_run_dim_station = BashOperator(
        task_id="dbt_run_dim_station",
        bash_command=(
            f"{DBT_BIN} run --select dim_station "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    dbt_run_fct_station_status_current  = BashOperator(
        task_id="dbt_run_fct_station_status_current",
        bash_command=(
            f"{DBT_BIN} run --select fct_station_status_current  "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    dbt_run_fct_station_demand_vs_supply  = BashOperator(
        task_id="dbt_run_fct_station_demand_vs_supply",
        bash_command=(
            f"{DBT_BIN} run --select fct_station_demand_vs_supply  "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )
    
    # Task 04: Menjalankan dbt test di staging
    dbt_test_stg_station_information = BashOperator(
        task_id="dbt_test_stg_station_information",
        bash_command=(
            f"{DBT_BIN} test --select stg_station_information "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    dbt_test_stg_station_status = BashOperator(
        task_id="dbt_test_stg_station_status",
        bash_command=(
            f"{DBT_BIN} test --select stg_station_status_streaming "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
        ),
    )

    # Task 05: Menjalankan dbt test di warehouse
    dbt_test_dim_station = BashOperator(
        task_id="dbt_test_dim_station",
        bash_command=(
            f"{DBT_BIN} test --select dim_station "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test_fct_station_status_current = BashOperator(
        task_id="dbt_test_fct_station_status_current",
        bash_command=(
            f"{DBT_BIN} test --select fct_station_status_current "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test_fct_station_demand_vs_supply = BashOperator(
        task_id="dbt_test_fct_station_demand_vs_supply",
        bash_command=(
            f"{DBT_BIN} test --select fct_station_demand_vs_supply "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    gbfs_to_bigquery >> dbt_run_stg_station_information >> dbt_run_stg_station_status
    dbt_run_stg_station_status >> dbt_run_dim_station >> dbt_run_fct_station_status_current
    dbt_run_fct_station_status_current >> dbt_run_fct_station_demand_vs_supply >> dbt_test_stg_station_information
    dbt_test_stg_station_information >> dbt_test_stg_station_status >> dbt_test_dim_station
    dbt_test_dim_station >> dbt_test_fct_station_status_current >> dbt_test_fct_station_demand_vs_supply