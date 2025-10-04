from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
from etl.fetch_and_upload import main as fetch_and_upload_main

DEFAULT_ARGS = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

BATCH_SIZE = int(Variable.get("BATCH_SIZE", 50000))
TOTAL_ROWS = int(Variable.get("TOTAL_ROWS", 106219500))
N_PARTS    = (TOTAL_ROWS + BATCH_SIZE - 1) // BATCH_SIZE

with DAG(
    dag_id='ingest_cdc_raw',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    max_active_tasks=20,
) as dag:

    fetch_tasks = []
    for i in range(N_PARTS):
        offset = i * BATCH_SIZE
        t = PythonOperator(
            task_id=f'fetch_chunk_{offset}',
            python_callable=fetch_and_upload_main,
            op_args=[offset],
)
        fetch_tasks.append(t)
    
    PROJECT_ID = Variable.get("GCP_PROJECT")
    REGION     = Variable.get("GCP_REGION", "us-east1")
    CLUSTER    = Variable.get("DATAPROC_CLUSTER", "airflow-dataproc")
    RAW_BUCKET = Variable.get("RAW_BUCKET", "raw-data-landing")
    PARQ_BUCKET= Variable.get("PARQUET_BUCKET", RAW_BUCKET)

    compact = DataprocSubmitJobOperator(
        task_id="compact_to_parquet",
        project_id= PROJECT_ID,
        region= REGION,
        gcp_conn_id="google_cloud_default",
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": "gs://<your-code-bucket>/compact_to_parquet.py",
                "args": [
                    "gs://{RAW_BUCKET}/covid/parts/date={{ ds }}/*",
                    "gs://{PARQ_BUCKET}/covid/parquet/date={{ ds }}"
                ],
            },
        },
    )


fetch_tasks >> compact

    
