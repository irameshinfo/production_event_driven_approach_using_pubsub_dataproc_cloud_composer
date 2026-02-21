from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator
import json
import base64

PROJECT_ID = "ranjanrishi-project"
REGION = "us-central1"
CLUSTER_NAME = "my-dataproc-cluster"
SUBSCRIPTION = "gcs-file-trigger-sub"
CONFIG_PATH = "gs://rameshsamplebucket/config/config.json"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1)
}

def extract_file(**context):
    message = context['ti'].xcom_pull(task_ids='wait_for_file')[0]
    data = json.loads(base64.b64decode(message['message']['data']).decode())
    bucket = data['bucket']
    name = data['name']
    file_path = f"gs://{bucket}/{name}"
    context['ti'].xcom_push(key="file_path", value=file_path)

with DAG(
    dag_id="gcs_auto_trigger_dataproc_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    wait_for_file = PubSubPullSensor(
        task_id="wait_for_file",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION,
        max_messages=1,
        ack_messages=True
    )

    extract = PythonOperator(
        task_id="extract_file_path",
        python_callable=extract_file
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        region=REGION,
        project_id=PROJECT_ID,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri":
                    "gs://rameshsamplebucket/scripts/pyspark_pubsub_job.py",
                "args": [
                    CONFIG_PATH,
                    "{{ ti.xcom_pull(key='file_path') }}"
                ]
            }
        }
    )

    wait_for_file >> extract >> submit_job