from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.base import BaseSensorOperator
from kubernetes import client, config
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dag_count_orc_rows_spark_operator",
    default_args=default_args,
    description="Run Spark job using SparkKubernetesOperator to count ORC rows",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_orc_count",
    namespace="default",
    application_file="https://vishalsparklogs.blob.core.windows.net/orc-data-container/yaml/spark_orc_count.yaml",
    get_logs=True,
    dag=dag
)
