from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.base import BaseSensorOperator
from kubernetes import client, config
from datetime import datetime, timedelta

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Spark Application specification embedded directly into DAG
spark_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "orc-count-sparkapp",
        "namespace": "default"
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "apache/spark:3.5.1-python3",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "https://vishalsparklogs.blob.core.windows.net/orc-data-container/python_script/orc_count.py",
        "arguments": ["wasbs://orc-data-container@vishalsparklogs.blob.core.windows.net/dummy_data/parquet/"],
        "sparkVersion": "3.5.1",
        "restartPolicy": {
            "type": "OnFailure",
            "onFailureRetries": 3,
            "onFailureRetryInterval": 30
        },
        "sparkConf": {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "wasbs://spark-logs@vishalsparklogs.blob.core.windows.net/spark-logs",
            "spark.hadoop.fs.azure.account.key.vishalsparklogs.blob.core.windows.net": "XZfQviaXeNqQVHjTD6Cwg1VbiUK8YhDWqOSTDskYv5oFd4YzfajqGUHZBE3/2My1mw9hPXfeceYn+AStsFBh7A=="
        },
        "driver": {
            "cores": 1,
            "memory": "2g",
            "serviceAccount": "spark",
            "labels": {}
        },
        "executor": {
            "cores": 2,
            "instances": 3,
            "memory": "4g"
        }
    }
}

# Define DAG
dag = DAG(
    "dag_count_orc_rows_spark_operator",
    default_args=default_args,
    description="Run Spark job using SparkKubernetesOperator to count ORC rows",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Submit SparkApplication using SparkKubernetesOperator
submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_orc_count",
    namespace="default",
    application_file=spark_spec,
    get_logs=True,
    dag=dag
)

submit_spark_job
