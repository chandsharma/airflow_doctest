from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Define SparkApplication spec
spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "dag-count-rows",  # No underscores allowed here
        "namespace": "default"
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "vishallsinghh/spark-count-job:latest",  # Replace with your image
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///app/spark_job_count_rows.py",  # Inside Docker image
        "sparkVersion": "3.1.2",
        "restartPolicy": {
            "type": "OnFailure",
            "onFailureRetries": 3,
            "onFailureRetryInterval": 10,
            "onSubmissionFailureRetries": 3,
            "onSubmissionFailureRetryInterval": 10
        },
        "sparkConf": {
            "spark.eventLog.enabled": "false",
            "spark.hadoop.fs.azure.account.key.vishallsparklogs.blob.core.windows.net": "XZfQviaXeNqQVHjTD6Cwg1VbiUK8YhDWqOSTDskYv5oFd4YzfajqGUHZBE3/2My1mw9hPXfeceYn+AStsFBh7A=="
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark",
            "labels": {
                "app": "spark"
            }
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "512m"
        }
    }
}

# Define DAG
dag = DAG(
    "dag_count_rows",  # DAG ID (can include underscores)
    default_args=default_args,
    description="Submit Spark job to count rows from Azure Blob",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
)

# Spark submit task
submit_job = SparkKubernetesOperator(
    task_id="submit_count_job",
    namespace="default",
    image="vishallsinghh/spark-count-job:latest",  # Redundant but fine
    template_spec=spec,
    get_logs=True,
    delete_on_termination=False,
    dag=dag,
)
