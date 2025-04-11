from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# SparkApplication Spec
spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "dag-count-rows",
        "namespace": "default"
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "vishallsinghh/spark-count-job:latest",  # Use your pushed image
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///app/spark_job_count_rows.py",
        "sparkVersion": "3.1.2",
        "restartPolicy": {
            "type": "OnFailure"
        },
        "sparkConf": {
            "spark.hadoop.fs.azure.account.key.vishallsparklogs.blob.core.windows.net": "XZfQviaXeNqQVHjTD6Cwg1VbiUK8YhDWqOSTDskYv5oFd4YzfajqGUHZBE3/2My1mw9hPXfeceYn+AStsFBh7A==",
            "spark.eventLog.enabled": "false"
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark"
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "512m"
        }
    }
}

dag = DAG(
    "dag_count_rows",
    default_args=default_args,
    description="Spark count rows from blob",
    catchup=False,
    start_date=datetime(2024, 1, 1)
)

submit_job = SparkKubernetesOperator(
    task_id="submit_count_job",
    namespace="default",
    image="vishallsinghh/spark-count-job:latest",  # Redundant but allowed
    template_spec=spec,
    get_logs=True,
    delete_on_termination=False,
    dag=dag,
)
