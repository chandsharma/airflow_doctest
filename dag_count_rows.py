from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "dag-count-orc-rows",
        "namespace": "default"
    },
    "spec": {
        "type": "Python",  # Important
        "mode": "cluster",
        "image": "docker.io/channnuu/chandan_spark:3.5.2",  # Your existing working image
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///opt/spark/scripts/count_orc_rows.py",  # Assumes script is present inside the image
        "sparkVersion": "3.1.2",
        "restartPolicy": {
            "type": "OnFailure",
            "onSubmissionFailureRetries": 3,
            "onSubmissionFailureRetryInterval": 10,
            "onFailureRetries": 3,
            "onFailureRetryInterval": 10
        },
        "sparkConf": {
            "spark.eventLog.enabled": "false",
            "spark.hadoop.fs.azure.account.key.vishalsparklogs.blob.core.windows.net": "XZfQviaXeNqQVHjTD6Cwg1VbiUK8YhDWqOSTDskYv5oFd4YzfajqGUHZBE3/2My1mw9hPXfeceYn+AStsFBh7A==",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
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

dag = DAG(
    "dag_count_orc_rows",  # DAG name can use underscores
    default_args=default_args,
    description="Count rows in ORC file using Spark on AKS",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
)

submit_job = SparkKubernetesOperator(
    task_id="submit_spark_orc_count",
    namespace="default",
    image="docker.io/channnuu/chandan_spark:3.5.2",
    template_spec=spec,
    get_logs=True,
    delete_on_termination=False,
    dag=dag,
)
