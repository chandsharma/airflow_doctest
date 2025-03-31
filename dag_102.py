from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

spark_app_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "fetch-alice-orc",
        "namespace": "default"
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "yourdockerusername/fetch-alice-orc:latest",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///opt/spark/app/script/fetch_alice.py",
        "sparkVersion": "3.1.2",
        "restartPolicy": {
            "type": "OnFailure",
            "onSubmissionFailureRetries": 3,
            "onSubmissionFailureRetryInterval": 10
        },
        "sparkConf": {
            "spark.eventLog.enabled": "false",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark",
            "env": [
                {"name": "AZURE_STORAGE_ACCOUNT_NAME", "value": "myazurestorage"},
                {"name": "AZURE_STORAGE_ACCOUNT_KEY", "value": "<Your Storage Key>"}
            ]
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "512m",
            "env": [
                {
                    "name": "AZURE_STORAGE_ACCOUNT_NAME",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": "azure-secret",
                            "key": "AZURE_STORAGE_ACCOUNT_NAME"
                        }
                    }
                },
                {
                    "name": "AZURE_STORAGE_ACCOUNT_KEY",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": "azure-secret",
                            "key": "AZURE_STORAGE_ACCOUNT_KEY"
                        }
                    }
                }
            ]

        }
    }
}

dag = DAG(
    "fetch_alice_orc_dag",
    default_args=default_args,
    description="Fetch Alice from ORC securely using Spark Kubernetes",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit_fetch_alice_orc",
    namespace="default",
    image="vishallsinghh/fetch-alice-orc:latest",
    template_spec=spark_app_spec,
    get_logs=True,
    delete_on_termination=False,
    dag=dag
)
