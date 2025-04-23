from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.base import BaseSensorOperator
from kubernetes import client, config
from datetime import datetime

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Sensor to check Spark Application completion
class SparkAppSensor(BaseSensorOperator):
    def __init__(self, namespace, app_name, **kwargs):
        super().__init__(**kwargs)
        self.namespace = namespace
        self.app_name = app_name

    def poke(self, context):
        config.load_kube_config()
        custom_api = client.CustomObjectsApi()

        app = custom_api.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications",
            name=self.app_name
        )

        state = app["status"]["applicationState"]["state"]
        self.log.info(f"Spark Application '{self.app_name}' state: {state}")

        if state == "COMPLETED":
            self.log.info("Spark Job completed successfully.")
            return True
        elif state in ["FAILED", "UNKNOWN", "SUBMISSION_FAILED"]:
            raise Exception(f"Spark Job failed with state: {state}")
        return False

spark_app_name = "spark-orc-count"

# Spark Application specification
spark_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": spark_app_name,
        "namespace": "default"
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "apache/spark:3.5.1-python3",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "wasbs://scripts@vishalsparklogs.blob.core.windows.net/spark/orc_count.py",
        "arguments": ["wasbs://data@vishalsparklogs.blob.core.windows.net/path/to/orc/files"],
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
            "serviceAccount": "spark"
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
    "spark_orc_count_no_docker",
    default_args=default_args,
    description="Spark job to count ORC rows without custom Docker images",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Submit Spark Application
submit_spark_job = SparkKubernetesOperator(
    task_id="submit_spark_orc_count",
    namespace="default",
    image="apache/spark:3.5.1-python3",
    template_spec=spark_spec,
    get_logs=True,
    delete_on_termination=False,
    dag=dag
)

# Monitor Spark Application
monitor_spark_job = SparkAppSensor(
    task_id="monitor_spark_orc_count",
    namespace="default",
    app_name=spark_app_name,
    poke_interval=30,
    timeout=3600,
    dag=dag
)

submit_spark_job >> monitor_spark_job
