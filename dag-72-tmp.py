from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
# from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import subprocess
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

spec = {
  "apiVersion": "sparkoperator.k8s.io/v1beta2",
  "kind": "SparkApplication",
  "metadata": {
    "name": "dag-72-pod",
    "namespace": "default"
  },
  "spec": {
    "type": "Scala",
    "mode": "cluster",
    "image": "docker.io/channnuu/chandan_spark:3.5.2",
    "imagePullPolicy": "IfNotPresent",
    "mainClass": "org.apache.spark.examples.SparkPi",
    "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.2.jar",
    "sparkVersion": "3.1.2",
    "restartPolicy": {
      "type": "Never"
    },
    "sparkConf": {
      "spark.eventLog.enabled": "true",
      "spark.eventLog.dir": "wasb://spark-logs@pocdatabricksdoc.blob.core.windows.net/spark-logs",
      "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
      "spark.hadoop.fs.azure.account.key.pocdatabricksdoc.blob.core.windows.net": "jr63R2MPRqUvSDZ/EtX+sNu32kz2wPVVU9S3wJoXZCx6xNJYI+yesstPOVxoyMuWqX2RmKBZRlZg+AStdYa2HA=="
    },
    "driver": {
      "cores": 1,
      "memory": "512m",
      "serviceAccount": "spark",
      "labels": {
        "version": "3.1.2"
      },
      "env": [
        {
          "name": "SPARK_DRIVER_MEMORY",
          "value": "512m"
        }
      ]
    },
    "executor": {
      "cores": 1,
      "instances": 2,
      "memory": "512m"
    }
  }
}

dag = DAG(
    "dag-72-pod", #same name as application. and there should be no underscore in application name
    default_args=default_args,
    description="Submit Spark job to Kubernetes via Airflow",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

submit_spark_job = SparkKubernetesOperator(
    task_id="submit-dag-72-pod", #NOT but in blog
    executor_config={
        "env": {
            "KUBERNETES_OPERATOR_LOG_LEVEL": "DEBUG",
        },
    },
    name="dag-72-pod-spark-job", #ok
    namespace="default", #ok
    image="bitnami/kubectl:latest", #ok
    template_spec=spec,
    do_xcom_push=True,
    get_logs=True, #ok
    delete_on_termination=False, #new
    dag=dag,
    # cmds=["kubectl", "apply", "-f", "https://vishalsparklogs.blob.core.windows.net/spark-logs/yaml/sparktest8.yaml"],
    # is_delete_operator_pod=False,
)
