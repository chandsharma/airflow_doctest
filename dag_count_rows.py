from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
    "dag_count_orc_rows_yaml",
    default_args=default_args,
    description="Run Spark job on AKS to count ORC rows via YAML",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Step 1: Create ConfigMap for inline PySpark script
create_configmap = KubernetesPodOperator(
    task_id="create_orc_script_configmap",
    name="create-configmap-orc-script",
    namespace="default",
    image="bitnami/kubectl:latest",
    cmds=[
        "kubectl",
        "apply",
        "-f",
        "https://vishalsparklogs.blob.core.windows.net/orc-data-container/yaml/orc-count-script.yaml"
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Step 2: Submit SparkApplication YAML
submit_spark_job = KubernetesPodOperator(
    task_id="submit_spark_orc_count",
    name="submit-spark-orc-job",
    namespace="default",
    image="bitnami/kubectl:latest",
    cmds=[
        "kubectl",
        "apply",
        "-f",
        "https://vishalsparklogs.blob.core.windows.net/orc-data-container/yaml/orc-count-sparkapp.yaml"
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Ensure configmap is created before Spark job runs
create_configmap >> submit_spark_job
