from airflow import DAG
from airflow.operators.python import PythonOperator
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

dag = DAG(
    dag_id="dag_submit_spark_yaml",
    default_args=default_args,
    description="Submit Spark job on AKS using Spark Operator YAML",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Define Python function to run kubectl apply
def submit_spark_yaml():
    yaml_url = "https://vishalsparklogs.blob.core.windows.net/orc-data-container/yaml/orc-count-sparkapp.yaml"
    command = ["kubectl", "apply", "-f", yaml_url]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

submit_spark_job_python = PythonOperator(
    task_id="submit_spark_yaml_via_python",
    python_callable=submit_spark_yaml,
    dag=dag,
)

submit_spark_job_python
