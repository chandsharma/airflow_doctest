from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime
import textwrap

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

# Define the inline Spark code as a multi-line string.
# This sample code creates a simple DataFrame and writes it as CSV data
# to your Azure Blob Storage container "spark-logs" under the path "sample_data".
spark_code = textwrap.dedent("""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("SampleJob").getOrCreate()
    
    # Create sample data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Show the data on the driver log
    df.show()
    
    # Write data to Azure Blob Storage.
    # Ensure that the SparkConf is already set with the appropriate account key,
    # as in your environment (see sparkConf below).
    output_path = "wasbs://spark-logs@sparklstorageyaw4w3.blob.core.windows.net/sample_data"
    df.write.format("csv").mode("overwrite").save(output_path)
    
    spark.stop()
""").strip()

# Build a shell command that writes the inline spark_code to a file
# and then runs spark-submit on that file.
# We need to escape newlines and quotes appropriately.
command_str = (
    "/bin/sh -c '"
    "echo \"{code}\" > /tmp/sample_job.py && "
    "spark-submit /tmp/sample_job.py'"
).format(code=spark_code.replace('"', '\\"').replace("\n", "\\n"))

# Define the SparkApplication spec that will be passed to the SparkKubernetesOperator.
# Note: mainApplicationFile is a dummy value here because the driver command is overridden.
spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "sample-spark-job",
        "namespace": "default"
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": "docker.io/channnuu/chandan_spark:3.5.2",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///tmp/sample_job.py",
        "sparkVersion": "3.5.2",
        "restartPolicy": {
            "type": "OnFailure",
            "onSubmissionFailureRetries": 3,
            "onSubmissionFailureRetryInterval": 10,
            "onFailureRetries": 3,
            "onFailureRetryInterval": 10
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "command": ["/bin/sh", "-c"],
            "args": [ command_str ],
            "serviceAccount": "spark"
        },
        "executor": {
            "cores": 1,
            "instances": 2,
            "memory": "512m"
        },
        "sparkConf": {
            "spark.eventLog.enabled": "false",
            "spark.hadoop.fs.azure.account.key.sparklstorageyaw4w3.blob.core.windows.net":
              "7IXAhwxhU3FBZnKcm96tkVj87zoA2OlbIIdkW1Dv8RdTWvew9vqTgx1Gj7npn6tkdEcW9HZGY1br+ASt3fWcRQ=="
        }
    }
}

dag = DAG(
    "sample_spark_job_dag",
    default_args=default_args,
    description="A DAG that submits a Spark job to create sample data in Azure Blob Storage",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

run_spark_job = SparkKubernetesOperator(
    task_id="run_sample_spark_job",
    namespace="default",
    template_spec=spec,
    get_logs=True,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
