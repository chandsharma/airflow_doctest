FROM bitnami/spark:3.5.0

WORKDIR /app

COPY spark_job_count_rows.py .

ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit", "spark_job_count_rows.py"]
