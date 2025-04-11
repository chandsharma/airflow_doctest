from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountRows").getOrCreate()

# Replace with your actual Azure Blob path
input_path = "wasbs://orc-data-container@vishalsparklogs.blob.core.windows.net/dummy_data/parquet/"

df = spark.read.parquet(input_path)
print(f"🔢 Total Rows: {df.count()}")

spark.stop()
