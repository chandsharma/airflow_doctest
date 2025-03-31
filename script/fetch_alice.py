from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FetchAlice").getOrCreate()

storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container_name = "orc-data-container"
orc_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

df = spark.read.format("orc").load(orc_path)

alice_df = df.filter(df["name"] == "Alice")

alice_df.show()

spark.stop()
