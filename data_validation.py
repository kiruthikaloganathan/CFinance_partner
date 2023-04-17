from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import uuid
from delta import *

# Set the path to the file
#storage_account_name = "devistorageaccount1"
#container_name = "container1"
#file_name = "employees.csv"
#file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_name}"


spark = SparkSession.builder \
    .appName("myApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Set up the storage account credentials
#spark.conf.set("fs.azure.account.key.devistorageaccount1.blob.core.windows.net", "XYpQZZ3QlR8uSmxzycEYXWQfq2bi9OyK/gkqYiPpu73a6OaaKB5ErdW/jBZqVzFjXF3V0sQR4TCq+AStwixMRg==")


# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").load("/data/employees.csv")
#df = spark.read.option("header", "true").csv("employees")
print(df.show(5))
#print("file read")

# Add the ingestion timestamp and batch ID as new columns to the DataFrame
ingestion_tms = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
batch_id = str(uuid.uuid4())
df_with_extras = df.withColumn("ingestion_tms", lit(ingestion_tms)).withColumn("batch_id", lit(batch_id))
print(df_with_extras.show(5))

# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("last_name", "STRING") \
  .addColumn("first_name", "STRING", comment = "surname") \
  .addColumn("badge_code", "STRING") \
  .addColumn("email", "STRING") \
  .addColumn("class", "STRING") \
  .addColumn("seniority", "INT") \
  .addColumn("job_title", "STRING") \
  .addColumn("ingestion_tms", "TIMESTAMP") \
  .addColumn("batch_id", "STRING") \
  .property("description", "table with amended data") \
  .location("/default/delta-table") \
  .execute()

# Write the DataFrame to DeltaLake using APPEND mode
output_path = "/tmp/delta-table"
#output_path = "/delta/output"
#df_with_extras.write.mode("append").format("delta").save(output_path)
df_with_extras.write.mode("append").format("delta").saveAsTable("output_path")
print(output_path)


# Stop the Spark session
spark.stop()
