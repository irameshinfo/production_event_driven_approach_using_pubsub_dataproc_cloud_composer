import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import re

spark = SparkSession.builder \
    .appName("Enterprise GCS Folder to BigQuery Pipeline") \
    .getOrCreate()

# -------------------------------
# Read Config Path
# -------------------------------
config_path = sys.argv[1]

# -------------------------------
# Read Config JSON
# -------------------------------
config_df = spark.read.text(config_path)
config_json = json.loads("\n".join([row.value for row in config_df.collect()]))

project_id = config_json["project"]["project_id"]
dataset = config_json["bigquery"]["raw_dataset"]
temp_bucket = config_json["storage"]["temporary_gcs_bucket"]
raw_bucket = config_json["storage"]["raw_bucket"]
raw_folder = config_json["storage"]["raw_folder"]

# -------------------------------
# Read All CSV Files from Folder
# -------------------------------
input_path = f"gs://{raw_bucket}/{raw_folder}*.csv"

files = spark.sparkContext.wholeTextFiles(input_path).keys().collect()

print("Files Found:", files)

for file in files:

    print("Processing:", file)

    file_name = file.split("/")[-1].lower()

    if "account" in file_name:
        table_name = "account"
    elif "branch" in file_name:
        table_name = "branch"
    elif "customer" in file_name:
        table_name = "customer"
    elif "product" in file_name:
        table_name = "products"
    elif "transaction" in file_name:
        table_name = "transaction"
    else:
        print("Skipping unknown file:", file_name)
        continue

    write_mode = config_json["tables"][table_name]["write_mode"]

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file)

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.{table_name}") \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode(write_mode) \
        .save()

    print(f"Loaded {table_name} successfully!")

spark.stop()