import sys
import os
import json
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Args
config_path = sys.argv[1]
gcs_file_path = sys.argv[2]

spark = SparkSession.builder \
    .appName("Enterprise GCS to BigQuery Pipeline") \
    .getOrCreate()

sc = spark.sparkContext

# Read Config
config_data = sc.textFile(config_path).collect()
config = json.loads("".join(config_data))

project_id = config["project"]["project_id"]
raw_dataset = config["bigquery"]["raw_dataset"]
temp_bucket = config["storage"]["temporary_gcs_bucket"]
raw_folder = config["storage"]["raw_folder"]
file_format = config["storage"]["file_format"]
partition_column = config["bigquery"]["partition_column"]
partition_type = config["bigquery"]["partition_type"]
default_write_mode = config["bigquery"]["write_mode_default"]

audit_dataset = config["audit"]["audit_dataset"]
audit_table_name = config["audit"]["audit_table"]

# Validate file
if not gcs_file_path.endswith(f".{file_format}"):
    print("Invalid file format")
    sys.exit(0)

if raw_folder not in gcs_file_path:
    print("Not in raw folder")
    sys.exit(0)

file_name = os.path.basename(gcs_file_path)
table_name = file_name.replace(".csv", "").split("_")[-1]

table_config = config["tables"].get(table_name, {})
write_mode = table_config.get("write_mode", default_write_mode)

start_time = datetime.utcnow()

try:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "FAILFAST") \
        .csv(gcs_file_path)

    if df.rdd.isEmpty():
        sys.exit(0)

    record_count = df.count()

    df = df.withColumn(partition_column, current_timestamp()) \
           .withColumn("source_file", lit(file_name))

    bq_table = f"{project_id}.{raw_dataset}.{table_name}"

    df.write \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .option("partitionField", partition_column) \
        .option("partitionType", partition_type) \
        .mode(write_mode) \
        .save()

    # Audit log
    audit_data = [(table_name, file_name, record_count,
                   "SUCCESS", start_time, datetime.utcnow())]

    audit_df = spark.createDataFrame(
        audit_data,
        ["table_name", "file_name", "record_count",
         "status", "start_time", "end_time"]
    )

    audit_table = f"{project_id}.{audit_dataset}.{audit_table_name}"

    audit_df.write \
        .format("bigquery") \
        .option("table", audit_table) \
        .option("temporaryGcsBucket", temp_bucket) \
        .mode("append") \
        .save()

except Exception as e:
    print(traceback.format_exc())
    sys.exit(1)

spark.stop()