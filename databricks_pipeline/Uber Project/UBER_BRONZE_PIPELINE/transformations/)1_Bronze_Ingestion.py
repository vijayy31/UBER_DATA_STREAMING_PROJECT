from pyspark import pipelines as dp
from pyspark.sql.functions import *

BUCKET_NAME = "s3://uber-project-31/bronze"

def ingest_to_bronze(endpoint:str):

    SCHEMA = """
        path STRING,
        modificationTime TIMESTAMP,
        length LONG,
        content BINARY,
        year INT,
        month INT,
        day INT
    """
    file_path = f"{BUCKET_NAME}/{endpoint}"

    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "binaryFile")\
                .schema(SCHEMA)\
                .load(file_path)
    
    bronze_df = df.withColumn("raw_payload", col("content").cast("string"))\
                .withColumn("source_file", col("path"))\
                .withColumn("file_size_KB", col("length"))\
                .withColumn("file_modified_time", col("modificationTime"))\
                .withColumn("__bronze_ingestion_time", current_timestamp())\
                .select(col("raw_payload"), 
                        col("source_file"), 
                        col("file_size_KB"), 
                        col("file_modified_time"),
                        col("__bronze_ingestion_time"))

    return bronze_df

@dp.table
def bronze_ride_events():
    ride_df = ingest_to_bronze(endpoint="ride_events")
    return ride_df

@dp.table
def bronze_payment_events():
    payment_df = ingest_to_bronze(endpoint="payment_events")
    return payment_df



