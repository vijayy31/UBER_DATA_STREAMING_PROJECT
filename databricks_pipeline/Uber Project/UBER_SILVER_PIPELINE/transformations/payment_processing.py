from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

from utilities.payment_params import payment_schema, payment_errors, payment_columns

def ingest_to_silver(table_name:str, table_schema, table_columns: list, table_errors: list):

    #reading from the bronze table
    bronze_df = spark.readStream.table(f"uber_data.bronze.{table_name}")

    #parsing the JSON files into pyspark object
    parsed_df = bronze_df.withColumn("parsed", from_json(col("raw_payload"), table_schema))\
                    .withColumn("_parsed_failed", col("parsed").isNull())
    
    #flattening the parsed JSON into individual columns only if the parsed column contains value
    flat_df = parsed_df.filter(~col("_parsed_failed"))\
            .withColumn("source_file", element_at(split(col("source_file"),'/'), -1))
    
    if table_name == "bronze_ride_events":
        flat_df = flat_df.select("parsed.*",
                    col("parsed.pickup.latitude").alias("pickup_latitude"),
                    col("parsed.pickup.longitude").alias("pickup_longitude"),
                    col("parsed.dropoff.latitude").alias("dropoff_latitude"),
                    col("parsed.dropoff.longitude").alias("dropoff_longitude"),
                    "*")\
            .drop("parsed", "_parsed_failed", "pickup", "dropoff", "file_modified_time", "raw_payload")

    elif table_name == "bronze_payment_events":
        flat_df = flat_df.select("parsed.*", "*")\
                        .drop("parsed", "_parsed_failed", "file_modified_time", "raw_payload")
 
    normalize_df = flat_df

    for c in normalize_df.columns:
        if c in ["event_id", "event_type", "ride_id", "rider_id", "producer", "driver_id", "cancelled_by", "cancellation_reason", "payment_id"]:
            normalize_df = normalize_df.withColumn(c, lower(trim(col(c))).cast("string"))

        elif c in ["currency", "city", "service_type", "payment_method", "payment_status"]:
            if c in ["service_type", "payment_status"]:
                normalize_df.withColumn(c, trim(col(c)).cast("string"))
            else:
                normalize_df = normalize_df.withColumn(c, upper(trim(col(c)).cast("string")))

        elif c in ["event_timestamp"]:
            normalize_df = normalize_df.withColumn(c, col(c).cast("timestamp"))

        elif c in ["pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "actual_distance_km", "actual_duration_minutes", "driver_distance_km", "estimated_arrival_minutes", "estimated_duration_minutes", "estimated_distance_km", "estimated_fare", "final_fare", "amount"]:
            normalize_df = normalize_df.withColumn(c, col(c).cast("double"))
        
        elif c in ["event_version"]:
            normalize_df = normalize_df.withColumn(c, col(c).cast("long"))
        
        else:
            pass
        
    validated_df = normalize_df.withColumn(
        "validation_errors", filter(array(*table_errors), lambda x: x.isNotNull())
    )
    validated_df = validated_df.withColumn("validation_status",
                                        when(
                                            size(col("validation_errors")) == 0,
                                            lit("VALID")
                                        ).otherwise(
                                            lit("INVALID")
                                        ))
    
    return validated_df


payment_df = ingest_to_silver(table_name="bronze_payment_events", table_schema=payment_schema, table_columns=payment_columns, table_errors=payment_errors)


@dp.table
def silver_payment_events():

    df = payment_df.filter(col("validation_status")=="VALID")
    df = df.withColumn("_silver_ingestion_time", current_timestamp())\
            .drop("validation_status", "validation_errors")

    return df

@dp.table
def quarantine_payment_events():
    df = payment_df.filter(col("validation_status")=="INVALID")

    return df
    