
from pyspark import pipelines as dp

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)


# ============================================================
# 1. SOURCE CONFIGURATION
# ============================================================

BRONZE_RIDE_TABLE = "main.bronze.bronze_ride_events"


# ============================================================
# 2. NESTED LOCATION SCHEMA
# ============================================================

location_schema = StructType([

    StructField("latitude", DoubleType(), True),

    StructField("longitude", DoubleType(), True)

])


# ============================================================
# 3. COMMON RIDE EVENT SCHEMA
# ============================================================

ride_schema = StructType([

    # Common mandatory fields
    StructField("event_id", StringType(), False),

    StructField("event_type", StringType(), False),

    StructField("event_timestamp", TimestampType(), False),

    StructField("event_version", IntegerType(), False),

    StructField("producer", StringType(), False),

    StructField("ride_id", StringType(), False),

    StructField("rider_id", StringType(), False),

    StructField("currency", StringType(), False),


    # Optional / event-specific fields
    StructField("driver_id", StringType(), True),

    StructField("service_type", StringType(), True),

    StructField("city", StringType(), True),

    StructField("pickup", location_schema, True),

    StructField("dropoff", location_schema, True),

    StructField("estimated_distance_km", DoubleType(), True),

    StructField("estimated_duration_minutes", IntegerType(), True),

    StructField("estimated_fare", DoubleType(), True),

    StructField("driver_distance_km", DoubleType(), True),

    StructField("estimated_arrival_minutes", IntegerType(), True),

    StructField("actual_distance_km", DoubleType(), True),

    StructField("actual_duration_minutes", IntegerType(), True),

    StructField("final_fare", DoubleType(), True),

    # Captures malformed JSON
    StructField("_corrupt_record", StringType(), True)

])


# ============================================================
# 4. COMMON PARSING AND VALIDATION LOGIC
# ============================================================

def build_validated_ride_events():

    # Read Bronze as a streaming source
    bronze_df = (
        spark.readStream
        .table(BRONZE_RIDE_TABLE)
    )


    # Parse raw JSON
    parsed_df = bronze_df.withColumn(

        "parsed",

        F.from_json(
            F.col("raw_payload"),
            ride_schema,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
                "timestampFormat": "yyyy-MM-dd'T'HH:mm:ssX"
            }
        )
    )


    # Flatten parsed JSON into columns
    flattened_df = parsed_df.select(

        "parsed.*",

        F.col("raw_payload"),

        F.col("source_file"),

        F.col("file_size"),

        F.col("file_modification_time"),

        F.col("ingestion_timestamp").alias(
            "bronze_ingestion_timestamp"
        ),

        F.col("event_year"),

        F.col("event_month"),

        F.col("event_day")

    )


    # Normalize event type and mandatory string fields
    normalized_df = flattened_df.withColumn(

        "event_type",
        F.lower(F.trim(F.col("event_type")))

    ).withColumn(

        "event_id",
        F.trim(F.col("event_id"))

    ).withColumn(

        "ride_id",
        F.trim(F.col("ride_id"))

    ).withColumn(

        "rider_id",
        F.trim(F.col("rider_id"))

    ).withColumn(

        "producer",
        F.trim(F.col("producer"))

    ).withColumn(

        "currency",
        F.upper(F.trim(F.col("currency")))

    )


    # --------------------------------------------------------
    # COMMON MANDATORY FIELD VALIDATIONS
    # --------------------------------------------------------

    common_errors = [

        F.when(
            F.col("parsed").isNull(),
            F.lit("JSON parsing failed")
        ),

        F.when(
            F.col("_corrupt_record").isNotNull(),
            F.lit("Malformed JSON")
        ),

        F.when(
            F.col("event_id").isNull() |
            (F.col("event_id") == ""),
            F.lit("Missing event_id")
        ),

        F.when(
            F.col("event_type").isNull() |
            (F.col("event_type") == ""),
            F.lit("Missing event_type")
        ),

        F.when(
            F.col("event_timestamp").isNull(),
            F.lit("Missing or invalid event_timestamp")
        ),

        F.when(
            F.col("event_version").isNull() |
            (F.col("event_version") <= 0),
            F.lit("Missing or invalid event_version")
        ),

        F.when(
            F.col("producer").isNull() |
            (F.col("producer") == ""),
            F.lit("Missing producer")
        ),

        F.when(
            F.col("ride_id").isNull() |
            (F.col("ride_id") == ""),
            F.lit("Missing ride_id")
        ),

        F.when(
            F.col("rider_id").isNull() |
            (F.col("rider_id") == ""),
            F.lit("Missing rider_id")
        ),

        F.when(
            F.col("currency").isNull() |
            (F.col("currency") == ""),
            F.lit("Missing currency")
        )

    ]


    # --------------------------------------------------------
    # EVENT TYPE VALIDATION
    # --------------------------------------------------------

    allowed_event_types = [
        "ride_requested",
        "driver_assigned",
        "ride_started",
        "ride_completed"
    ]

    common_errors.append(

        F.when(
            F.col("event_type").isNotNull() &
            (~F.col("event_type").isin(allowed_event_types)),
            F.lit("Unsupported ride event type")
        )

    )


    # --------------------------------------------------------
    # EVENT-SPECIFIC VALIDATIONS
    # --------------------------------------------------------

    # RIDE REQUESTED
    common_errors.extend([

        F.when(
            (F.col("event_type") == "ride_requested") &
            (
                F.col("pickup").isNull() |
                F.col("dropoff").isNull()
            ),
            F.lit("ride_requested: pickup/dropoff missing")
        ),

        F.when(
            (F.col("event_type") == "ride_requested") &
            (
                F.col("pickup.latitude").isNull() |
                F.col("pickup.longitude").isNull() |
                F.col("dropoff.latitude").isNull() |
                F.col("dropoff.longitude").isNull()
            ),
            F.lit("ride_requested: invalid location coordinates")
        ),

        F.when(
            (F.col("event_type") == "ride_requested") &
            (
                F.col("estimated_fare").isNull() |
                (F.col("estimated_fare") < 0)
            ),
            F.lit("ride_requested: missing or invalid estimated_fare")
        )

    ])


    # DRIVER ASSIGNED
    common_errors.extend([

        F.when(
            (F.col("event_type") == "driver_assigned") &
            (
                F.col("driver_id").isNull() |
                (F.trim(F.col("driver_id")) == "")
            ),
            F.lit("driver_assigned: missing driver_id")
        ),

        F.when(
            (F.col("event_type") == "driver_assigned") &
            F.col("estimated_arrival_minutes").isNull(),
            F.lit("driver_assigned: missing estimated_arrival_minutes")
        )

    ])


    # RIDE STARTED
    common_errors.append(

        F.when(
            (F.col("event_type") == "ride_started") &
            (
                F.col("driver_id").isNull() |
                (F.trim(F.col("driver_id")) == "")
            ),
            F.lit("ride_started: missing driver_id")
        )

    )


    # RIDE COMPLETED
    common_errors.extend([

        F.when(
            (F.col("event_type") == "ride_completed") &
            (
                F.col("driver_id").isNull() |
                (F.trim(F.col("driver_id")) == "")
            ),
            F.lit("ride_completed: missing driver_id")
        ),

        F.when(
            (F.col("event_type") == "ride_completed") &
            (
                F.col("final_fare").isNull() |
                (F.col("final_fare") < 0)
            ),
            F.lit("ride_completed: missing or invalid final_fare")
        ),

        F.when(
            (F.col("event_type") == "ride_completed") &
            (
                F.col("actual_distance_km").isNull() |
                (F.col("actual_distance_km") < 0)
            ),
            F.lit("ride_completed: missing or invalid actual_distance_km")
        ),

        F.when(
            (F.col("event_type") == "ride_completed") &
            (
                F.col("actual_duration_minutes").isNull() |
                (F.col("actual_duration_minutes") < 0)
            ),
            F.lit("ride_completed: missing or invalid actual_duration_minutes")
        )

    ])


    # --------------------------------------------------------
    # CREATE VALIDATION ERROR ARRAY
    # --------------------------------------------------------

    validated_df = normalized_df.withColumn(

        "validation_errors",

        F.filter(
            F.array(*common_errors),
            lambda x: x.isNotNull()
        )

    )


    # Add processing metadata
    validated_df = validated_df.withColumn(

        "validation_status",

        F.when(
            F.size(F.col("validation_errors")) == 0,
            F.lit("VALID")
        ).otherwise(
            F.lit("INVALID")
        )

    ).withColumn(

        "silver_processing_timestamp",

        F.current_timestamp()

    )


    return validated_df


# ============================================================
# 5. SILVER RIDE EVENTS TABLE
# ============================================================

@dp.table(
    name="silver_ride_events",
    comment="Validated and structured Uber ride lifecycle events"
)
def silver_ride_events():

    df = build_validated_ride_events()

    return (

        df
        .filter(
            F.col("validation_status") == "VALID"
        )
        .drop(
            "validation_errors",
            "validation_status"
        )

    )


# ============================================================
# 6. QUARANTINE RIDE EVENTS TABLE
# ============================================================

@dp.table(
    name="quarantine_ride_events",
    comment="Invalid ride events retained for investigation"
)
def quarantine_ride_events():

    df = build_validated_ride_events()

    return (

        df
        .filter(
            F.col("validation_status") == "INVALID"
        )

    )
