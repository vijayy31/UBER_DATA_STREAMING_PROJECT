from pyspark.sql.functions import *
from pyspark.sql.types import *

common_errors = [
    when(
        (col("event_id").isNull()) | (trim(col("event_id"))==""),
        lit("Missing event_id")
    ),
    when(
        (col("event_type").isNull()) | (trim(col("event_type")) == ""),
        lit("Missing event_type")
    ),
    when(
        col("event_timestamp").isNull(),
        lit("Missing or invalid event_timestamp")
    ),
    when(
        (col("event_version").isNull()) | (col("event_version") <= 0),
        lit("Invalid event_version")
    ),
    when(
        col("producer").isNull() | (trim(col("producer")) == ""),
        lit("Missing producer")
    ),
    when(
        (col("ride_id").isNull()) | (trim(col("ride_id")) == ""),
        lit("Missing ride_id")
    ),
    when(
        (col("rider_id").isNull()) | (trim(col("rider_id")) == ""),
        lit("Missing rider_id")
    ),
    when(
        (col("currency").isNull()) | (trim(col("currency")) == ""),
        lit("Missing currency")
    ),
    when(
        (col("service_type").isNull()) | (trim(col("service_type")) == ""),
        lit("Missing service_type")
    ),
    when(
        (col("city").isNull()) | (trim(col("city")) == ""),
        lit("Missing city")
    ),
    when(
        (col("event_type").isNull() | (trim(col("event_type")) == "")) | 
        (~col("event_type").isin("ride_requested", "driver_assigned", "ride_started", "ride_completed", "ride_cancelled", "payment_event")),
        lit("Missing or Invalid event_type")
    ),
    when(
        (~col("event_type").isin("ride_requested", "ride_cancelled") ) &
        ((col("driver_id").isNull()) | (trim(col("driver_id")) == "")),
        lit("Missing driver_id")
    )
]