from pyspark.sql.functions import *
from pyspark.sql.types import *
from utilities.common import common_errors

ride_schema=StructType([
            StructField('event_id', StringType(), True), 
            StructField('event_type', StringType(), True), 
            StructField('event_timestamp', StringType(), True), 
            StructField('event_version', LongType(), True),
            StructField('producer', StringType(), True),
            StructField('ride_id', StringType(), True), 
            StructField('rider_id', StringType(), True),
            StructField('driver_id', StringType(), True),
            StructField('service_type', StringType(), True),
            StructField('city', StringType(), True),
            StructField('currency', StringType(), True),
            StructField('actual_distance_km', DoubleType(), True),
            StructField('actual_duration_minutes', LongType(), True),
            StructField('estimated_fare', DoubleType(), True),
            StructField('final_fare', DoubleType(), True),
            StructField('pickup', StructType([StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True)]), True),
            StructField('dropoff', StructType([StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True)]), True),
            StructField('driver_distance_km', DoubleType(), True),
            StructField('estimated_arrival_minutes', LongType(), True),
            StructField('estimated_distance_km', DoubleType(), True),
            StructField('estimated_duration_minutes', LongType(), True),
            StructField('cancelled_by', StringType(), True), 
            StructField('cancellation_reason', StringType(), True)
])

ride_columns = [
 'event_id',
 'event_timestamp',
 'event_type',
 'event_version',
 'producer', 
 'ride_id',
 'rider_id',
 'driver_id',
 'service_type',
 'city',
 'currency',  
 'actual_distance_km',
 'actual_duration_minutes',
 'estimated_fare',
 'final_fare',
 'pickup_latitude',
 'pickup_longitude',
 'dropoff_latitude',
 'dropoff_longitude',
 'driver_distance_km',
 'estimated_arrival_minutes',
 'estimated_distance_km',
 'estimated_duration_minutes',
 'cancelled_by',
 'cancellation_reason'
]


ride_errors = common_errors
ride_errors.extend([
    when(
        (col("event_type")=="ride_requested") & 
        (col("pickup_latitude").isNull() | (col("pickup_longitude").isNull()) |
         (col("dropoff_latitude").isNull()) | (col("dropoff_longitude").isNull())),
        lit("Missing pickup/dropoff coordinates")
    ),
    when(
        (col("event_type")=="ride_requested") & 
        (col("estimated_fare").isNull() | (col("estimated_fare")<0 )),
        lit("Missing or invalid estimated_fare")
    ),
    when(
        (col("event_type")=="ride_requested") & 
        (col("estimated_duration_minutes").isNull() | (col("estimated_duration_minutes")<0)),
        lit("Missing or invalid estimated_duration_minutes")
    ),
    when(
        (col("event_type")=="ride_requested") & 
        (col("estimated_distance_km").isNull() | (col("estimated_distance_km")<0)),
        lit("Missing or invalid estimated_distance_km")
    ) 
])

ride_errors.extend([
    when(
        (col("event_type")=="driver_assigned") & 
        (col("estimated_arrival_minutes").isNull() | (col("estimated_arrival_minutes")<0)),
        lit("Missing or invalid estimated_arrival_minutes")
    ),
    when(
        (col("event_type")=="driver_assigned") & 
        (col("pickup_latitude").isNull() | (col("pickup_longitude").isNull())),
        lit("Missing pickup coordinates")
    ),
    when(
        (col("event_type")=="driver_assigned") & 
        (col("driver_distance_km").isNull() | (col("driver_distance_km")<0)),
        lit("Missing or invalid driver_distance_km")
    )
])

ride_errors.extend([
    when(
        (col("event_type")=="ride_started") & 
        (col("dropoff_latitude").isNull() | (col("dropoff_longitude").isNull())),
        lit("Missing dropoff coordinates")
    )
])

ride_errors.extend([
    when(
        (col("event_type")=="ride_completed") & 
        (col("actual_distance_km").isNull() | (col("actual_distance_km")<0)),
        lit("Missing or invalid actual_distance_km")
    ),
    when(
        (col("event_type")=="ride_completed") & 
        (col("actual_duration_minutes").isNull() | (col("actual_duration_minutes")<0)),
        lit("Missing or invalid actual_duration_minutes")
    ),
    when(
        (col("event_type")=="ride_completed") & 
        (col("final_fare").isNull() | (col("final_fare")<0)),
        lit("Missing or invalid final_fare")
    )
])

ride_errors.extend([
    when(
        (col("event_type")=="ride_cancelled") & 
        (col("cancelled_by").isNull() | (trim(col("cancelled_by"))=="")),
        lit("Missing cancellation_reason")
    ),
    when(
        (col("event_type")=="ride_cancelled") & 
        (col("cancellation_reason").isNull() | (trim(col("cancellation_reason"))=="")),
        lit("Missing cancellation_reason")
    )
])