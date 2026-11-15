from pyspark.sql.types import *
from pyspark.sql.functions import *
from utilities.common import common_errors

payment_schema = StructType([
        StructField('event_id', StringType(), True), 
        StructField('event_timestamp', StringType(), True), 
        StructField('event_type', StringType(), True), 
        StructField('event_version', LongType(), True), 
        StructField('producer', StringType(), True), 
        StructField('ride_id', StringType(), True), 
        StructField('rider_id', StringType(), True),
        StructField('service_type', StringType(), True),
        StructField('driver_id', StringType(), True),
        StructField('city', StringType(), True), 
        StructField('currency', StringType(), True), 
        StructField('payment_id', StringType(), True),
        StructField('amount', DoubleType(), True), 
        StructField('payment_method', StringType(), True), 
        StructField('payment_status', StringType(), True)
])

payment_errors = common_errors
payment_errors.extend([
    when(
        (col("payment_id").isNull()) | (trim(col("payment_id"))==""),
        lit("Missing payment_id")
    ),
    when(
        col("amount").isNull() | (col("amount")<0),
        lit("Missing or Invalid amount")
    ),
    when(
        (col("payment_method").isNull()) | (trim(col("payment_method"))==""),
        lit("Missing payment_method")
    ),
    when(
        (col("payment_status").isNull()) | (trim(col("payment_status"))==""),
        lit("Missing payment_status")
    )
])


payment_columns=[
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
 'amount',
 'payment_id',
 'payment_method',
 'payment_status'
]
