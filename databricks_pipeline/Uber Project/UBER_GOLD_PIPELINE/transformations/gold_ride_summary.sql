CREATE OR REPLACE MATERIALIZED VIEW gold_ride_summary as 

WITH ride_summary as (
    SELECT

    MAX(ride_id) as ride_id,
    MAX(rider_id) as rider_id,
    MAX(driver_id) as driver_id,
    MAX(city) as city,
    MAX(currency) as currency,
    MAX(
        CASE 
            WHEN event_type=="ride_requested"
            THEN event_timestamp
        END
    ) AS ride_requested_timestamp,
    MAX(
        CASE 
            WHEN event_type=="driver_assigned"
            THEN event_timestamp
        END
    ) AS driver_assigned_timestamp,
    MAX(
        CASE 
            WHEN event_type=="ride_started"
            THEN event_timestamp
        END
    ) AS ride_started_timestamp,
    MAX(
        CASE 
            WHEN event_type=="ride_completed"
            THEN event_timestamp
        END
    ) AS ride_completed_timestamp,
    MAX(
        CASE 
            WHEN event_type=="ride_cancelled"
            THEN event_timestamp
        END
    ) AS ride_cancelled_timestamp,
    MAX(estimated_fare) as estimated_fare,
    MAX(final_fare) as final_fare,
    MAX(estimated_distance_km) as estimated_distance_km,
    MAX(actual_distance_km) as actual_distance_km,
    MAX(estimated_duration_minutes) as estimated_duration_minutes,
    MAX(actual_duration_minutes) as actual_duration_minutes,
    MAX(cancelled_by) as cancelled_by,
    MAX(cancellation_reason) as cancellation_reason
FROM uber_data.silver.silver_ride_events

GROUP BY ride_id
)

SELECT *,
    (CASE
        WHEN ride_completed_timestamp IS NOT NULL THEN "COMPLETED"
        WHEN ride_started_timestamp IS NOT NULL THEN "STARTED"
        WHEN ride_cancelled_timestamp IS NOT NULL THEN "CANCELLED"
        WHEN driver_assigned_timestamp IS NOT NULL THEN "ASSIGNED"
        WHEN ride_requested_timestamp IS NOT NULL THEN "REQUESTED"
        ELSE "UNKNOWN"
    END) AS ride_status,
    ROUND((UNIX_TIMESTAMP(driver_assigned_timestamp)-UNIX_TIMESTAMP(ride_requested_timestamp))/60,2) AS driver_assignment_time_minutes,
    ROUND((UNIX_TIMESTAMP(ride_started_timestamp)-UNIX_TIMESTAMP(driver_assigned_timestamp))/60,2) AS driver_wait_time_minutes,
    ROUND((UNIX_TIMESTAMP(ride_completed_timestamp)-UNIX_TIMESTAMP(ride_started_timestamp))/60,2) AS ride_duration_minutes,
    ROUND((UNIX_TIMESTAMP(ride_completed_timestamp)-UNIX_TIMESTAMP(ride_requested_timestamp))/60,2) AS total_ride_time_minutes
FROM ride_summary