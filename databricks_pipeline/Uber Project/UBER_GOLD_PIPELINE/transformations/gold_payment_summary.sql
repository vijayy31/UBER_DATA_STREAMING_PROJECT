CREATE OR REPLACE MATERIALIZED VIEW gold_payment_summary AS

SELECT payment_id, ride_id, rider_id, driver_id, currency, event_timestamp as payment_timestamp,
        amount as payment_amount, city, payment_method, payment_status
    
FROM uber_data.silver.silver_payment_events