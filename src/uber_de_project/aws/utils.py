from datetime import datetime

def s3_key_generator(event: dict, parent_folder:str):

    tz=event["event_timestamp"]

    dt = datetime.fromisoformat(tz.replace("Z", "+00:00"))
    
    date_key=f"year={dt.year}/month={dt.month}/day={dt.day}"

    event_type = event["event_type"]

    if event_type == "payment_event":
        event_key = "payment_events"
        filename = "payment"
        
    else:
        event_key = "ride_events"
        filename = "ride"

    id = event["event_id"].split("_")[1]
    s3_key = f"{parent_folder}/{event_key}/{date_key}/{filename}_{id}.json"

    return s3_key   


event1 = {
    "event_id": "evt_2dcb9d408269",
    "event_type": "ride_completed",
    "event_timestamp": "2026-11-11T06:16:17Z",
    "event_version": 1,
    "producer": "ride-service",
    "ride_id": "ride_0c7acb30ee31",
    "rider_id": "rider_1005",
    "driver_id": "driver_2002",
    "service_type": "UberX",
    "city": "Mumbai",
    "actual_distance_km": 3.03,
    "actual_duration_minutes": 11,
    "final_fare": 102.3,
    "currency": "INR"
}

event2 = {
    "event_id": "evt_0f29302faf37",
    "event_type": "payment_event",
    "event_timestamp": "2026-11-11T06:16:39Z",
    "event_version": 1,
    "producer": "payment-service",
    "ride_id": "ride_0c7acb30ee31",
    "rider_id": "rider_1005",
    "driver_id": "driver_2002",
    "city": "Mumbai",
    "payment_id": "payment_db29769133a0",
    "amount": 102.3,
    "currency": "INR",
    "payment_method": "UPI",
    "payment_status": "Paid"
}

# ev = s3_key_generator(event2)

# print(ev)