# from  uber_de_project.aws.s3_client  import BUCKET_NAME, s3_client
# import json

# event = {
#     "event_id": "EVT001",
#     "event_type": "RIDE_REQUESTED",
#     "ride_id": "RIDE101",
#     "rider_id": "USER101"
# }

# s3_client.put_object(
#     Bucket=BUCKET_NAME,
#     Key=f"test/json/{event['event_id']}.json",
#     Body=json.dumps(event),
#     ContentType="application/json"
# )

# print("Successfully uploaded JSON to S3!")
