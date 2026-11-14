import os
from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    import boto3

    AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION=os.getenv("AWS_DEFAULT_REGION")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )

    return s3_client

def upload_to_s3(s3_client, event: dict, parent_folder: str):
    from uber_de_project.aws.utils import s3_key_generator
    import json

    s3_key = s3_key_generator(event=event, parent_folder=parent_folder)
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(event),
        ContentType="application/json"
    )
