import os
from dotenv import load_dotenv
import boto3
import datetime

load_dotenv()

MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET = "weather-data"
current_time_utc = datetime.datetime.now(datetime.UTC)
# quando creo DAG lasciare solo ora (viene eseguita ogni ora alle :00)
with open("raw_data/datetime.txt", "w") as f:
    f.write(f"{current_time_utc.hour}-{current_time_utc.minute}")

OBJECT_NAME = f"raw/dt_{current_time_utc.date()}/city_milano_{current_time_utc.hour}-{current_time_utc.minute}.json"

# creiamo il client
s3 = boto3.client(
    "s3",
    endpoint_url= "http://localhost:9000", 
    aws_access_key_id = MINIO_ROOT_USER, 
    aws_secret_access_key = MINIO_ROOT_PASSWORD
)

# creiamo il bucket se non esiste
try:
    s3.create_bucket(Bucket= BUCKET)
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

# carichiamo il file
with open("raw_data/weather_raw.json", "rb") as file:
    s3.put_object(Bucket=BUCKET, Key=OBJECT_NAME,Body = file)

# messaggio finale
print("uploaded to MinIO")