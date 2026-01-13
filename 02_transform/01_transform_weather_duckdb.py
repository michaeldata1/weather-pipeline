import datetime
import os
import duckdb
from dotenv import load_dotenv
load_dotenv()
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

with open("raw_data/datetime.txt") as file:
    raw_object_time_spec = file.read()
current_date = datetime.datetime.now(datetime.UTC).date()
# percorsi
RAW_FILE_PATH = f"s3://raw/dt_{current_date}/city_milano_{raw_object_time_spec}.json"
OUTPUT_FILE_PATH = f"s3://processed/dt_{current_date}/city_milano_{raw_object_time_spec}.parquet"
# creo la connessione
con = duckdb