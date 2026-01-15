import datetime
import os
import duckdb
from dotenv import load_dotenv
load_dotenv()
POSTGRES_DB=os.getenv("POSTGRES_DB")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")

MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

# I access parquet path file
with open("raw_data/datetime.txt") as file:
    raw_object_time_spec = file.read()
current_date = datetime.datetime.now(datetime.UTC).date()
# percorsi
OUTPUT_FILE_PATH = (
    f"s3://weather-data/processed/city_milano_{current_date}_{raw_object_time_spec}.parquet"
)

con = duckdb.connect()

# save credentials in secret
con.execute(
    f"""CREATE OR REPLACE SECRET(
            TYPE s3,
            KEY_ID '{MINIO_ROOT_USER}',
            SECRET '{MINIO_ROOT_PASSWORD}',
            ENDPOINT 'localhost:9000',
            USE_SSL false,
            URL_STYLE 'path'
        );"""
)

con.execute(
    f"""CREATE OR REPLACE SECRET(
        TYPE postgres,
        HOST '127.0.0.1',
        PORT 5432,
        DATABASE '{POSTGRES_DB}',
        USER '{POSTGRES_USER}',
        PASSWORD '{POSTGRES_PASSWORD}'
    );"""
)

con.execute("install postgres;")


# connect to postgres database using secret credentials
con.execute("ATTACH '' AS postgres_db (TYPE postgres);")

# from here postgres_db is our postgress database
query = f"""INSERT INTO postgres_db.weather_daily SELECT * FROM '{OUTPUT_FILE_PATH}';"""

# we insert the content of the parquet into the table in postgres
con.execute(query)

print("loaded to postgres")
