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
RAW_FILE_PATH = f"s3://weather-data/raw/dt_{current_date}/city_milano_{raw_object_time_spec}.json"
OUTPUT_FILE_PATH = (
    f"s3://weather-data/processed/city_milano_{current_date}_{raw_object_time_spec}.parquet"
)
# creo la connessione
con = duckdb.connect()

# httpfs -> estensione principale di duckdb per leggere e scrivere file tramite l’API S3

# For many of DuckDB's core extensions, explicitly loading and installing extensions
# is not necessary. DuckDB contains an autoloading mechanism which can install and 
# load the core extensions as soon as they are used in a query.

# [installo e carico l'estensione]
# con.execute("INSTALL httpfs;")
# con.execute("LOAD httpfs;")

# save credentials in SECRET
# https://duckdb.org/docs/stable/configuration/secrets_manager

con.execute(
    f"""CREATE OR REPLACE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{MINIO_ROOT_USER}',
            SECRET '{MINIO_ROOT_PASSWORD}',
            ENDPOINT 'localhost:9000',
            USE_SSL false,
            URL_STYLE 'path'
        );"""
)


# read from the JSON (on s3)
# do transformations
# save in parquet format (on s3)
# PARQUET FILE FORMAT
query = """
copy(
    SELECT
        name AS city, 
        to_timestamp(dt) as timestamp,      
        DATE(to_timestamp(dt)) AS date,
        main.temp - 273.15 AS temp_c,
        main.humidity AS humidity,
        main.pressure AS pressure,
        wind.speed as wind_speed
    FROM read_json($input)
    )
TO $output(FORMAT PARQUET);
"""
con.execute(query, {"input": RAW_FILE_PATH, "output": OUTPUT_FILE_PATH})

# ogni giorno produciamo un parquet
# potremmo schedulare che ogni anno (oppure diverso delta temporale)
# viene prodotto un unico parquet unendo tutti i singoli e partizionandolo (per mese per esempio)
# il parquet prodotto potrebbe essere usato per analytics
# (si presta meglio del database per alcune analisi)
# # # COPY (
# # #     SELECT *
# # #     FROM parquet_scan('s3://weather-data/processed/*.parquet')
# # # )
# # # TO 's3://weather-data/analytics/anno_del_file.parquet'
# # # (
# # #     FORMAT PARQUET,
# # #     PARTITION_BY month(date)
# # # );

print("parquet file written successfully")