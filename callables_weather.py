import datetime
import json
import os

import boto3
import duckdb
import requests
from dotenv import load_dotenv

# carico come variabili d'ambiente le coppie k:v contenute in .env
load_dotenv()

# ottengo il valore associato a una variabile d'ambiente
API_KEY = os.getenv("OPENWEATHER_API_KEY")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def fetch_data():

    # DA FARE
    # rendere il fetch una funzione con parametro city variabile
    # ogni volta chiamarla su 3 città diverse
    CITY = "Milano"
    API_URL = "https://api.openweathermap.org/data/2.5/weather"

    params = {"q": CITY, "appid": API_KEY}

    # facciamo la richiesta all'API inserendo i parametri
    response = requests.get(API_URL, params)

    # solleva un eccezione (ferma lo script) se ci sono stati errori nella richiesta
    response.raise_for_status()

    # trasformo la risposta (json) in un oggetto python (dict in questo caso)
    data = response.json()

    # aggiungiamo il timestamp di ingestione (mi permette di risalire)
    data["ingestion_ts"] = datetime.datetime.now(datetime.UTC).isoformat()

    # salviamo il file
    with open("raw_data/weather_raw.json", "w") as file:
        json.dump(data, file, indent=2)

    # finisce nell'xcom
    return "raw_data/weather_raw.json"


def upload_to_minio(**context):

    logical_date = context["logical_date"]

    BUCKET = "weather-data"

    OBJECT_NAME = f"raw/dt_{logical_date.date()}/city_milano_{logical_date.strftime('%H-%M')}.json"

    # creiamo il client
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )

    # creiamo il bucket se non esiste
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print("The bucket already exists")
        # se non voglio fare nulla uso pass
        # pass

    # carichiamo il file
    with open("raw_data/weather_raw.json", "rb") as file:
        s3.put_object(Bucket=BUCKET, Key=OBJECT_NAME, Body=file)

    # ritorno il nome del file -> finisce nell'xcom
    return OBJECT_NAME


def transform_duckdb(ti):

    raw_key = ti.xcom_pull(task_ids="upload_to_minio")

    raw_parts = raw_key.split("/")

    RAW_FILE_PATH = f"s3://weather-data/{raw_key}"

    dt_part = raw_parts[1]         
    filename = raw_parts[2].replace(".json", ".parquet")

    OUTPUT_KEY = f"processed/{dt_part}/{filename}"

    OUTPUT_FILE_PATH = f"s3://weather-data/{OUTPUT_KEY}"

    # creo la connessione
    con = duckdb.connect()

    con.execute(
        f"""CREATE OR REPLACE SECRET (
                TYPE s3,
                KEY_ID '{MINIO_ROOT_USER}',
                SECRET '{MINIO_ROOT_PASSWORD}',
                ENDPOINT 'localhost:9000',
                USE_SSL false,
                URL_STYLE 'path'
            );"""
    )

    # leggiamo dal json (su storage s3)
    # facciamo delle trasformazioni
    # salviamo in formato parquet (su storage s3)
    # PARQUET FILE FORMAT
    query = """
    COPY 
        ( SELECT 
            name AS city,
            main.temp - 273.15 AS temp_c,
            main.humidity AS humidity,
            main.pressure AS pressure,
            wind.speed AS wind_speed,
            to_timestamp(dt) AT TIME ZONE 'UTC' AS timestamp,
            DATE(to_timestamp(dt) AT TIME ZONE 'UTC') AS date,
        FROM read_json($input)
        )
    TO $output (FORMAT PARQUET);
    """

    con.execute(query, {"input": RAW_FILE_PATH, "output": OUTPUT_FILE_PATH})


    return OUTPUT_KEY


def load_postgres(ti):

    parquet_key = ti.xcom_pull(task_ids="transform_duckdb")

    INPUT_FILE_PATH = f"s3://weather-data/{parquet_key}"

    # creo connessione
    con = duckdb.connect()

    # salviamo le credenziali in SECRET per entrambi i servizi (minio, postgres)
    # https://duckdb.org/docs/stable/configuration/secrets_manager
    con.execute(
        f"""CREATE OR REPLACE SECRET (
                TYPE s3,
                KEY_ID '{MINIO_ROOT_USER}',
                SECRET '{MINIO_ROOT_PASSWORD}',
                ENDPOINT 'localhost:9000',
                USE_SSL false,
                URL_STYLE 'path'
            );"""
    )

    con.execute(
        f"""CREATE OR REPLACE SECRET (
            TYPE postgres,  
            HOST '127.0.0.1',
            PORT 5432,
            DATABASE '{POSTGRES_DB}',
            USER '{POSTGRES_USER}',
            PASSWORD '{POSTGRES_PASSWORD}'
        );"""
    )

    # ci colleghiamo al database postgres (sfruttando le credenziali nel SECRET)
    con.execute("ATTACH '' AS postgres_db (TYPE postgres);")
    # da questo momento postgres_db è il nostro db postgres

    # IMPLEMENTARE CREAZIONE TABELLA
    con.execute(
        """CREATE TABLE IF NOT EXISTS postgres_db.weather_daily (
                    city TEXT,
                    temp_c REAL,
                    humidity INTEGER,
                    pressure INTEGER,
                    wind_speed REAL,
                    timestamp TIMESTAMP,
                    date DATE
                );"""
    )

    # inseriamo nella tabella su postgres il contenuto del file parquet
    query = (
        f"""INSERT INTO postgres_db.weather_daily SELECT * FROM '{INPUT_FILE_PATH}';"""
    )
    con.execute(query)

    print("Data loaded into Postgres")