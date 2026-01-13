import requests
import os
from dotenv import load_dotenv
import json
import datetime


# carico come variabili d'ambiente le coppie k:v contenute in .env
load_dotenv()

# ottengo il valore associto a una variabile d'ambiente
API_KEY = os.getenv("PGADMIN_API_KEY")
CITY = "Milano"
API_URL = "https://api.openweathermap.org/data/2.5/weather"
params = {"q": CITY, "appid": API_KEY}

# facciamo la richiesta all'API inserendo i parametri 
response = requests.get(API_URL, params)

# solleva un eccezione (ferma lo script) se ci sono errori 
response.raise_for_status()

# trasforma la risposta (json) in un oggetto python (dict in questo caso)
data = response.json()

# aggiungiamo il timestamp di ingstione (mi permette di rislaire)
data["ingestion_ts"] = datetime.datetime.now(datetime.UTC).isoformat()

# salviamo il file 
with open ("raw_data/weather_raw.json", "w") as file:
    json.dump(data, file, indent=2)

# messaggio finale
print("Weather data fetched")