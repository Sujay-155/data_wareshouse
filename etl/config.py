# etl/config.py
import os
from dotenv import load_dotenv

# Load variables from .env file into environment
load_dotenv()

# etl/config.py
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "weather_dw"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

if WEATHER_API_KEY is None:
    raise ValueError("WEATHER_API_KEY not found. Please set it in the .env file.")
