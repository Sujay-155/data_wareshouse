# etl/config.py
import os
from dotenv import load_dotenv

# Load variables from .env file into environment
load_dotenv()

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

if WEATHER_API_KEY is None:
    raise ValueError("WEATHER_API_KEY not found. Please set it in the .env file.")
