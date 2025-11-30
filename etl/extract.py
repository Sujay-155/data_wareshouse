# etl/extract.py
import pandas as pd
import requests

from .config import WEATHER_API_KEY

# --------------- CSV (Kaggle) side ---------------

def load_city_csv(path: str) -> pd.DataFrame:
    """
    Load city data from a CSV file into a pandas DataFrame.
    """
    df = pd.read_csv(path)

    # Optional: small cleanup (strip whitespace, standardize column names)
    df.columns = [col.strip() for col in df.columns]

    return df


# --------------- Weather API side ---------------

BASE_URL_CURRENT = "http://api.weatherapi.com/v1/current.json"


def fetch_current_weather_for_city(city_name: str) -> pd.DataFrame:
    """
    Call WeatherAPI current weather endpoint for a single city
    and return a 1-row DataFrame.
    """
    params = {
        "key": WEATHER_API_KEY,
        "q": city_name,
        "aqi": "yes",  
    }

    response = requests.get(BASE_URL_CURRENT, params=params)
    response.raise_for_status()  # will raise an error if request failed
    data = response.json()

    # Extract fields we care about
    location = data.get("location", {})
    current = data.get("current", {})

    row = {
        # keep lat/lon for join
        "lat": location.get("lat"),
        "lon": location.get("lon"),

        # useful to still have these for debugging / checks
        "city_name_api": location.get("name"),
        "country_api": location.get("country"),

        # metrics we actually care about
        "temp_c": current.get("temp_c"),
        "humidity": current.get("humidity"),
        "condition_text": current.get("condition", {}).get("text"),
        "aqi": current.get("air_quality", {}).get("us-epa-index"),  # Air Quality Index
    }

    df = pd.DataFrame([row])
    return df


def fetch_current_weather_for_cities(city_list):
    """
    Fetch current weather for a list of cities (dicts with id, city, country) and return a combined DataFrame.
    """
    all_rows = []

    for city_dict in city_list:
        city_name = f"{city_dict['city']}, {city_dict['country']}"
        try:
            df_city = fetch_current_weather_for_city(city_name)
            # Add original CSV data for robust joining
            df_city["id"] = city_dict["id"]
            df_city["city_csv"] = city_dict["city"]
            df_city["country_csv"] = city_dict["country"]
            all_rows.append(df_city)
        except Exception as e:
            print(f"[WARN] Failed to fetch weather for {city_name}: {e}")

    if not all_rows:
        return pd.DataFrame()  # empty if all failed

    return pd.concat(all_rows, ignore_index=True)
