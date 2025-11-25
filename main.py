# main.py
import pandas as pd

from etl.extract import load_city_csv, fetch_current_weather_for_cities


def main():
    # ---------- 1. Read Kaggle CSV ----------
    cities_path = "data/worldcities.csv"

    print("=== Loading city data from CSV ===")
    cities_df = load_city_csv(cities_path)

    print("\n[City DataFrame - first 5 rows]")
    print(cities_df.head())           # show sample
    print("\n[City DataFrame - info]")
    print(cities_df.info())           # show columns & types

    # Decide which column holds city names
    # Adjust this if your CSV has different column names
    # e.g., 'city', 'City', 'name', etc.
    possible_city_cols = ["city", "City", "city_name", "City_Name"]
    city_col = None
    for col in possible_city_cols:
        if col in cities_df.columns:
            city_col = col
            break

    if city_col is None:
        raise ValueError(
            f"Could not find a city column in CSV. "
            f"Looked for {possible_city_cols}, found {list(cities_df.columns)}"
        )

    # Pick a small sample of cities to test API
    # We combine City + Country to avoid ambiguity (e.g. Delhi, Canada vs Delhi, India)
    sample_df = cities_df.head(3)
    sample_cities = (sample_df[city_col].astype(str) + ", " + sample_df["country"].astype(str)).tolist()
    print(f"\nSample cities we will use for WeatherAPI calls: {sample_cities}")

    # ---------- 2. Fetch Weather from API ----------
    print("\n=== Fetching current weather for sample cities ===")
    weather_df = fetch_current_weather_for_cities(sample_cities)

    print("\n[Weather DataFrame]")
    print(weather_df)

    print("\n[Weather DataFrame - info]")
    print(weather_df.info())


if __name__ == "__main__":
    main()
