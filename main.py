# main.py
import pandas as pd

from etl.extract import load_city_csv, fetch_current_weather_for_cities

def main():
    # ---------- 1. Read Kaggle CSV ----------
    cities_path = "data/worldcities.csv"

    print("=== Loading city data from CSV ===")
    cities_df = load_city_csv(cities_path)

    print("\n[City DataFrame - first 10 rows (raw)]")
    print(cities_df.head(10))
    print("\n[City DataFrame - info (raw)]")
    print(cities_df.info())

    required_cols = ["id", "country", "city", "population", "capital", "lat", "lng"]

    missing = [c for c in required_cols if c not in cities_df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in CSV: {missing}")

    # Keep only the columns we care about from the CSV
    cities_df = cities_df[required_cols].copy()

    # Rename lng -> lon so it matches WeatherAPI
    cities_df = cities_df.rename(columns={"lng": "lon"})

    print("\n[City DataFrame - trimmed columns]")
    print(cities_df.head(10))
    print("\n[City DataFrame - info (trimmed)]")
    print(cities_df.info())

    # ---------- 2. Build sample city list for Weather API ----------
    # Use "city, country" format to reduce ambiguity ("Delhi, India")
    sample_df = cities_df.head(10)
    sample_cities = (sample_df["city"].astype(str) + ", " + sample_df["country"].astype(str)).tolist()
    print(f"\nSample cities we will use for WeatherAPI calls: {sample_cities}")

    # ---------- 3. Fetch Weather from API ----------
    print("\n=== Fetching current weather for sample cities ===")
    weather_df = fetch_current_weather_for_cities(sample_cities)

    print("\n[Weather DataFrame - raw]")
    print(weather_df)
    print("\n[Weather DataFrame - info (raw)]")
    print(weather_df.info())

    if weather_df.empty:
        print("\nNo weather data returned, cannot merge.")
        return

    # ---------- 4. Prepare both DataFrames for merge on lat/lon ----------
    # Round coordinates to whole degrees for broader matching
    cities_df["lat_round"] = cities_df["lat"].round(0)
    cities_df["lon_round"] = cities_df["lon"].round(0)

    weather_df["lat_round"] = weather_df["lat"].round(0)
    weather_df["lon_round"] = weather_df["lon"].round(0)

    # Keep only needed weather columns + rounded coords
    weather_df = weather_df[[
        "lat_round",
        "lon_round",
        "temp_c",
        "humidity",
        "condition_text",
        "aqi",
        "city_name_api",
        "country_api"
    ]].copy()

    print("\n[Weather DataFrame - trimmed for merge]")
    print(weather_df.head())

    # ---------- 5. Merge on lat_round & lon_round ----------
    merged_df = cities_df.merge(
        weather_df,
        on=["lat_round", "lon_round"],
        how="inner",  # only cities that have both CSV + weather match
        suffixes=("_csv", "_api")
    )

    # You can drop lat_round/lon_round now or keep them
    # Also we still have original lat/lon from cities_df
    merged_df = merged_df.drop(columns=["lat_round", "lon_round"])

    print("\n=== Merged DataFrame (cities + weather) ===")
    print(merged_df.head(10))

    print("\n[Merged DataFrame - info]")
    print(merged_df.info())

    # ---------- 6. Final Output: id, Country, City, population, AQI, Temperature, Condition ----------
    final_df = merged_df[["id", "country", "city", "population", "aqi", "temp_c", "condition_text"]].copy()
    final_df = final_df.rename(columns={
        "country": "Country",
        "city": "City",
        "aqi": "AQI",
        "temp_c": "Temperature",
        "condition_text": "Condition"
    })

    print("\n=== Final Output ===")
    print(final_df)


if __name__ == "__main__":
    main()

