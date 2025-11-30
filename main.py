# main.py
import pandas as pd

from etl.extract import load_city_csv, fetch_current_weather_for_cities
from etl.load import load_to_postgres

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

    # Round coordinates to whole degrees for broader matching
    cities_df["lat_round"] = cities_df["lat"].round(0)
    cities_df["lon_round"] = cities_df["lon"].round(0)

    # Deduplicate cities based on rounded lat/lon to ensure unique cities per coordinate
    cities_df = cities_df.drop_duplicates(subset=["lat_round", "lon_round"])

    print("\n[City DataFrame - after deduplication]")
    print(cities_df.head(10))
    print("\n[City DataFrame - info (after deduplication)]")
    print(cities_df.info())

    # ---------- 2. Build sample city list for Weather API ----------
    # Create sample DataFrame - using 100 cities for a more useful dataset
    # Increase this number for more data (but be mindful of API rate limits)
    NUM_CITIES = 100
    sample_df = cities_df.head(NUM_CITIES)
    # Create list of dicts with id, city, country for robust mapping
    sample_cities = [
        {"id": row["id"], "city": row["city"], "country": row["country"]}
        for _, row in sample_df.iterrows()
    ]
    print(f"\nFetching weather for {len(sample_cities)} cities...")

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

    # ---------- 4. Merge on id (robust mapping from CSV to API) ----------
    merged_df = cities_df.merge(
        weather_df,
        on="id",
        how="inner",  # only cities that have weather data
        suffixes=("_csv", "_api")
    )

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

    # ---------- 7. Load to PostgreSQL ----------
    load_to_postgres(final_df, merged_df)


if __name__ == "__main__":
    main()

