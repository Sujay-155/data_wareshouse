# etl/load.py
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

from .config import DB_CONFIG


def get_engine():
    """Create SQLAlchemy engine for PostgreSQL connection."""
    # URL-encode the password to handle special characters like @
    encoded_password = quote_plus(DB_CONFIG['password'])
    connection_string = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{encoded_password}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(connection_string)


def load_dim_city(df: pd.DataFrame, merged_df: pd.DataFrame):
    """
    Load city dimension data into dim_city table.
    Expects merged_df to have: id, city, country, population, capital, lat, lon
    """
    engine = get_engine()
    
    # Prepare data for dim_city from merged_df (has all columns)
    dim_city_df = merged_df[["id", "city", "country", "population", "capital", "lat_csv", "lon_csv"]].copy()
    dim_city_df = dim_city_df.rename(columns={
        "id": "city_id",
        "city": "city_name",
        "lat_csv": "latitude",
        "lon_csv": "longitude"
    })
    
    # Insert into dim_city (using upsert logic to avoid duplicates)
    with engine.connect() as conn:
        for _, row in dim_city_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO dim_city (city_id, city_name, country, population, capital, latitude, longitude)
                    VALUES (:city_id, :city_name, :country, :population, :capital, :latitude, :longitude)
                    ON CONFLICT (city_id) DO UPDATE SET
                        city_name = EXCLUDED.city_name,
                        country = EXCLUDED.country,
                        population = EXCLUDED.population,
                        capital = EXCLUDED.capital,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude
                """),
                {
                    "city_id": int(row["city_id"]),
                    "city_name": row["city_name"],
                    "country": row["country"],
                    "population": int(row["population"]) if pd.notna(row["population"]) else None,
                    "capital": row["capital"] if pd.notna(row["capital"]) else None,
                    "latitude": float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                    "longitude": float(row["longitude"]) if pd.notna(row["longitude"]) else None
                }
            )
        conn.commit()
    
    print(f"[LOAD] Inserted/Updated {len(dim_city_df)} rows into dim_city")


def load_fact_weather(df: pd.DataFrame, merged_df: pd.DataFrame):
    """
    Load weather fact data into fact_weather table.
    Expects merged_df to have: id, temp_c, humidity, condition_text
    """
    engine = get_engine()
    
    # Prepare data for fact_weather
    fact_weather_df = merged_df[["id", "temp_c", "humidity", "condition_text"]].copy()
    fact_weather_df = fact_weather_df.rename(columns={
        "id": "city_id",
        "temp_c": "temp_c"
    })
    
    obs_time = datetime.now()
    
    # Insert into fact_weather (append new records)
    with engine.connect() as conn:
        for _, row in fact_weather_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO fact_weather (city_id, obs_time, temp_c, humidity, condition_text)
                    VALUES (:city_id, :obs_time, :temp_c, :humidity, :condition_text)
                """),
                {
                    "city_id": int(row["city_id"]),
                    "obs_time": obs_time,
                    "temp_c": float(row["temp_c"]) if pd.notna(row["temp_c"]) else None,
                    "humidity": int(row["humidity"]) if pd.notna(row["humidity"]) else None,
                    "condition_text": row["condition_text"]
                }
            )
        conn.commit()
    
    print(f"[LOAD] Inserted {len(fact_weather_df)} rows into fact_weather")


def load_to_postgres(final_df: pd.DataFrame, merged_df: pd.DataFrame):
    """Load final DataFrame into both dim_city and fact_weather tables."""
    print("\n=== Loading data to PostgreSQL ===")
    load_dim_city(final_df, merged_df)
    load_fact_weather(final_df, merged_df)
    print("[LOAD] Data loading complete!")
