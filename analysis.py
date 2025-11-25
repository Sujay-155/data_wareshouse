import pandas as pd
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

def analyze_csv(file_path):
    print(f"--- Analyzing {file_path} ---")
    df = pd.read_csv(file_path)
    
    print(f"Shape: {df.shape}")
    print("\nColumns & Types:")
    print(df.dtypes)
    
    print("\nMissing Values:")
    print(df.isnull().sum())
    
    print("\nDuplicate Rows:", df.duplicated().sum())
    
    # Check Primary Key candidates
    if 'id' in df.columns:
        unique_ids = df['id'].nunique()
        print(f"\nUnique 'id' count: {unique_ids}")
        if unique_ids == len(df):
            print("-> 'id' is a valid Primary Key.")
        else:
            print("-> 'id' is NOT unique.")
            
    # Check City+Country uniqueness
    if 'city' in df.columns and 'country' in df.columns:
        dupes = df.duplicated(subset=['city', 'country'], keep=False)
        print(f"\nDuplicate City+Country combinations: {dupes.sum()}")
        if dupes.sum() > 0:
            print("Examples of duplicates (same city name in same country):")
            print(df[dupes][['city', 'country', 'admin_name', 'lat', 'lng']].sort_values(by=['country', 'city']).head(10))

    return df

def analyze_weather_api_structure(city_name="London"):
    print(f"\n--- Analyzing Weather API Structure for {city_name} ---")
    url = "http://api.weatherapi.com/v1/current.json"
    params = {
        "key": WEATHER_API_KEY,
        "q": city_name,
        "aqi": "yes" # Let's check AQI structure too
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        print("Top level keys:", list(data.keys()))
        if 'location' in data:
            print("\n'location' keys:", list(data['location'].keys()))
        if 'current' in data:
            print("\n'current' keys:", list(data['current'].keys()))
            if 'condition' in data['current']:
                print("'current.condition' keys:", list(data['current']['condition'].keys()))
            if 'air_quality' in data['current']:
                print("'current.air_quality' keys:", list(data['current']['air_quality'].keys()))
                
        return data
    except Exception as e:
        print(f"API Error: {e}")
        return None

if __name__ == "__main__":
    # 1. Analyze CSV
    csv_path = "data/worldcities.csv"
    if os.path.exists(csv_path):
        df = analyze_csv(csv_path)
    else:
        print(f"File not found: {csv_path}")

    # 2. Analyze API
    if WEATHER_API_KEY:
        analyze_weather_api_structure("Tokyo")
    else:
        print("No API Key found.")
