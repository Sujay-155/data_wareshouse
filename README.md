# Weather Data Warehouse

## Overview
This project is a straightforward ETL pipeline that pulls real-time weather data from WeatherAPI, combines it with city details from a CSV file, cleans everything up, and loads it into a PostgreSQL database. It's designed for easy setup and reliable daily updates.

## Features
- Fetches weather data for cities using WeatherAPI
- Merges and cleans data with Pandas
- Stores data in a simple star schema (cities and weather facts)
- Runs automatically every day at 11 AM with a scheduler

## Setup
1. Make sure you have Python (3.8+) and PostgreSQL installed.
2. Clone or download this repo.
3. Install the required packages: `pip install -r requirements.txt`
4. Create a `.env` file in the root folder with your WeatherAPI key and database details (like host, user, password, and database name).
5. Set up your PostgreSQL database (create a database called `weather_dw` if you want to match the code).

## Usage
- To run the ETL once: `python main.py`
- For daily automated runs: `python scheduler.py` (it'll run at 11 AM every day)

That's it! The pipeline handles the rest, from fetching data to storing it safely in the database.