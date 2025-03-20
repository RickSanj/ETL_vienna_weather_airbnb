"""
Project: Impact of Weather Conditions on Airbnb Bookings
Analyzes Airbnb listing data alongside historical weather data 
to explore correlations between seasonal weather changes and tourism demand.
"""

import os
import csv
import datetime as dt
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from geopy.geocoders import Photon

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def configure():
    """Loads environment variables from .env file."""
    load_dotenv()


def get_db_engine():
    """Creates a database connection."""
    pwd = os.getenv("PGPASS")
    uid = os.getenv("PGUID")
    server = "localhost"
    port = "5432"
    database = "vienna_data"

    if not pwd or not uid:
        logging.error("Missing database credentials (PGPASS or PGUID).")
        return None

    try:
        engine = create_engine(
            f"postgresql://{uid}:{pwd}@{server}:{port}/{database}")
        return engine
    except Exception as err:
        logging.error(f"Database connection failed: {err}")
        return None


def get_lon_and_lat(city: str):
    """Finds the latitude and longitude for a given city."""
    try:
        geolocator = Photon(user_agent="geoapiExercises", timeout=10)
        location = geolocator.geocode(city)

        if not location:
            logging.error(f"Could not find coordinates for {city}.")
            return None, None

        return location.latitude, location.longitude
    except Exception as e:
        logging.error(f"Geolocation error: {e}")
        return None, None


def get_weather_data(date_: str, city: str):
    """Fetches historical weather data for a given date and city."""
    lat, lon = get_lon_and_lat(city)
    if lat is None or lon is None:
        return None

    timestamp = int(dt.datetime.strptime(date_, "%Y-%m-%d").timestamp())
    api_key = os.getenv("api_key")

    url = (
        f"https://history.openweathermap.org/data/2.5/history/city?"
        f"lat={lat}&lon={lon}&type=hour&start={timestamp}&end={timestamp}&appid={api_key}"
    )

    response = requests.get(url, timeout=10)
    if response.status_code == 200:
        return response.json()

    logging.error(
        f"Error fetching weather for {city} on {date_}: {response.status_code}")
    return None


def kelvin_to_celsius(temp_kel: float) -> float:
    """Converts temperature from Kelvin to Celsius."""
    return round(temp_kel - 273.15, 2)


def process_weather(dates: list, city: str):
    """Processes weather data for multiple dates and saves as CSV."""
    all_data = {}

    for date in dates:
        weather_data = get_weather_data(date, city)
        if weather_data:
            all_data[date] = weather_data
        else:
            logging.warning(f"Skipping {date} due to missing data.")

    return transform_weather(all_data, f"./data/output/{city.lower()}_weather.csv")


def transform_weather(data: dict, csv_filename: str):
    """Transforms and saves weather data to a CSV file."""
    fieldnames = [
        "date", "temp", "feels_like", "pressure", "humidity",
        "temp_min", "temp_max", "wind_speed", "clouds", "rain"
    ]

    with open(csv_filename, "w", newline="", encoding="UTF-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for date, details in data.items():
            if "list" in details and len(details["list"]) > 0:
                weather_entry = details["list"][0]

                row = {
                    "date": date,
                    "temp": kelvin_to_celsius(weather_entry["main"]["temp"]),
                    "feels_like": kelvin_to_celsius(weather_entry["main"]["feels_like"]),
                    "pressure": weather_entry["main"]["pressure"],
                    "humidity": weather_entry["main"]["humidity"],
                    "temp_min": kelvin_to_celsius(weather_entry["main"]["temp_min"]),
                    "temp_max": kelvin_to_celsius(weather_entry["main"]["temp_max"]),
                    "wind_speed": weather_entry["wind"]["speed"],
                    "clouds": weather_entry["clouds"]["all"],
                    "rain": weather_entry.get("rain", {}).get("1h", 0),
                }

                writer.writerow(row)

    logging.info(f"Weather data saved to {csv_filename}")
    return csv_filename


def transform_listing(file_name: str, date: str) -> pd.DataFrame:
    """Cleans and formats Airbnb listing data."""
    df = pd.read_csv(file_name)
    df.drop_duplicates(inplace=True)
    df = df[["id", "name", "room_type", "accommodates",
             "price", "bedrooms", "beds", "number_of_reviews"]]
    df["date"] = pd.to_datetime(date)
    return df


def load(df: pd.DataFrame, table_name: str):
    """Loads a DataFrame into PostgreSQL."""
    engine = get_db_engine()
    if engine is None:
        logging.error("Skipping database load due to connection failure.")
        return

    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Data loaded successfully into '{table_name}'.")
    except Exception as err:
        logging.error(f"Error loading data into '{table_name}': {err}")


def main():
    """Main ETL process."""
    dates = ["2024-03-22", "2024-06-15", "2024-09-11", "2024-12-12"]
    listings = [
        "./data/input/vienna_mar_listings.csv",
        "./data/input/vienna_jun_listings.csv",
        "./data/input/vienna_sep_listings.csv",
        "./data/input/vienna_dec_listings.csv"
    ]
    city = "Vienna"

    # Process Airbnb listings
    df_listings = pd.concat([transform_listing(listing, date)
                            for listing, date in zip(listings, dates)], ignore_index=True)
    df_listings.to_csv("./data/output/vienna_listings.csv",
                       mode="w", index=False)
    load(df_listings, "listings")

    # Process weather data
    weather_csv = process_weather(dates, city)
    df_weather = pd.read_csv(weather_csv)
    load(df_weather, "weather")


if __name__ == "__main__":
    configure()
    main()
