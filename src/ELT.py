# import datetime as dt
# import requests
# import pandas as pd
# from sqlalchemy import create_engine


# def extract() -> str:
#     URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
#     file_name = "nyc_taxi_data.parquet"

#     response = requests.get(URL)
#     if response.status_code == 200:
#         with open(file_name, "wb") as file:
#             file.write(response.content)
#         print(f"Downloaded {file_name}")
#     else:
#         print("Failed to download data")
#     return file_name


# def transform(file_name: str):
#     df = pd.read_parquet(file_name)

#     # Select useful columns
#     df = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'fare_amount', 'tip_amount']]

#     # Convert timestamps
#     df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
#     df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

#     # Remove null values
#     df = df.dropna()

#     # Add a new column: trip duration in minutes
#     df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60

#     # Print preview
#     print(df.head())


# def load():
#     DB_PATH = "nyc_taxi.db"
#     engine = create_engine("postgresql://username:password@localhost:5432/nyc_taxi")


#     # Store the cleaned data
#     df.to_sql("nyc_taxi_data", con=engine, if_exists="replace", index=False)

#     print(f"Data loaded into {DB_PATH} database.")

# def query():
#     query = "SELECT * FROM nyc_taxi_data LIMIT 5"
#     df_new = pd.read_sql(query, con=engine)
#     print(df_new)

# file_name = "nyc_taxi_data.parquet"
# # file = extract()
# transform(file_name)


import datetime

timestamp = 1738760277
# Convert Unix timestamp to human-readable date
date = datetime.datetime.utcfromtimestamp(timestamp)
print(date)
