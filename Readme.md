# Weather and Airbnb Data Analysis

This project aims to create an ETL pipeline that can be used for exploring how weather conditions influence Airbnb bookings in Vienna. By analyzing historical weather data and Airbnb listing data across different seasons, the goal can be to identify correlations between seasonal weather changes and tourism demand.

## Table of Contents

- [Project Description](#project-description)
- [Features](#features)
- [Setup Instructions](#setup-instructions)
- [Usage Instructions](#usage-instructions)
- [File Structure](#file-structure)

## Project Description

The project combines two datasets:

1. **Airbnb Listings**: Data from Airbnb listings in Vienna over multiple seasons.
2. **Weather Data**: Historical weather data for Vienna, sourced from OpenWeatherMap API.

By analyzing both datasets together, this project aims to explore how temperature, humidity, and other weather conditions correlate with the number of bookings on Airbnb.

### Key Goals:
- Collect and process Airbnb listing data.
- Fetch weather data for Vienna across different seasons.
- Clean and transform the data for analysis.
- Load the data into a PostgreSQL database.

## Features

- **Weather Data Fetching**: Uses OpenWeatherMap’s API to retrieve historical weather data for a given city.
- **Airbnb Listings Transformation**: Processes and transforms Airbnb listing data to be consistent across different seasons.
- **Database Integration**: Loads processed data into a PostgreSQL database using SQLAlchemy.
- **CSV Data Export**: Saves the processed data to CSV files for easier analysis.

## Setup Instructions

### 1. Clone the Repository

First, clone the repository to your local machine:

```bash
git clone <repository_url>
cd <project_directory>
```

### 2. Create a Virtual Environment

It's a good practice to create a virtual environment for Python dependencies:

```bash
python3 -m venv venv
```

### 3. Activate the Virtual Environment

- **macOS/Linux:**

  ```bash
  source venv/bin/activate
  ```

- **Windows:**

  ```bash
  .\venv\Scripts\activate
  ```

### 4. Install Required Dependencies

Install all necessary packages using `pip`:

```bash
pip install -r requirements.txt
```

### 5. Set Up Environment Variables

Create a `.env` file in the root of the project directory, and add the following environment variables:

```ini
PGPASS=your_postgresql_password
PGUID=your_postgresql_user
api_key=your_openweathermap_api_key
```

Make sure to replace the placeholders with your actual credentials.

### 6. Set Up PostgreSQL Database

You need a PostgreSQL database to store the processed data. You can set up a database using the following commands:

1. Open the PostgreSQL command line:
   ```bash
   psql -U postgres
   ```

2. Create the database:
   ```sql
   CREATE DATABASE vienna_data;
   ```

3. Exit PostgreSQL:
   ```bash
   \q
   ```

## Usage Instructions

### 1. Run the Script

After setting up the environment and the database, you can run the script using the following command:

```bash
python main.py
```

This will:

- Fetch the weather data for Vienna for the specified dates.
- Process the Airbnb listings from the CSV files for different seasons.
- Save both datasets into a PostgreSQL database under the `listings` and `weather` tables.

### 2. Query Data from the Database

Once the data is loaded into the PostgreSQL database, you can query it using `psql` or any other database management tool like PgAdmin:

```bash
psql -U postgres -d vienna_data
```

For example, to retrieve the listings data:

```sql
SELECT * FROM listings;
```

Or to retrieve the weather data:

```sql
SELECT * FROM weather;
```

## File Structure

```
project_directory/
│
├── data/
│   ├── input/               # Input CSV files for Airbnb listings
│   └── output/              # Output CSV files for processed data
│
├── venv/                    # Virtual environment folder
├── .env                     # Environment variables for sensitive data
├── main.py                  # Main script for the project
├── requirements.txt         # List of Python dependencies
└── README.md                # Project documentation
```
