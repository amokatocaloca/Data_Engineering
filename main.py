import time
import requests
import psycopg2
import pandas as pd
import traceback
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

# ── Simplified logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ── Spark session ────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("TomorrowIoBatchApp") \
    .getOrCreate()

# ── Define schema ─────────────────────────────────────────────────────────
weather_schema = StructType([
    StructField("data", StructType([
        StructField("time", StringType(), True),
        StructField("values", StructType([
            StructField("cloudBase", DoubleType(), True),
            StructField("cloudCeiling", DoubleType(), True),
            StructField("cloudCover", DoubleType(), True),
            StructField("dewPoint", DoubleType(), True),
            StructField("freezingRainIntensity", DoubleType(), True),
            StructField("hailProbability", DoubleType(), True),
            StructField("hailSize", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("precipitationProbability", DoubleType(), True),
            StructField("pressureSeaLevel", DoubleType(), True),
            StructField("pressureSurfaceLevel", DoubleType(), True),
            StructField("rainIntensity", DoubleType(), True),
            StructField("sleetIntensity", DoubleType(), True),
            StructField("snowIntensity", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("temperatureApparent", DoubleType(), True),
            StructField("uvHealthConcern", DoubleType(), True),
            StructField("uvIndex", DoubleType(), True),
            StructField("visibility", DoubleType(), True),
            StructField("weatherCode", DoubleType(), True),
            StructField("windDirection", DoubleType(), True),
            StructField("windGust", DoubleType(), True),
            StructField("windSpeed", DoubleType(), True),
        ]), True)
    ]), True),
    StructField("location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
    ]), True)
])

# ── Tomorrow.io API URL & settings ─────────────────────────────────────────
API_URL = (
    "https://api.tomorrow.io/v4/weather/realtime"
    "?location=almaty&apikey=MdtJ4G3OgyB7Dk7oitngojT7iNYpYGQ4"
)
POLL_INTERVAL = 180  # seconds


def fetch_tomorrowio_data():
    """
    Fetch JSON data from Tomorrow.io.
    Returns a Python dict on success, or an empty dict on error.
    """
    try:
        resp = requests.get(API_URL, timeout=10)
        resp.raise_for_status()
        logging.info("Fetched data from Tomorrow.io successfully")
        return resp.json()
    except Exception as e:
        logging.error("Error fetching Tomorrow.io data: %s", e)
        return {}


def force_values_to_floats(data):
    """
    Convert integer fields in data['data']['values'] to floats
    so they match the DoubleType schema.
    """
    for key, val in data.get("data", {}).get("values", {}).items():
        if isinstance(val, int):
            data["data"]["values"][key] = float(val)
    return data


def write_to_postgres(df):
    """
    Write a single-row DataFrame to PostgreSQL. Uses parameterized INSERT.
    """
    pdf = df.toPandas()
    if pdf.empty:
        logging.warning("No data to write to Postgres")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres", port="5432",
            database="weather_data", user="amira", password="cookie"
        )
        conn.autocommit = True
        cur = conn.cursor()

        row = pdf.iloc[0]
        record_time = datetime.strptime(
            row['data']['time'].rstrip("Z"), "%Y-%m-%dT%H:%M:%S"
        )
        vals = row['data']['values']
        loc = row['location']

        insert_query = (
            "INSERT INTO tomorrowio_data ("
            "time, cloudBase, cloudCeiling, cloudCover, dewPoint, freezingRainIntensity, "
            "hailProbability, hailSize, humidity, precipitationProbability, pressureSeaLevel, "
            "pressureSurfaceLevel, rainIntensity, sleetIntensity, snowIntensity, temperature, "
            "temperatureApparent, uvHealthConcern, uvIndex, visibility, weatherCode, "
            "windDirection, windGust, windSpeed, lat, lon, location_name, location_type) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        )
        params = (
            record_time,
            vals['cloudBase'], vals['cloudCeiling'], vals['cloudCover'], vals['dewPoint'],
            vals['freezingRainIntensity'], vals['hailProbability'], vals['hailSize'],
            vals['humidity'], vals['precipitationProbability'], vals['pressureSeaLevel'],
            vals['pressureSurfaceLevel'], vals['rainIntensity'], vals['sleetIntensity'],
            vals['snowIntensity'], vals['temperature'], vals['temperatureApparent'],
            vals['uvHealthConcern'], vals['uvIndex'], vals['visibility'], vals['weatherCode'],
            vals['windDirection'], vals['windGust'], vals['windSpeed'],
            loc['lat'], loc['lon'], loc['name'], loc['type']
        )

        cur.execute(insert_query, params)
        logging.info("Data successfully written to Postgres for %s", record_time)

    except Exception as e:
        logging.error("Error writing to Postgres: %s", e)
    finally:
        if conn:
            conn.close()


def main():
    while True:
        logging.info("Starting fetch-transform-write cycle")
        data = fetch_tomorrowio_data()
        if not data:
            logging.info("No data fetched, sleeping for %s seconds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        data = force_values_to_floats(data)
        try:
            df = spark.createDataFrame([data], schema=weather_schema)
            df = df.withColumn("timestamp", col("data.time"))
            write_to_postgres(df)
        except Exception as e:
            logging.error("Processing or write failed: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
