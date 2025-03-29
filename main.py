import time
import requests
import psycopg2
import pandas as pd
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import col

# ------------------------------------------------------------------
# 1) Spark session
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("TomorrowIoBatchApp") \
    .getOrCreate()

# ------------------------------------------------------------------
# 2) Define schema
# ------------------------------------------------------------------
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

# ------------------------------------------------------------------
# 3) Tomorrow.io API URL
# ------------------------------------------------------------------
api_url = "https://api.tomorrow.io/v4/weather/realtime?location=almaty&apikey=MdtJ4G3OgyB7Dk7oitngojT7iNYpYGQ4"

def fetch_tomorrowio_data():
    """
    Fetch JSON data from Tomorrow.io.
    Returns a Python dict if successful, or an empty dict on error.
    """
    headers = {
        "accept": "application/json",
        "accept-encoding": "deflate, gzip, br"
    }
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data. Status code: {response.status_code}")
        return {}

def force_values_to_floats(data_dict):
    """
    Convert integer fields in data_dict["data"]["values"] to float,
    so PySpark won't complain about DoubleType receiving int.
    """
    if "data" in data_dict and "values" in data_dict["data"]:
        for key, val in data_dict["data"]["values"].items():
            if isinstance(val, int):
                data_dict["data"]["values"][key] = float(val)
    return data_dict

# ------------------------------------------------------------------
# 4) PostgreSQL writing function
# ------------------------------------------------------------------
def write_to_postgres(spark_df):
    pdf = spark_df.toPandas()
    if pdf.empty:
        print("No data to write.")
        return
    conn = None 
    try:
        conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="weather_data",
        user="amira",
        password="cookie"
    )
        conn.autocommit = True
        cur = conn.cursor()

    

        # Access nested data correctly
        row = pdf.iloc[0]
        record_time = datetime.strptime(row['data']['time'].rstrip("Z"), "%Y-%m-%dT%H:%M:%S")
        cloudBase = row['data']['values']['cloudBase']
        cloudCeiling = row['data']['values']['cloudCeiling']
        cloudCover = row['data']['values']['cloudCover']
        dewPoint = row['data']['values']['dewPoint']
        freezingRainIntensity = row['data']['values']['freezingRainIntensity']
        hailProbability = row['data']['values']['hailProbability']
        hailSize = row['data']['values']['hailSize']
        humidity = row['data']['values']['humidity']
        precipitationProbability = row['data']['values']['precipitationProbability']
        pressureSeaLevel = row['data']['values']['pressureSeaLevel']
        pressureSurfaceLevel = row['data']['values']['pressureSurfaceLevel']
        rainIntensity = row['data']['values']['rainIntensity']
        sleetIntensity = row['data']['values']['sleetIntensity']
        snowIntensity = row['data']['values']['snowIntensity']
        temperature = row['data']['values']['temperature']
        temperatureApparent = row['data']['values']['temperatureApparent']
        uvHealthConcern = row['data']['values']['uvHealthConcern']
        uvIndex = row['data']['values']['uvIndex']
        visibility = row['data']['values']['visibility']
        weatherCode = row['data']['values']['weatherCode']
        windDirection = row['data']['values']['windDirection']
        windGust = row['data']['values']['windGust']
        windSpeed = row['data']['values']['windSpeed']
        lat = row['location']['lat']
        lon = row['location']['lon']
        location_name = row['location']['name']
        location_type = row['location']['type']

        # Prepare insert query and parameters
        insert_query = """
        INSERT INTO tomorrowio_data 
        (time, cloudBase, cloudCeiling, cloudCover, dewPoint, freezingRainIntensity, hailProbability, hailSize, humidity, precipitationProbability, 
         pressureSeaLevel, pressureSurfaceLevel, rainIntensity, sleetIntensity, snowIntensity, temperature, temperatureApparent, uvHealthConcern, 
         uvIndex, visibility, weatherCode, windDirection, windGust, windSpeed, lat, lon, location_name, location_type) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        params = (
            record_time, cloudBase, cloudCeiling, cloudCover, dewPoint, freezingRainIntensity, hailProbability, hailSize, humidity, 
            precipitationProbability, pressureSeaLevel, pressureSurfaceLevel, rainIntensity, sleetIntensity, snowIntensity, temperature, 
            temperatureApparent, uvHealthConcern, uvIndex, visibility, weatherCode, windDirection, windGust, windSpeed, lat, lon, 
            location_name, location_type
        )

        cur.execute(insert_query, params)
        print("Data successfully written to Postgres.")
        conn.close()

    except Exception as err:
        print(f"Error: {err}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

# ------------------------------------------------------------------
# 5) Main loop
# ------------------------------------------------------------------
def main():
    while True:
        print("Fetching data from Tomorrow.io...")
        data_dict = fetch_tomorrowio_data()

        if not data_dict:
            print("No data returned. Sleeping...")
            time.sleep(180)
            continue

        data_dict = force_values_to_floats(data_dict)
        df = spark.createDataFrame([data_dict], schema=weather_schema)
        df = df.withColumn("timestamp", col("data.time"))

        write_to_postgres(df)
        time.sleep(180)

if __name__ == "__main__":
    main()
