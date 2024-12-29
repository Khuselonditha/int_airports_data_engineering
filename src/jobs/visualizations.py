# imports
from pyspark.sql import SparkSession
import pandas as pd

# Create spark instance
spark = SparkSession.builder.appName("End To End Processing").getOrCreate()

# Read the data from txt file
df = spark.read.option("delimiter", ":").csv("input/GlobalAirportDatabase.txt", header=False)

# Assign the column names
df = df.toDF("ICAO_code", "IATA_code",
             "airport_name", "city",
             "country", "latitude_degrees",
             "latitude_minutes", "latitude_seconds",
             "latitude_direction", "longitude_degrees",
             "longitude_minutes", "longitude_seconds",
             "longitude_direction", "altitude",
             "latitude_coord", "longitude_coord",)

# Drop all null columns
df = df.dropna(how='all')

# Return preffered columns
df = df.select("ICAO_code", "IATA_code", "airport_name",
               "city", "country", "latitude_direction",
               "longitude_direction", "altitude",
               "latitude_coord", "longitude_coord")

# show dataframe
df.show(truncate=False)

# Stop spark
spark.stop()