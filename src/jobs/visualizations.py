# imports
from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.functions import col
from functools import reduce
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

# Check each row for 'U' in any column
df_filtered = df.filter(
    reduce(
        lambda acc, col: acc & ~F.trim(F.col(col)).isin("U", "N/A"),
        df.columns,
        F.lit(True)
    )
)

# Drop all null columns
df_filtered = df_filtered.dropna(how='any')

# Return preffered columns
df_filtered = df_filtered.select("ICAO_code", "IATA_code", "airport_name",
               "city", "country", "latitude_direction",
               "longitude_direction", "altitude",
               "latitude_coord", "longitude_coord")

# show dataframe
df_filtered.show(truncate=False)

# Stop spark
spark.stop()