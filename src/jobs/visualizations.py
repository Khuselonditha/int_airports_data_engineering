# imports
import pycountry
from pyspark.sql import SparkSession ,functions as F, types
from functools import reduce
from fuzzywuzzy import process


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
             "latitude_coord", "longitude_coord"
             )

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

# function for correcting countries
def correct_country_name(name, threshold=85):
    countries = [country.name for country in pycountry.countries]

    correct_name, score = process.extractOne(name, countries)

    if score >= threshold:
        return correct_name
    
    return name

# Register the function as a UDF
correct_country_name_udf = F.udf(correct_country_name, types.StringTypes())

# Apply the UDF to the 'country' column to correct country names
df_filtered = df_filtered.withColumn('country', correct_country_name_udf(df_filtered['country']))

# show dataframe
df_filtered.show(truncate=False)

# Stop spark
spark.stop()