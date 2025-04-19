from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("WeatherClassification").getOrCreate()

# Load the weather data CSV
file_path = "data.csv"
df = spark.read.option("header", "true").csv(file_path, inferSchema=True)

# Define classification rules for "Shiny" and "Cool" days
df_with_classification = df.withColumn(
    "Weather_Type", 
    when((col("Temperature_C") >= 28) & (col("Sunshine_hours") >= 6) & (col("Humidity_%") <= 60), "Shiny")
    .when((col("Temperature_C") <= 24) & (col("Sunshine_hours") <= 4) & (col("Humidity_%") >= 60), "Cool")
    .otherwise("Unknown")  # In case the day does not fit either category
)

# Select the relevant columns and show the result
result_df = df_with_classification.select("Date", "Weather_Type")
result_df.show(truncate=False)

# Stop Spark session
spark.stop()
