from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("MaxTemperaturePerYearMonth").getOrCreate()

# Path to the CSV file (replace with the correct file path if needed)
file_path = "temperatures.csv"

# Load the data from the CSV file
temperature_df = spark.read.option("header", "true").csv(file_path)

# Cast appropriate columns to correct types
temperature_df = temperature_df.withColumn("Year", temperature_df["Year"].cast("int"))
temperature_df = temperature_df.withColumn("AvgTemperature", temperature_df["AvgTemperature"].cast("double"))

# Create a window specification to find the max temperature per year
window_spec = Window.partitionBy("Year")

# Find the maximum temperature per year and the corresponding month
max_temp_df = temperature_df.withColumn("MaxTemperature", max("AvgTemperature").over(window_spec))

# Filter the data to get rows where temperature equals max temperature for each year
max_temp_df_filtered = max_temp_df.filter(col("AvgTemperature") == col("MaxTemperature"))

# Collect the results to loop and format output in a table-like format
results = max_temp_df_filtered.select("Year", "Month", "MaxTemperature").distinct().collect()

# Print the header of the table
print(f"{'Year':<6} {'Max Temperature':<15} {'Month'}")

# Print each row in the table
for row in results:
    print(f"{row['Year']:<6} {row['MaxTemperature']:<15} {row['Month']}")

# Stop the Spark session
spark.stop()
