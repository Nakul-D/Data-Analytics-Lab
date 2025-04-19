from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("MaxConsumptionPerYear").getOrCreate()

# Load the CSV
file_path = "data.csv"
df = spark.read.option("header", "true").csv(file_path, inferSchema=True)

# Define a window partitioned by year and ordered by descending consumption
windowSpec = Window.partitionBy("Year").orderBy(col("Consumption_kWh").desc())

# Add a row number and filter for top 1 per year
df_with_row = df.withColumn("rank", row_number().over(windowSpec))
max_per_year = df_with_row.filter(col("rank") == 1).select("Year", "Month", "Consumption_kWh")

# Show the result
max_per_year.show()
