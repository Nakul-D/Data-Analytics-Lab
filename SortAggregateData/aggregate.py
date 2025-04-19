from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("salesData.csv", header=True, inferSchema=True)

# Sales Volume Aggregated by Day
sales_by_day = df.groupBy("Date").agg(sum("Amount").alias("Total_Sales")).orderBy("Date")

# Display sales volume by day with a print statement
sales_by_day.show()

# Stop the Spark session
spark.stop()
