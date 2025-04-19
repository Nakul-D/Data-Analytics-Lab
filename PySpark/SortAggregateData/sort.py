from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("salesData.csv", header=True, inferSchema=True)

# Top 10 Most Expensive Sales (Sorted by Amount)
top_10_sales = df.orderBy(desc("Amount")).limit(10)

# Display top 10 most expensive sales with a print statement
top_10_sales.show()

# Stop the Spark session
spark.stop()
