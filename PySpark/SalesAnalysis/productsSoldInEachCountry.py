from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize the Spark session
spark = SparkSession.builder.appName("Product Sales by Country").getOrCreate()

# Load the CSV into a DataFrame
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Group by the 'Country' column and count the number of products sold (distinct 'Product' count for each country)
country_product_count = df.groupBy("Country").agg({"Product": "count"}).withColumnRenamed("count(Product)", "ProductsSold")

# Show the result in a tabular format
country_product_count.show()

# Stop the Spark session
spark.stop()
