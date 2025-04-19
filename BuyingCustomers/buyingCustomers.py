from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerTotalAmountSpent").getOrCreate()

# File paths
customers_file = "customers.csv"
orders_file = "orders.csv"

# Load customers and orders data into DataFrames
customers_df = spark.read.option("header", "true").csv(customers_file)
orders_df = spark.read.option("header", "true").csv(orders_file)

# Cast appropriate columns to correct types
customers_df = customers_df.withColumn("ID", customers_df["ID"].cast("int"))
orders_df = orders_df.withColumn("ID", orders_df["ID"].cast("int"))
orders_df = orders_df.withColumn("AMOUNT", orders_df["AMOUNT"].cast("int"))

# Perform an inner join on customers and orders by ID
customer_orders_df = customers_df.join(orders_df, "ID")

# Group by CustomerID and calculate the total amount spent by each customer
total_spent_df = customer_orders_df.groupBy("ID", "NAME", "AGE", "SALARY").agg(sum("AMOUNT").alias("TOTAL_SPENT"))

# Show the result
total_spent_df.show()

# Stop the Spark session
spark.stop()
