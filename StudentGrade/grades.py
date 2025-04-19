from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when

# Initialize Spark session
spark = SparkSession.builder.appName("StudentGrades").getOrCreate()

# Load the CSV
file_path = "marks.csv"
df = spark.read.option("header", "true").csv(file_path, inferSchema=True)

# Calculate total and average marks
df_with_scores = df.withColumn("Total", 
    col("English") + col("Hindi") + col("Maths") + col("Science") + col("Social Science")
).withColumn("Average", col("Total") / 5)

# Assign grades based on average
df_with_grades = df_with_scores.withColumn("Grade", when(col("Average") >= 90, "A")
                                           .when(col("Average") >= 80, "B")
                                           .when(col("Average") >= 70, "C")
                                           .when(col("Average") >= 60, "D")
                                           .when(col("Average") >= 50, "E")
                                           .otherwise("F"))

# Select relevant columns
result_df = df_with_grades.select("StudentID", "Name", "Total", "Average", "Grade")

# Show the results
result_df.show(truncate=False)

# Stop Spark session
spark.stop()
