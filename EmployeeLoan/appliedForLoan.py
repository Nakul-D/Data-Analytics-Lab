from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("EmployeeLoanStatus").getOrCreate()

# File paths
employees_file = "employees.csv"
loans_file = "loans.csv"

# Load the data into DataFrames
employees_df = spark.read.option("header", "true").csv(employees_file)
loans_df = spark.read.option("header", "true").csv(loans_file)

# Cast LoanAmount to integer and LoanApplied to boolean
loans_df = loans_df.withColumn("LoanAmount", loans_df["LoanAmount"].cast("int")) \
                   .withColumn("LoanApplied", loans_df["LoanApplied"].cast("boolean"))

# Join the two DataFrames on EmployeeID
joined_df = employees_df.join(loans_df, "EmployeeID")

# Filter out employees who have applied for a loan (LoanApplied = true)
employees_with_loans = joined_df.filter(joined_df["LoanApplied"] == True)

# Select relevant columns to display
employees_with_loans = employees_with_loans.select("EmployeeID", "Name", "LoanAmount")

# Show the result
employees_with_loans.show()

# Stop the Spark session
spark.stop()
