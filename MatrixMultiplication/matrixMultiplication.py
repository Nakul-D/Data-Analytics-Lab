from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("MatrixMultiply5x5").getOrCreate()
sc = spark.sparkContext

# File paths
file_a = "matrixA.txt"
file_b = "matrixB.txt"

# Read file A and determine size n
lines_a = sc.textFile(file_a)
n = lines_a.first().count(' ') + 1

# Helper: Read matrix from txt and convert to ((i, j), val) format
def load_matrix(file_path):
    lines = sc.textFile(file_path)
    return lines.zipWithIndex().flatMap(
        lambda row: [((row[1], j), int(val)) for j, val in enumerate(row[0].split())]
    )

# Load both matrices
A = load_matrix(file_a)  # ((i, j), val)
B = load_matrix(file_b)  # ((i, j), val)

# Reformat for join
A_mapped = A.map(lambda x: (x[0][1], (x[0][0], x[1])))  # key: j
B_mapped = B.map(lambda x: (x[0][0], (x[0][1], x[1])))  # key: j

# Join on common index j
joined = A_mapped.join(B_mapped)

# For each join, emit ((i, k), a_ij * b_jk)
product_terms = joined.map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1]))

# Sum partial products
result = product_terms.reduceByKey(lambda x, y: x + y)

# Collect and print as matrix
matrix_result = result.collect()
matrix_dict = {(i, j): val for ((i, j), val) in matrix_result}

# Print result matrix
print("Resultant Matrix (C = A x B):")
for i in range(n):
    row = [str(matrix_dict.get((i, j), 0)) for j in range(n)]
    print(' '.join(row))

spark.stop()
