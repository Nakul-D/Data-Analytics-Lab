from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize Spark session
spark = SparkSession.builder.appName("WordCountAnalysis").getOrCreate()

# Path to the text file (make sure to replace this with your actual file path)
file_path = "input.txt"

# Read the text file
text_df = spark.read.text(file_path)

# Split the text into words and count their occurrences
word_df = text_df.select(explode(split(text_df['value'], '\s+')).alias('word'))  # Split by whitespace
word_df = word_df.groupBy('word').count().orderBy('count', ascending=False)

# Show all rows (with truncate=False to avoid truncation of words)
word_df.show(word_df.count(), truncate=False)

# Stop the Spark session
spark.stop()
