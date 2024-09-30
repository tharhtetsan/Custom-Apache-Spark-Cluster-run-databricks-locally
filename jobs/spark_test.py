from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder.appName("spark_test").master("spark://spark-master:7077") \
        .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 20), ("Bob", 25), ("Charlie", 30)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
print(df.show())
spark.stop()