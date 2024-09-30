from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


spark = SparkSession.builder.appName("spark_sample_train").master("spark://spark-master:7077") \
        .getOrCreate()



# Sample training data
data = [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0), (4.0, 5.0), (5.0, 6.0)]
df = spark.createDataFrame(data, ["features", "label"])

# Define a feature vector assembler
assembler = VectorAssembler(inputCols=["features"], outputCol="features_vec")

# Transform the DataFrame with the feature vector assembler
df = assembler.transform(df)

# Create a LinearRegression model
lr = LinearRegression(featuresCol="features_vec", labelCol="label")

# Fit the model to the training data
model = lr.fit(df)

# Print the coefficients and intercept of the model
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))

# Stop the SparkSession
spark.stop()