from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("TestSpark") \
    .getOrCreate()


data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
columns = ["Name", "Id"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()