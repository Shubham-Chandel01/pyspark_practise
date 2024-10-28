import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import *

with open('../config/config.yaml', 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

sample_json_file_path = config['sample_json_file_path']

spark = SparkSession.builder \
    .appName("JSON Reader with Schema") \
    .getOrCreate()


schema = StructType([
    StructField("name", StringType(), True),
    StructField("language", StringType(), True),
    StructField("id", StringType(), True),
    StructField("bio", StringType(), True),
    StructField("version", FloatType(), True)
])

df = spark.read.json(sample_json_file_path, schema=schema,multiLine=True)

df.show()

spark.stop()
