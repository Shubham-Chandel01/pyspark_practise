from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def get_schema():
    return StructType([
        StructField("Title", StringType(), True),
        StructField("US_Gross", IntegerType(), True),
        StructField("Worldwide_Gross", IntegerType(), True),
        StructField("US_DVD_Sales", IntegerType(), True),
        StructField("Production_Budget", IntegerType(), True),
        StructField("Release_Date", StringType(), True),
        StructField("MPAA_Rating", StringType(), True),
        StructField("Running_Time_min", IntegerType(), True),
        StructField("Distributor", StringType(), True),
        StructField("Source", StringType(), True),
        StructField("Major_Genre", StringType(), True),
        StructField("Creative_Type", StringType(), True),
        StructField("Director", StringType(), True),
        StructField("Rotten_Tomatoes_Rating", IntegerType(), True),
        StructField("IMDB_Rating", FloatType(), True),
        StructField("IMDB_Votes", IntegerType(), True)
    ])

def load_data(spark, file_path, schema):
    return spark.read.format("csv").option("header", True).schema(schema).load(file_path)