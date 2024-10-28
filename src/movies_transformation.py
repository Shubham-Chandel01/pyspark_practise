import yaml
import pyspark.sql.functions as Func
from pyspark.sql import SparkSession
from pyspark.sql.types import *


with open('../config/config.yaml', 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

movies_gross_path = config['movies_gross_file_path']

movies_output_path = config['movies_output_path']

# defining schema for movies data frame
def get_schema():
    return StructType([
        StructField("Title",StringType(),True),
        StructField("US_Gross",IntegerType(),True),
        StructField("Worldwide_Gross",IntegerType(),True),
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
    return spark.read.format("csv").option("header",True).schema(schema).load(file_path)

# handeling missing values by filling na as 0 and dropping columns that has missing values more than 70%
def handle_missing_values(df):
    df_clean = df.drop("US_DVD_Sales", "Running_Time_min")

    df_filled = df_clean.fillna({
    "Rotten_Tomatoes_Rating": 0,
    "IMDB_Votes": 0,
    "IMDB_Rating": 0.0
    })
    return df_filled

# remove duplicates
def remove_duplicates(df):
    return df.dropDuplicates(["Title"])

#normalize the release date
def normalize_release_date(df):
    df = df.withColumn("Normalized_Release_Date",
         Func.coalesce(
             Func.date_format(Func.to_date("Release_Date","dd-MMM-yy"),"dd-MM-yyyy"),
                   Func.date_format(Func.to_date("Release_Date","yyyy-MM-dd"),"dd-MM-yyyy"),
                   Func.date_format(Func.to_date("Release_Date","MMMM, yyyy"),"dd-MM-yyyy")
                       ))
    return df

def save_data(df, output_path):
    df.write.mode('overwrite').csv(output_path, header=True)

spark = SparkSession.builder \
    .appName("movies_gross_transformation") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

movies_schema = get_schema()

movies_df = load_data(spark, movies_gross_path, movies_schema)

movies_df_filtered = handle_missing_values(movies_df)

movies_df_no_duplicates = remove_duplicates(movies_df_filtered)

moves_df_date_normalized = normalize_release_date(movies_df_no_duplicates)

save_data(moves_df_date_normalized,movies_output_path)

moves_df_date_normalized.show()


spark.stop()
















