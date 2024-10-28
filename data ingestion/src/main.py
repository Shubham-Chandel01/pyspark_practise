from pyspark.sql import SparkSession








def main():

    spark = SparkSession.builder \
            .appName("Data ingestion project") \
            .getOrCreate()





if __name__ == "__main__":
    main()