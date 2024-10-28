from pyspark.sql import DataFrame , functions as func
from pyspark.sql.functions import col


def count_films_per_distributor(df: DataFrame) -> None :
   df.groupby("Distributor").agg(func.count(col("Distributor")).alias("count")) \
   .orderBy("count",ascending = False) \
   .show()

def average_revenue_per_distributor(df : DataFrame) -> None:
    df.groupby("Distributor").agg(func.avg(col("US_Gross") + col("Worldwide_Gross"))
      .alias("Avg_Total_Gross")).orderBy("Avg_Total_Gross",ascending = False).show()

def director_analysis(df : DataFrame) -> None:
    df.groupby("Director").agg(func.avg(col("US_Gross") + col("Worldwide_Gross"))
       .alias("Avg_Total_Gross") , func.avg(col("IMDB_Rating")).alias("Avg_IMDB_Rating"),
       func.avg(col("Rotten_Tomatoes_Rating").alias("Avg_Rotten_Tomatoes_Rating"))) \
       .orderBy("Avg_Total_Gross", ascending = False)

def revenue_per_year(df : DataFrame) -> None:
    df.withColumn("Year",func.year(col("Normalized_Release_Date"))) \
      .groupby("Year") \
      .agg(func.avg(col("US_Gross") + col("Worldwide_Gross"))).alias("Avg_Total_Gross") \
      .orderBy("Year") \
      .show()





