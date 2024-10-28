from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col ,when , to_date , coalesce



def to_date_(col_name: str, formats=("dd-MMM-yy", "yyyy-MM-dd", "MMMM, yyyy")) -> Column:
    date_columns = [to_date(col(col_name), fmt) for fmt in formats]

    return coalesce(*date_columns)

def normalize_release_date(df: DataFrame) -> DataFrame:
    return df.withColumn("Normalized_Release_Date", to_date_("Release_Date"))


def calculate_revenue_and_profit(df):
    """Function to calculate Total Revenue and Profit."""
    df = df.withColumn("Total_Revenue", col("US_Gross") + col("Worldwide_Gross")) \
        .withColumn("Profit", col("Total_Revenue") - col("Production_Budget"))
    return df

def categorize_ratings(df):
    """Function to categorize movie ratings."""
    df = df.withColumn("Rating_Category",
                       when(col("IMDB_Rating") >= 7, "High Rated")
                       .when(col("IMDB_Rating") >= 5, "Medium Rated")
                       .otherwise("Low Rated"))
    return df

def save_data(df: DataFrame, output_path: str):
    df.write.mode('overwrite').parquet(output_path)