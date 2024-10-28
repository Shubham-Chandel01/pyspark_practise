from pyspark.sql import DataFrame

def handle_missing_values(df: DataFrame) -> DataFrame:
    df_clean = df.drop("US_DVD_Sales", "Running_Time_min")

    df_filled = df_clean.fillna({
        "Rotten_Tomatoes_Rating": 39.3,
        "IMDB_Votes": 27861,
        "IMDB_Rating": 5.8
    })
    return df_filled

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["Title"])