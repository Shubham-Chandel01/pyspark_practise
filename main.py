from pyspark.sql import SparkSession
from config.config import load_config
from data.data_loader import load_data, get_schema
from data.data_cleaner import handle_missing_values, remove_duplicates
from data.data_transformer import normalize_release_date, save_data , calculate_revenue_and_profit , categorize_ratings
from data.data_analysis import count_films_per_distributor, average_revenue_per_distributor, director_analysis, revenue_per_year
def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Movie Data Processing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Load configuration
    config = load_config('config/config.yaml')
    movies_gross_path = config['movies_gross_file_path']
    movies_output_path = config['movies_output_path']

    # Load data
    schema = get_schema()
    movies_df = load_data(spark, movies_gross_path, schema)

    # Data cleaning and transformation
    movies_df = handle_missing_values(movies_df)
    movies_df = remove_duplicates(movies_df)
    movies_df = normalize_release_date(movies_df)
    movies_df = calculate_revenue_and_profit(movies_df)
    movies_df = categorize_ratings(movies_df)

    movies_df.show()

    #analysis functions
    count_films_per_distributor(movies_df)
    average_revenue_per_distributor(movies_df)
    director_analysis(movies_df)
    revenue_per_year(movies_df)

    # Save cleaned data
    save_data(movies_df, movies_output_path)

    spark.stop()


if __name__ == "__main__":
    main()