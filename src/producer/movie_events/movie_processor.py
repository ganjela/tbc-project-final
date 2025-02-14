import os
import logging
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import split, col, regexp_extract, regexp_replace, expr
from pyspark.sql import SparkSession

load_dotenv()

MOVIES_FILE_PATH: str = os.getenv("MOVIES_FILE_PATH")
MOVIES_OUTPUT_PATH: str = os.getenv("MOVIES_OUTPUT_PATH")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_key_value(df: DataFrame, key: str) -> DataFrame:
    """
    Extracts a key-value pair from a multi-line text column and adds it as a new column.

    :param df: Input DataFrame containing a column with multi-line text.
    :param key: The key whose corresponding value needs to be extracted.
    :return: DataFrame with the extracted key-value pair as a new column.
    """
    df = df.withColumn(
        key,
        expr(f"filter(lines, x -> split(x, '=')[0] = '{key}')[0]")
    ).withColumn(
        key,
        regexp_replace(col(key), f"^{key}=", "")
    )
    
    return df    

def process_movies(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads and processes a movie dataset stored as multi-line text.

    :param spark: The Spark session instance.
    :param file_path: Path to the file containing movie data.
    :return: Processed DataFrame with extracted and transformed movie details.
    """
    MOVIE_ID_PATTERN: str = r"ITEM (\d+)"
    PRICE_PATTERN: str = r"\$(\d+\.\d+)"
    REQUIRED_FIELDS: list[str] = ["Title", "ListPrice"]

    raw_df: DataFrame = spark.read.text(file_path, lineSep="\n\n")

    split_df: DataFrame = (
        raw_df
        .withColumn("lines", split(col("value"), "\n"))
        .withColumn("movie_id", 
                   regexp_extract(col("lines").getItem(0), MOVIE_ID_PATTERN, 1)
                   .cast("int"))
    )

    for field in REQUIRED_FIELDS:
        split_df = extract_key_value(split_df, field)

    processed_df: DataFrame = (
        split_df
        .withColumn("parsed_price",
                   regexp_extract(col("ListPrice"), PRICE_PATTERN, 1)
                   .cast("float"))
        .drop("ListPrice", "value", "lines")
    )

    return processed_df.dropna()


def main():
    if not MOVIES_FILE_PATH or not MOVIES_OUTPUT_PATH:
        raise ValueError("Missing required environment variables")
    
    logger.info(f"Processing movies from: {MOVIES_FILE_PATH}")
    
    spark = SparkSession.builder.appName("MovieProcessing").getOrCreate()
    
    try:
        processed_df = process_movies(spark, MOVIES_FILE_PATH)
        processed_df.write.parquet(MOVIES_OUTPUT_PATH, mode="overwrite")

        logger.info(f"Successfully wrote processed data to: {MOVIES_OUTPUT_PATH}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
