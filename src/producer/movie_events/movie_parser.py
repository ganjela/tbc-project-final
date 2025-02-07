from pyspark.sql.functions import split, col, element_at
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import logging
import sys

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_movie_blocks(partition):
    current_movie = {}
    for line in partition:
        line = line.strip()
        if line.startswith("ITEM "):
            if "movie_id" in current_movie:  
                yield (current_movie["movie_id"], 
                       current_movie.get("Title"), 
                       current_movie.get("ListPrice"))
            try:
                current_movie = {"movie_id": int(line.split()[1])}
            except (IndexError, ValueError):
                logging.error(f"Malformed ITEM line: {line}")
                continue
        elif "=" in line:
            key, value = line.split("=", 1)
            key = key.strip()
            if key in ["Title", "ListPrice"]:
                current_movie[key] = value.strip()
    
    if "movie_id" in current_movie:
        yield (current_movie["movie_id"], 
               current_movie.get("Title"), 
               current_movie.get("ListPrice"))


def process_movies(spark, file_path):
    schema = StructType([
        StructField("movie_id", IntegerType()),
        StructField("Title", StringType()),
        StructField("ListPrice", StringType()),
    ])

    raw_rdd = spark.sparkContext.textFile(file_path)
    parsed_rdd = raw_rdd.mapPartitions(parse_movie_blocks)
    parsed_df = spark.createDataFrame(parsed_rdd, schema)

    processed_df = parsed_df.withColumn(
        "price_parts", split(col("ListPrice"), "\$")
    ).withColumn(
        "parsed_price", 
        element_at(col("price_parts"), -1).cast(FloatType())
    ).drop("price_parts", "ListPrice")

    return processed_df.filter(
        (col("parsed_price").isNotNull()) & 
        (col("Title").isNotNull())
    )
