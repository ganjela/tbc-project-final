import logging
from datetime import datetime
from typing import Dict, Union
from producer.utils.KafkaProducer import KafkaProducer
from producer.utils.AvroSerializationManager import AvroSerializationManager
from producer.utils.Config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, element_at
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import sys
import time
import os
from dotenv import load_dotenv 

load_dotenv()

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MovieCatalogEnriched:
    def __init__(self, movie_id: int, title: str, parsed_price: float):
        self.movie_id = movie_id
        self.title = title
        self.parsed_price = parsed_price
        self.event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def __call__(self) -> Dict[str, Union[str, int, float]]:
        return {
            "event_name": "movies_catalog_enriched",
            "movie_id": self.movie_id,
            "title": self.title,
            "parsed_price": self.parsed_price,
            "event_time": self.event_time
        }

    @staticmethod
    def to_dict(data, ctx) -> Dict[str, Union[int, str, float]]:
        return {
            "event_name": data.get("event_name"),
            "movie_id": data.get("movie_id"),
            "title": data.get("title"),
            "parsed_price": data.get("parsed_price"),
            "event_time": data.get("event_time")
        }

def parse_movie_blocks(partition):
    current_movie = {}
    for line in partition:
        line = line.strip()
        if line.startswith("ITEM "):
            if "movie_id" in current_movie:  # Ensure 'movie_id' exists before yielding
                yield (current_movie["movie_id"], 
                       current_movie.get("Title"), 
                       current_movie.get("ListPrice"))
            try:
                current_movie = {"movie_id": int(line.split()[1])}  # Extract movie_id safely
            except (IndexError, ValueError):
                logging.error(f"Malformed ITEM line: {line}")
                continue
        elif "=" in line:
            key, value = line.split("=", 1)
            key = key.strip()
            if key in ["Title", "ListPrice"]:
                current_movie[key] = value.strip()
    
    if "movie_id" in current_movie:  # Final check before yielding the last record
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

if __name__ == "__main__":
    topic = "movies_catalog_enriched"
    movies_file = os.getenv("MOVIES_FILE_PATH")

    spark = SparkSession.builder \
        .appName("MovieEventsProducer") \
        .getOrCreate()

    try:
         # Start measuring time
        start_time = time.time()

        # This is the line you want to measure
        valid_movies = process_movies(spark, movies_file)

        # Calculate the elapsed time
        elapsed_time = time.time() - start_time
        logging.info(f"Time taken to process movies: {elapsed_time:.4f} seconds")
        
        def send_partition(partition):
            avro_manager = AvroSerializationManager(
                schema_registry_url,
                auth_user_info,
                topic,
                MovieCatalogEnriched.to_dict
            )
            producer = KafkaProducer(producer_conf)
            
            for row in partition:
                movie = MovieCatalogEnriched(
                    movie_id=row.movie_id,
                    title=row.Title,
                    parsed_price=row.parsed_price,
                )
                
                # Get the dictionary representation of the movie data.
                movie_data = movie()
                
                # Log the data before sending
                print("Preparing to send movie data: ", movie_data)
                # Alternatively, you can use print:
                # print("Preparing to send movie data:", movie_data)
                
                try:
                    serialized_value = avro_manager.avro_serializer(
                        movie_data, 
                        SerializationContext(topic, MessageField.VALUE)
                    )
                    producer.produce_message(
                        topic=topic,
                        message_key=str(movie.movie_id),
                        message_value=serialized_value
                    )
                except Exception as e:
                    logging.error(f"Error serializing/sending: {str(e)}")
            
            producer.close()


        valid_movies.rdd.foreachPartition(send_partition)

    except Exception as e:
        logging.error(f"Spark processing error: {str(e)}")
    finally:
        spark.stop()
        logging.info("Spark session closed")