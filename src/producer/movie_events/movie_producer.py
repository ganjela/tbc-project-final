import logging
import time
from pyspark.sql import SparkSession
from producer.utils.kafka_producer import KafkaProducer
from producer.utils.avro_manager import AvroSerializationManager
from producer.utils.producer_config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField
from producer.movie_events.movie_events import MovieCatalogEnriched
from producer.movie_events.movie_parser import process_movies
import os
import sys 
from dotenv import load_dotenv

load_dotenv()

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        
        movie_data = movie()
        
        print("Preparing to send movie data: ", movie_data)
        
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


if __name__ == "__main__":
    topic = "movies_catalog_enriched"
    movies_file = os.getenv("MOVIES_FILE_PATH")

    spark = SparkSession.builder \
        .appName("MovieEventsProducer") \
        .getOrCreate()

    try:
        start_time = time.time()

        valid_movies = process_movies(spark, movies_file)

        elapsed_time = time.time() - start_time
        logging.info(f"Time taken to process movies: {elapsed_time:.4f} seconds")

        valid_movies.rdd.foreachPartition(send_partition)

    except Exception as e:
        logging.error(f"Spark processing error: {str(e)}")
    finally:
        spark.stop()
        logging.info("Spark session closed")