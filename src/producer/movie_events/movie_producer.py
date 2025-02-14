import logging
import os
import sys
from typing import Iterator, Tuple

from pyspark.sql import SparkSession, Row, DataFrame
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from utils.producer_config import PRODUCER_CONF, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from confluent_kafka.serialization import SerializationContext, MessageField
from producer.movie_events.movie_event_helpers import movie_row_to_dict, movie_to_dict
from producer.movie_events.movie_processor import process_movies
from dotenv import load_dotenv

load_dotenv()

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC: str = "movies_catalog_enriched"
MOVIES_FILE_PATH: str = os.getenv("MOVIES_FILE_PATH")
PROCESSED_INPUT_PATH : str = os.getenv("PROCESSED_INPUT_PATH")


def serialize_movie(row: Row,
                    avro_manager: AvroSerializationManager,
                    topic: str) -> Tuple[str, bytes]:
    """
    Transform a Spark Row into a dictionary and serialize it using Avro.
    
    :param row: A single Spark Row containing movie data.
    :param avro_manager: An instance of AvroSerializationManager for serialization.
    :param topic: The Kafka topic name.
    :return: A tuple containing the movie id as a string and the serialized message as bytes.
    """
    movie_data = movie_row_to_dict(row)
    print("Preparing to send movie data:", movie_data)
    
    serialized_value: bytes = avro_manager.avro_serializer(
        movie_data,
        SerializationContext(topic, MessageField.VALUE)
    )
    return str(movie_data["movie_id"]), serialized_value


def send_partition(partition: Iterator[Row]) -> None:
    """
    Process each partition of the RDD by initializing external Kafka objects
    on the worker node, serializing the movie data, and sending it to Kafka.
    
    :param partition: An iterator over Spark Rows in a partition.
    """
    avro_manager: AvroSerializationManager = AvroSerializationManager(
        SCHEMA_REGISTRY_URL,
        AUTH_USER_INFO,
        TOPIC,
        movie_to_dict
    )
    producer: KafkaProducer = KafkaProducer(PRODUCER_CONF)
    
    serialized_messages = map(
        lambda row: serialize_movie(row, avro_manager, TOPIC),
        partition
    )
    
    for message_key, serialized_value in serialized_messages:
        try:
            producer.produce_message(
                topic=TOPIC,
                message_key=message_key,
                message_value=serialized_value
            )
        except Exception as e:
            logging.error(f"Error serializing/sending: {str(e)}")
    
    producer.close()

def main():
    
    if not PROCESSED_INPUT_PATH:
        raise ValueError("Missing PROCESSED_INPUT_PATH environment variable")
    
    logging.info(f"Producing events from: {PROCESSED_INPUT_PATH}")
    
    spark = SparkSession.builder.appName("MovieEventProduction").getOrCreate()
    
    try:
        valid_movies = spark.read.parquet(PROCESSED_INPUT_PATH)
        valid_movies.rdd.foreachPartition(send_partition)
        logging.info("Successfully produced all events to Kafka")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


# if __name__ == "__main__":
#     spark: SparkSession = SparkSession.builder \
#         .appName("MovieEventsProducer") \
#         .getOrCreate()
    
#     try:
#         valid_movies: DataFrame = process_movies(spark, MOVIES_FILE_PATH)
#         valid_movies.rdd.foreachPartition(send_partition)
#     except Exception as e:
#         logging.error(f"Spark processing error: {str(e)}")
#     finally:
#         spark.stop()
#         logging.info("Spark session closed")
