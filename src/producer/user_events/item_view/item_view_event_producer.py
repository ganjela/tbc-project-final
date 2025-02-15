import os
import sys
import logging
from uuid import uuid4
from dotenv import load_dotenv
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from utils.producer_config import PRODUCER_CONF, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from item_view_event import ItemViewEvent, generate_random_item_view
from confluent_kafka.serialization import SerializationContext, MessageField

load_dotenv()

sys.dont_write_bytecode = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC = "item_view_topic"
USER_DB_PATH = os.getenv("USER_DB_PATH")
ITEM_IDS_PATH = os.getenv("ITEM_IDS_PATH")

if not USER_DB_PATH or not ITEM_IDS_PATH:
    logging.error("Environment variables USER_DB_PATH and ITEM_IDS_PATH must be set.")
    sys.exit(1)

avro_manager = AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, TOPIC, ItemViewEvent.to_dict)
string_serializer = avro_manager.string_serializer

def main() -> None:
    """
    Main function to continuously produce item view events to a Kafka topic.
    """
    producer = KafkaProducer(PRODUCER_CONF)
    
    try:
        while True:
            item_view_event = generate_random_item_view(USER_DB_PATH, ITEM_IDS_PATH)
            
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(
                item_view_event(), 
                SerializationContext(TOPIC, MessageField.VALUE)
            )
            
            logging.info("Produced item view event: %s", item_view_event())
            
            producer.produce_message(
                topic=TOPIC,
                message_key=serialized_key,
                message_value=serialized_value
            )
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error("An error occurred: %s", e, exc_info=True)
    finally:
        producer.close()

if __name__ == "__main__":
    main()
