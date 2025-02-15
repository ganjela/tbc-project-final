import os
import sys
import logging
from uuid import uuid4
from dotenv import load_dotenv
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from add_to_cart_event import AddToCartEvent, generate_random_add_to_cart_event
from utils.producer_config import PRODUCER_CONF, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from confluent_kafka.serialization import SerializationContext, MessageField

load_dotenv()

sys.dont_write_bytecode = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

USER_DB_PATH = os.getenv("USER_DB_PATH")
ITEM_IDS_PATH = os.getenv("ITEM_IDS_PATH")
if not USER_DB_PATH or not ITEM_IDS_PATH:
    logging.error("Required environment variables (USER_DB_PATH, ITEM_IDS_PATH) are not set.")
    sys.exit(1)

TOPIC = "add_to_cart_topic"

avro_manager = AvroSerializationManager(
    SCHEMA_REGISTRY_URL,
    AUTH_USER_INFO,
    TOPIC,
    AddToCartEvent.to_dict
)
string_serializer = avro_manager.string_serializer

def main():
    """
    Main function to run the Kafka producer for add-to-cart events.
    """
    producer = KafkaProducer(PRODUCER_CONF)
    try:
        while True:
            event_callable = generate_random_add_to_cart_event(USER_DB_PATH, ITEM_IDS_PATH)
            event_data = event_callable()

            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(
                event_data,
                SerializationContext(TOPIC, MessageField.VALUE)
            )

            logging.info(f"Producing event: {event_data}")

            producer.produce_message(
                topic=TOPIC,
                message_key=serialized_key,
                message_value=serialized_value
            )
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as exc:
        logging.exception("An unexpected error occurred:")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
