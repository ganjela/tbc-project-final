import sys
import logging
from uuid import uuid4
import os
from dotenv import load_dotenv

from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from checkout_event import CheckoutEvent, generate_random_checkout
from confluent_kafka.serialization import SerializationContext, MessageField
from utils.config import PRODUCER_CONFIG, SCHEMA_REGISTRY_URL, AUTH_USER_INFO

load_dotenv()

sys.dont_write_bytecode = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC = "checkout_to_cart_topic"
USER_DB_PATH: str = os.getenv("USER_DB_PATH", "")

avro_manager = AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, TOPIC, CheckoutEvent.to_dict)
string_serializer = avro_manager.string_serializer

def main() -> None:
    """
    Main function to continuously produce checkout events to a Kafka topic.
    """
    producer = KafkaProducer(PRODUCER_CONFIG)
    try:
        while True:
            checkout_event = generate_random_checkout(USER_DB_PATH)
            
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(
                checkout_event(),
                SerializationContext(TOPIC, MessageField.VALUE)
            )

            logging.info("Produced checkout event: %s", checkout_event())

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
