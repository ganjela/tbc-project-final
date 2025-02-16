import os
import sys
import logging
from uuid import uuid4
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from user_registration_event import UserRegistration, generate_random_registration
from utils.config import PRODUCER_CONFIG, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()

sys.dont_write_bytecode = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

USER_DB_PATH: str = os.getenv("USER_DB_PATH", "")
TOPIC = "user_registration_topic"
if not USER_DB_PATH:
    logging.error("Required environment variables (USER_DB_PATH) are not set.")
    sys.exit(1)

avro_manager = AvroSerializationManager(
    SCHEMA_REGISTRY_URL,
    AUTH_USER_INFO,
    TOPIC,
    UserRegistration.to_dict
)
string_serializer = avro_manager.string_serializer

def main():
    """
    Main function to produce user registration events to Kafka.
    """
    producer = KafkaProducer(PRODUCER_CONFIG)
    
    try:
        while True:
            user_registration_data = generate_random_registration(USER_DB_PATH)()

            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(
                user_registration_data,
                SerializationContext(TOPIC, MessageField.VALUE)
            )

            logging.info(f"Producing event: {user_registration_data}")

            producer.produce_message(
                topic=TOPIC,
                message_key=serialized_key,
                message_value=serialized_value
            )
                            
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        producer.close()

if __name__ == "__main__":
    main()
