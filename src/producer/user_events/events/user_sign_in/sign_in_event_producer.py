import sys
import logging
from uuid import uuid4
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from utils.config import PRODUCER_CONFIG, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from sign_in_event import SignInEvent, generate_random_sign_in
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC = "sign_in_topic"
avro_manager = AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, TOPIC, SignInEvent.to_dict)
string_serializer = avro_manager.string_serializer

def main() -> None:
    """
    Main function to continuously produce sign in events to Kafka.
    """
    producer = KafkaProducer(PRODUCER_CONFIG)
    try:
        while True:
            # Generate a random sign in event
            sign_in_event = generate_random_sign_in()

            # Serialize the key and event data
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(
                sign_in_event(),
                SerializationContext(TOPIC, MessageField.VALUE)
            )

            # Log the generated event
            logging.info("Produced sign in event: %s", sign_in_event())

            # Send the message to Kafka
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
