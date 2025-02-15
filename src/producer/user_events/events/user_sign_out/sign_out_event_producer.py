import sys
import logging
from uuid import uuid4
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from utils.producer_config import PRODUCER_CONF, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from sign_out_event import SignOutEvent, generate_random_sign_out
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TOPIC = "sign_out_topic"  
avro_manager = AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, TOPIC, SignOutEvent.to_dict)
string_serializer = avro_manager.string_serializer

def main() -> None:
    """
    Main function to continuously produce sign-out events to a Kafka topic.
    """
    producer = KafkaProducer(PRODUCER_CONF)
    try:
        while True:
            sign_out_event = generate_random_sign_out()
            
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(
                sign_out_event(),
                SerializationContext(TOPIC, MessageField.VALUE)
            )
            
            logging.info("Produced sign-out event: %s", sign_out_event())
            
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
