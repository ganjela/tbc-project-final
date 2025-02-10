import sys
import random
import logging
from uuid import uuid4
from KafkaProducer import KafkaProducer
from AvroSerializationManager import AvroSerializationManager
from CheckoutEvent import CheckoutEvent, generate_random_checkout
from Config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic = "CheckoutEvent"
avro_manager = AvroSerializationManager(schema_registry_url, auth_user_info, topic, CheckoutEvent.to_dict)
string_serializer = avro_manager.string_serializer

if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)
    users = [random.randint(1, 100000) for _ in range(1000)]  # Simulating existing users
    cart_ids = [str(uuid4()) for _ in range(500)]  # Simulating cart IDs
    
    try:
        while True:
            checkout_data = generate_random_checkout(users, cart_ids)
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(checkout_data(), SerializationContext(topic, MessageField.VALUE))

            print(checkout_data())

            producer.produce_message(
                topic=topic,
                message_key=serialized_key,
                message_value=serialized_value
            )
                            
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
        producer.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        producer.close()