import sys
import logging
from uuid import uuid4
from utils.kafka_producer import KafkaProducer
from utils.avro_manager import AvroSerializationManager
from utils.config import PRODUCER_CONFIG, SCHEMA_REGISTRY_URL, AUTH_USER_INFO
from events import SignInEvent, generate_random_sign_in
from events import SignOutEvent, generate_random_sign_out
from events import CheckoutEvent, generate_random_checkout
from events import ItemViewEvent, generate_random_item_view
from events import UserRegistration, generate_random_registration
from events import AddToCartEvent, generate_random_add_to_cart_event
from confluent_kafka.serialization import SerializationContext, MessageField
import os
from dotenv import load_dotenv

load_dotenv()

sys.dont_write_bytecode = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

producer = KafkaProducer(PRODUCER_CONFIG)

EVENT_TOPICS = {
    "sign_in": "sign_in_topic",
    "sign_out": "sign_out_topic",
    "checkout_to_cart": "checkout_to_cart_topic",
    "item_view": "item_view_topic",
    "user_registration": "user_registration_topic",
    "add_to_cart": "add_to_cart_topic",
}

avro_managers = {
    "sign_in": AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, EVENT_TOPICS["sign_in"], SignInEvent.to_dict),
    "sign_out": AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, EVENT_TOPICS["sign_out"], SignOutEvent.to_dict),
    "checkout_to_cart": AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, EVENT_TOPICS["checkout_to_cart"], CheckoutEvent.to_dict),
    "item_view": AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, EVENT_TOPICS["item_view"], ItemViewEvent.to_dict),
    "user_registration": AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, EVENT_TOPICS["user_registration"], UserRegistration.to_dict),
    "add_to_cart": AvroSerializationManager(SCHEMA_REGISTRY_URL, AUTH_USER_INFO, EVENT_TOPICS["add_to_cart"], AddToCartEvent.to_dict),
}

USER_DB_PATH = os.getenv("USER_DB_PATH")
ITEM_IDS_PATH = os.getenv("ITEM_IDS_PATH")

def generate_event(event_type: str):
    if event_type == "sign_in":
        return generate_random_sign_in()
    elif event_type == "sign_out":
        return generate_random_sign_out()
    elif event_type == "checkout_to_cart":
        return generate_random_checkout(USER_DB_PATH)
    elif event_type == "item_view":
        return generate_random_item_view(USER_DB_PATH, ITEM_IDS_PATH)
    elif event_type == "user_registration":
        return generate_random_registration(USER_DB_PATH)
    elif event_type == "add_to_cart":
        return generate_random_add_to_cart_event(USER_DB_PATH, ITEM_IDS_PATH)
    else:
        raise ValueError(f"Invalid event type: {event_type}")

if __name__ == "__main__":
    try:
        while True:
            for event_type in EVENT_TOPICS.keys():
                event_data = generate_event(event_type)
                serialized_key = avro_managers[event_type].string_serializer(str(uuid4()))
                serialized_value = avro_managers[event_type].avro_serializer(
                    event_data(), SerializationContext(EVENT_TOPICS[event_type], MessageField.VALUE)
                )

                logging.info(f"Sending {event_type} event: {event_data()}")

                producer.produce_message(
                    topic=EVENT_TOPICS[event_type],
                    message_key=serialized_key,
                    message_value=serialized_value
                )
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.close()
