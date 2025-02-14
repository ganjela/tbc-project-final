import sys
import random
import logging
from uuid import uuid4
from KafkaProducer import KafkaProducer
from AvroSerializationManager import AvroSerializationManager
from AddToCartEvent import AddToCartEvent, load_items_from_file, generate_random_add_to_cart
from CheckoutEvent import CheckoutEvent, generate_random_checkout
from ItemViewEvent import ItemViewEvent, generate_random_item_view
from UserRegistration import UserRegistration
from SignInEvent import SignInEvent
from SignOutEvent import SignOutEvent
from Config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField
from generationEventProducer.userRegistration.userregistrationEvent import initialize_db

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_event(producer, avro_manager, topic, generate_event):
    try:
        while True:
            event_data = generate_event()
            serialized_key = avro_manager.string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(event_data(), SerializationContext(topic, MessageField.VALUE))

            print(event_data())
            producer.produce_message(
                topic=topic,
                message_key=serialized_key,
                message_value=serialized_value
            )
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)
    initialize_db()

    event_configurations = [
        ("AddToCartEvent", AddToCartEvent.to_dict, generate_random_add_to_cart, load_items_from_file("items.txt")),
        ("CheckoutEvent", CheckoutEvent.to_dict, generate_random_checkout, [str(uuid4()) for _ in range(500)]),
        ("ItemViewEvent", ItemViewEvent.to_dict, generate_random_item_view, load_items_from_file("items.txt")),
        ("UserRegistration", UserRegistration.to_dict, UserRegistration.generate_random_registration, None),
        ("SignInEvent", SignInEvent.to_dict, SignInEvent.generate_random_sign_in, None),
        ("SignOutEvent", SignOutEvent.to_dict, SignOutEvent.generate_random_sign_out, None),
    ]

    users = [random.randint(1, 100000) for _ in range(1000)]  # Simulating existing users

    for topic, to_dict, generator, additional_data in event_configurations:
        avro_manager = AvroSerializationManager(schema_registry_url, auth_user_info, topic, to_dict)
        if additional_data:
            send_event(producer, avro_manager, topic, lambda: generator(users, additional_data))
        else:
            send_event(producer, avro_manager, topic, generator)
