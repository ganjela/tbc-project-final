import sys
import random
import logging
from uuid import uuid4
from KafkaProducer import KafkaProducer
from AvroSerializationManager import AvroSerializationManager
from ItemViewEvent import ItemViewEvent, load_items_from_file, generate_random_item_view
from Config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic = "ItemViewEvent"
avro_manager = AvroSerializationManager(schema_registry_url, auth_user_info, topic, ItemViewEvent.to_dict)
string_serializer = avro_manager.string_serializer

if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)
    users = [random.randint(1, 100000) for _ in range(1000)]  # Simulating existing users
    items = load_items_from_file("items.txt")  # Load items from a file
    
    try:
        while True:
            item_view_data = generate_random_item_view(users, items)
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(item_view_data(), SerializationContext(topic, MessageField.VALUE))

            print(item_view_data())

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