import sys
import logging
from uuid import uuid4
from KafkaProducer import KafkaProducer
from AvroSerializationManager import AvroSerializationManager
from UserRegistration import UserRegistration
from Config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic = "UserRegistration"
avro_manager = AvroSerializationManager(schema_registry_url, auth_user_info, topic, UserRegistration.to_dict)
string_serializer = avro_manager.string_serializer

if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)
    
    try:
        while True:
            user_registration_data = UserRegistration.generate_random_registration()
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(user_registration_data(), SerializationContext(topic, MessageField.VALUE))

            print(user_registration_data())

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