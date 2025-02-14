import sys
import logging
from uuid import uuid4
from KafkaProducer import KafkaProducer
from AvroSerializationManager import AvroSerializationManager
from SignInEvent import SignInEvent
from Config import producer_conf, schema_registry_url, auth_user_info
from confluent_kafka.serialization import SerializationContext, MessageField

sys.dont_write_bytecode = True
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

topic = "SignInEvent"
avro_manager = AvroSerializationManager(schema_registry_url, auth_user_info, topic, SignInEvent.to_dict)
string_serializer = avro_manager.string_serializer

if __name__ == "__main__":
    producer = KafkaProducer(producer_conf)
    
    try:
        while True:
            sign_in_data = SignInEvent.generate_random_sign_in()
            serialized_key = string_serializer(str(uuid4()))
            serialized_value = avro_manager.avro_serializer(sign_in_data(), SerializationContext(topic, MessageField.VALUE))

            print(sign_in_data())

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