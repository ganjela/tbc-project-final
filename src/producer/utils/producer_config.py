import os
from dotenv import load_dotenv
load_dotenv()

PRODUCER_CONF = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'client.id': os.getenv('KAFKA_CLIENT_ID'),
}

SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
AUTH_USER_INFO = os.getenv('BASIC_AUTH_USER_INFO')