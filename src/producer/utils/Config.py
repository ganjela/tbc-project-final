import os
from dotenv import load_dotenv
load_dotenv()

producer_conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'client.id': os.getenv('KAFKA_CLIENT_ID'),
}

schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL')
auth_user_info = os.getenv('BASIC_AUTH_USER_INFO')