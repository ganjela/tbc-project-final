import os
from dotenv import load_dotenv

load_dotenv()

PRODUCER_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'client.id': os.getenv('KAFKA_CLIENT_ID'),
}

SNOWFLAKE_OPTIONS = {
    "sfURL": os.getenv("SF_URL"),
    "sfAccount": os.getenv("SF_ACCOUNT"),
    "sfUser": os.getenv("SF_USER"),
    "sfPassword": os.getenv("SF_PASSWORD"),
    "sfDatabase": os.getenv("SF_DATABASE"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE"),
    "sfRole": os.getenv("SF_ROLE")
}

CONSUMER_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
    "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
    "sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
    "sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
    "group.id": os.getenv("KAFKA_CONSUMER_GROUP_ID"),
    "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
}

SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
AUTH_USER_INFO = os.getenv('BASIC_AUTH_USER_INFO')

SCHEMA_REGISTRY_CONFIG = {
    "url": SCHEMA_REGISTRY_URL,
    "basic.auth.user.info": AUTH_USER_INFO
}
