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


spark_consumer_conf = {
    "kafka.bootstrap.servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    "kafka.security.protocol": os.getenv('KAFKA_SECURITY_PROTOCOL'),
    "kafka.sasl.mechanism": os.getenv('KAFKA_SASL_MECHANISM'),
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule "
                              f"required username=\"{os.getenv('KAFKA_SASL_USERNAME')}\" "
                              f"password=\"{os.getenv('KAFKA_SASL_PASSWORD')}\";"
}

snowflake_conf = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}

schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL')
auth_user_info = os.getenv('BASIC_AUTH_USER_INFO')

