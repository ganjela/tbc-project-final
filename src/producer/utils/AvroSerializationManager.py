from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

class AvroSerializationManager:
    def __init__(self, schema_registry_url, auth_user_info, topic, to_dict):
        """
        Initializes the AvroSerializationManager with Schema Registry client and serializers.

        :param schema_registry_url: URL of the Schema Registry
        :param auth_user_info: Authentication information for Schema Registry
        :param topic: Kafka topic to derive the schema subject
        """
        self.schema_registry_conf = {
            'url': schema_registry_url,
            'basic.auth.user.info': auth_user_info
        }
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
        self.topic = topic
        self.subject = f"{self.topic}-value"
        self.schema = self._get_latest_schema()
        self.to_dict = to_dict
        self.avro_serializer = AvroSerializer(self.schema_registry_client, self.schema, self.to_dict)
        self.string_serializer = StringSerializer('utf_8')

    def _get_latest_schema(self):
        """
        Fetches the latest schema from Schema Registry.
        :return: Schema string
        """
        schema_metadata = self.schema_registry_client.get_latest_version(self.subject)
        return schema_metadata.schema.schema_str
    
    