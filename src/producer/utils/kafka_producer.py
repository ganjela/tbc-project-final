from confluent_kafka import Producer
import logging

class KafkaProducer:
    def __init__(self, config):
        """
        Initialize the Kafka producer with the provided configuration.

        :param config: Dictionary containing Kafka configuration settings
        """
        self.config = config
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self):
        """Initialize the Kafka producer with the configuration."""
        try:
            self.producer = Producer(self.config)
            logging.info("Kafka Producer initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing Kafka Producer: {e}")
            raise

    def produce_message(self, topic, message_key, message_value):
        """
        Produce a message to Kafka topic.

        :param topic: Kafka topic to send the message to
        :param message_key: Key of the message (optional)
        :param message_value: Value of the message (the actual data)
        """
        try:
            self.producer.produce(topic, key=message_key, value=message_value, callback=self._delivery_report)
            self.producer.poll(1)
            
        except Exception as e:
            logging.error(f"Error producing message: {e}")

    def close(self):
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self._process_delivery_reports()
            logging.info("Kafka Producer connection closed.")
        else:
            logging.warning("Producer is not initialized.")

    @staticmethod
    def _delivery_report(err, msg):
        """Callback function to handle message delivery report."""
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message sent to topic {msg.topic()} with key {msg.key()} at offset {msg.offset()}.")

    def _process_delivery_reports(self):
        """Process remaining delivery reports."""
        self.producer.poll(0)
