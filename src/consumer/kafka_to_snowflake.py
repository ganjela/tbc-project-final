from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro
from utils.config import SNOWFLAKE_OPTIONS, CONSUMER_CONFIG, SCHEMA_REGISTRY_CONFIG
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOPIC_TABLE_MAPPING = {
    "user_registration_topic": ("user_events", "user_registration"),
    "sign_in_topic": ("user_events", "sign_in_events"),
    "sign_out_topic": ("user_events", "sign_out_events"),
    "item_view_topic": ("item_events", "item_view_events"),
    "add_to_cart_topic": ("cart_events", "added_to_cart_events"),
    "checkout_to_cart_topic": ("cart_events", "checkout_events"),
    "movies_catalog_enriched": ("CATALOG", "movies_catalog_enriched")
}

spark = SparkSession.builder.appName("Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def write_to_snowflake(micro_df, batch_id, table_name, schema):
    """Writes micro-batch DataFrame to Snowflake with schema/table routing."""
    if micro_df.isEmpty():
        logger.warning("Batch %d is empty. Skipping write.", batch_id)
        return

    try:
        (micro_df.write
                 .format("snowflake")
                 .options(**SNOWFLAKE_OPTIONS)
                 .option("sfSchema", schema)
                 .option("dbtable", table_name)
                 .mode("append")
                 .save())
        logger.info(
            "Batch %d written successfully to Table %s.%s",
            batch_id, schema, table_name
        )
    except Exception as e:
        logger.error(
            "Error writing batch %d to %s.%s: %s",
            batch_id, schema, table_name, str(e)
        )
        raise


def get_schema_from_schema_registry(schema_registry_config, schema_registry_subject):
    """Retrieves latest Avro schema from Schema Registry."""
    try:
        schema_registry_client = SchemaRegistryClient(schema_registry_config)
        return schema_registry_client.get_latest_version(schema_registry_subject)
    except Exception as e:
        logger.error(
            "Error fetching schema for subject %s: %s",
            schema_registry_subject, str(e)
        )
        raise


def spark_consumer(topic, schema, table_name):
    """Structured Streaming consumer with Avro decoding and Snowflake writer."""
    logger.info("Initializing consumer for topic: %s", topic)
    
    try:
        # Kafka Source Configuration
        df = (
            spark.readStream
            .format("kafka")
            .option("subscribe", topic)
            .options(**CONSUMER_CONFIG)
            .load()
        )

        # Binary Format Parsing
        df = (df
              .withColumn("magicByte", func.expr("substring(value, 1, 1)"))
              .withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))
              .withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))
        )

        value_df = df.select("magicByte", "valueSchemaId", "fixedValue")

        # Schema Retrieval
        logger.debug("Fetching schema for topic: %s", topic)
        schema_version = get_schema_from_schema_registry(
            SCHEMA_REGISTRY_CONFIG,
            f"{topic}-value"
        )

        # Avro Deserialization Configuration
        from_avro_options = {"mode": "PERMISSIVE"}
        decoded_output = value_df.select(
            from_avro(
                func.col("fixedValue"),
                schema_version.schema.schema_str,
                from_avro_options
            ).alias("data")
        )

        value_df = decoded_output.select("data.*")

        # Streaming Query Configuration
        query = (
            value_df.writeStream
            .trigger(processingTime='1 second')
            .outputMode("append")
            .option("truncate", "True")
            .foreachBatch(lambda df, id: write_to_snowflake(df, id, table_name, schema))
            .start()
        )
        
        logger.info("Successfully started streaming query for topic: %s", topic)
        return query

    except Exception as e:
        logger.error("Failed to initialize consumer for topic %s: %s", topic, str(e))
        raise


if __name__ == "__main__":
    try:
        logger.info("Starting streaming application")
        queries = [
            spark_consumer(topic, schema, table_name)
            for topic, (schema, table_name) in TOPIC_TABLE_MAPPING.items()
        ]

        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down gracefully...")
    except Exception as e:
        logger.critical("Application failed with error: %s", str(e))
        raise
    finally:
        logger.info("Application shutdown completed")