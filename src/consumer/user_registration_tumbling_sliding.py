import logging
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col, window, lit, current_timestamp
from utils.config import SCHEMA_REGISTRY_URL, AUTH_USER_INFO, CONSUMER_CONFIG, SNOWFLAKE_OPTIONS

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("user_registration_windows") \
    .getOrCreate()

subject = "user_registration_topic-value"
schema_registry_conf = {
    'url': SCHEMA_REGISTRY_URL,
    'basic.auth.user.info': AUTH_USER_INFO
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

latest_version = schema_registry_client.get_latest_version(subject)
schema = latest_version.schema.schema_str

topic = 'user_registration_topic'

raw_stream = spark.readStream \
    .format("kafka") \
    .options(**CONSUMER_CONFIG) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")

user_registration = raw_stream.select(
    from_avro(col("value"), schema).alias("user_registration")
).select(
    expr("CAST(user_registration.timestamp AS timestamp)").alias("timestamp"),
    col("user_registration.user_id")
)

user_registration = user_registration.withWatermark("timestamp", '10 minute')

tumbling_window_agg = user_registration \
    .groupBy(window("timestamp", "1 hour")) \
    .count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("tumbling").alias("window_type"),
        col("count").alias("registration_count"),
        current_timestamp().alias("processing_time")
    )

sliding_window_agg = user_registration \
    .groupBy(window("timestamp", "1 hour", "15 minute")) \
    .count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("sliding").alias("window_type"),
        col("count").alias("registration_count"),
        current_timestamp().alias("processing_time")
    )

combined_results = tumbling_window_agg.union(sliding_window_agg)

def write_to_snowflake(batch_df: DataFrame, batch_id):
    try:
        batch_df.write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("sfSchema", 'user_metrics') \
            .option("dbtable", 'user_registration_metrics') \
            .mode("append") \
            .save()
        logger.info("Batch %d written successfully to user_registration_metrics", batch_id)
    except Exception as e:
        logger.error("Error writing batch %d: %s", batch_id, str(e))
        raise

query = combined_results.writeStream \
    .trigger(processingTime='1 second') \
    .outputMode("append") \
    .option("truncate", "True") \
    .foreachBatch(write_to_snowflake) \
    .start()

query.awaitTermination()
