from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col, window, lit, current_timestamp

from Config import schema_registry_url, auth_user_info, spark_consumer_conf, snowflake_conf


spark = SparkSession.builder \
    .appName("user_registration_windows") \
    .master("spark://ip-172-31-21-47.eu-central-1.compute.internal:7077") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.shuffle.partitions", 6) \
    .getOrCreate()


subject = "user_registration-value"
schema_registry_conf = {
    'url': schema_registry_url,
    'basic.auth.user.info': auth_user_info
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)


latest_version = schema_registry_client.get_latest_version(subject)
schema = latest_version.schema.schema_str


topic = 'user_registration'


raw_stream = spark.readStream \
    .format("kafka") \
    .options(**spark_consumer_conf) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", expr("substring(value, 6, length(value)-5)")) \
    .select("value")


user_registration = raw_stream.select(from_avro(col("value"), schema).alias("user_registration")) \
    .select(expr("CAST(user_registration.timestamp AS timestamp)").alias("timestamp"),
            col("user_registration.user_id"))


user_registration = user_registration.withWatermark("timestamp", '10 minute')

tumbling_window_agg = user_registration \
    .groupBy(window("timestamp", "1 hour")) \
    .count() \
    .select(col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("tumbling").alias("window_type"),
            col("count").alias("registration_count"),
            current_timestamp().alias("processing_time"))


sliding_window_agg = user_registration \
    .groupBy(window("timestamp", "1 hour", "15 minute")) \
    .count() \
    .select(col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("sliding").alias("window_type"),
            col("count").alias("registration_count"),
            current_timestamp().alias("processing_time"))


combined_results = tumbling_window_agg.union(sliding_window_agg)


def write_to_snowflake(batch_df: DataFrame, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", 'user_registration_metrics') \
        .mode("append") \
        .save()


query = combined_results.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpointUserRegistrationWindows") \
    .start()



query.awaitTermination()