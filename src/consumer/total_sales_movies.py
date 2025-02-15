from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, expr, current_timestamp, udf
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import BinaryType
from Config import schema_registry_url, auth_user_info, spark_consumer_conf, snowflake_conf


spark = (
    SparkSession.builder
    .appName("MovieSalesAggregator")
    .getOrCreate()
)


def create_kafka_stream(topic_name):
    return (
        spark.readStream
        .format("kafka")
        .options(**spark_consumer_conf)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )


checkout_stream = create_kafka_stream("checkout_topic")


def deserialize_avro_data(dataframe, event_type):
    """
    Deserializes Avro data and adds an event type column
    """
    try:
        schema_registry_conf = {
            'url': schema_registry_url,
            'basic.auth.user.info': auth_user_info
        }
        print("Schema Registry Config:", schema_registry_conf)

        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        print("Schema Registry Client Initialized Successfully!")

        latest_version = schema_registry_client.get_latest_version(f"{event_type}-value")
        schema = latest_version.schema.schema_str
        print("Schema Retrieved Successfully:", schema)

       
        def remove_confluent_header(binary):
            if binary is not None and len(binary) > 5:
                return binary[5:]
            return binary

        remove_header_udf = udf(remove_confluent_header, BinaryType())

        return (
            dataframe
            .withColumn("avro_payload", remove_header_udf(col("value")))
            .select(from_avro(col("avro_payload"), schema).alias("data"))
            .select("data.*")
        )
    except Exception as e:
        print(f"Error Deserializing Avro Data for {event_type}:", e)
        raise


checkout_processed = deserialize_avro_data(checkout_stream, "checkout")


checkout_processed = checkout_processed.select(
    col("movie_id").cast("string"),
    col("purchase_amount").cast("double"),
    expr("CAST(timestamp AS timestamp)").alias("timestamp")
)


movie_sales = (
    checkout_processed
    .withWatermark("timestamp", "10 minutes")  
    .groupBy(
        window("timestamp", "1 day"),  
        col("movie_id")
    )
    .agg(
        sum("purchase_amount").alias("total_sales")
    )
    .select(
        col("movie_id"),
        col("total_sales"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        current_timestamp().alias("processing_time")
    )
)


def write_to_snowflake(batch_df, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**snowflake_conf) \
        .option("dbtable", "movie_sales_metrics") \
        .mode("append") \
        .save()


query = (
    movie_sales.writeStream
    .foreachBatch(write_to_snowflake)
    .outputMode("update")  
    .option("checkpointLocation", "checkpointMovieSales")
    .start()
)




query.awaitTermination()