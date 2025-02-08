from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


spark = SparkSession.builder \
    .appName("UserRegistrationAndSessionMetrics") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .getOrCreate()


schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_time", TimestampType())
])


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS") \
    .option("subscribe", "user_registration_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json", "timestamp") \
    .select(from_json("json", schema).alias("data"), "timestamp") \
    .select("data.*", "timestamp")


df_with_watermark = df.withWatermark("event_time", "2 hours")


tumbling_window_counts = df_with_watermark \
    .groupBy(
        window(col("event_time"), "1 hour").alias("window")
    ) \
    .agg(count("user_id").alias("registration_count")) \
    .select(
        col("window.start").alias("timestamp"),
        expr("'tumbling'").alias("window_type"),
        col("registration_count")
    )

sliding_window_counts = df_with_watermark \
    .groupBy(
        window(col("event_time"), "1 hour", "15 minutes").alias("window")
    ) \
    .agg(count("user_id").alias("registration_count")) \
    .select(
        col("window.start").alias("timestamp"),
        expr("'sliding'").alias("window_type"),
        col("registration_count")
    )


final_df = tumbling_window_counts.union(sliding_window_counts)

snowflake_options = {
    "sfUrl": "your_snowflake_url",
    "sfUser": "your_username",
    "sfPassword": "your_password",
    "sfDatabase": "your_database",
    "sfSchema": "your_schema",
    "sfWarehouse": "your_warehouse",
    "dbtable": "user_registration_metrics"
}

query = final_df.writeStream \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .outputMode("append") \
    .start()

query.awaitTermination()