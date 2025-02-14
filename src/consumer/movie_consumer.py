from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro

spark = SparkSession.builder.appName("Test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def write_to_snowflake(micro_df, batch_id):
    if micro_df.isEmpty():
        print(f"Batch {batch_id} is empty. Skipping write.")
        return

    (micro_df
            .write
            .format("snowflake")
            .options(**snowflake_options)
            .option("dbtable", "movies_catalog_enriched")
            .mode("append")
            .save())

    print(f"Batch {batch_id} written successfully.")

def get_schema_from_schema_registry(schema_registry_config, schema_registry_subject):
    sr = SchemaRegistryClient(schema_registry_config)
    latest_version = sr.get_latest_version(schema_registry_subject)

    return latest_version

def spark_consumer():
    df =( spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", kafka_producer_topic)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_SASL_USERNAME}" password="{KAFKA_SASL_PASSWORD}";')
        .load()
    )

    #df.printSchema()

    #  get magic byte value
    df = df.withColumn("magicByte", func.expr("substring(value, 1, 1)"))

    #  get schema id from value
    df = df.withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))

    # remove first 5 bytes from value
    df = df.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))

    # creating a new df with magicBytes, valueSchemaId & fixedValue
    value_df = df.select("magicByte", "valueSchemaId", "fixedValue")

    latest_version_df = get_schema_from_schema_registry(schema_registry_config, schema_registry_subject)

    fromAvroOptions = {"mode":"PERMISSIVE"}
    decoded_output = value_df.select(
        from_avro(
            func.col("fixedValue"), latest_version_df.schema.schema_str, fromAvroOptions
        )
        .alias("data")
    )

    value_df = decoded_output.select("data.*")

    value_df \
    .writeStream \
    .trigger(processingTime='1 second') \
    .outputMode("append") \
    .option("truncate", "false") \
    .foreachBatch(write_to_snowflake) \
    .start() \
    .awaitTermination()

spark_consumer()