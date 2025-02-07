from pyspark.sql.functions import split, col, regexp_extract, regexp_replace, expr

def extract_key_value(df, key):
    df = df.withColumn(
        key,
        expr(f"filter(value, x -> split(x, '=')[0] = '{key}')[0]")
    ).withColumn(
        key,
        regexp_replace(col(key), f"^{key}=", "")
    )
    
    return df

def process_movies(spark, file_path):
    
    df = spark.read.text(file_path, lineSep="\n\n")
    df_split = df.withColumn("value", split(df["value"], "\n"))

    df_extracted = df_split.withColumn(
        "movie_id", regexp_extract(col("value").getItem(0), r"ITEM (\d+)", 1).cast("int")
    )

    required_keys = ["Title", "ListPrice"]
    for key in required_keys:
        df_extracted = extract_key_value(df_extracted, key)

    df_extracted = df_extracted.withColumn(
        "parsed_price", regexp_extract(col("ListPrice"), r"\$(\d+\.\d+)", 1).cast("float")
    ).drop("ListPrice")

    return df_extracted.filter(col("parsed_price").isNotNull() &
                                col("movie_id").isNotNull() &
                                col("Title").isNotNull())
