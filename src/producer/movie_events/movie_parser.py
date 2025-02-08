from pyspark.sql.functions import split, col, regexp_extract, regexp_replace, expr

def extract_key_value(df, key):
    df = df.withColumn(
        key,
        expr(f"filter(lines, x -> split(x, '=')[0] = '{key}')[0]")
    ).withColumn(
        key,
        regexp_replace(col(key), f"^{key}=", "")
    )
    
    return df    

def process_movies(spark, file_path):
    MOVIE_ID_PATTERN = r"ITEM (\d+)"
    PRICE_PATTERN = r"\$(\d+\.\d+)"
    REQUIRED_FIELDS = ["Title", "ListPrice"]

    raw_df = spark.read.text(file_path, lineSep="\n\n")

    split_df = (
        raw_df
        .withColumn("lines", split(col("value"), "\n"))
        .withColumn("movie_id", 
                   regexp_extract(col("lines").getItem(0), MOVIE_ID_PATTERN, 1)
                   .cast("int"))
    )

    for field in REQUIRED_FIELDS:
        split_df = extract_key_value(split_df, field)

    processed_df = (
        split_df
        .withColumn("parsed_price",
                   regexp_extract(col("ListPrice"), PRICE_PATTERN, 1)
                   .cast("float"))
        .drop("ListPrice", "value", "lines")
    )

    return processed_df.dropna()
