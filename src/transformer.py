from pyspark.sql.functions import col, to_timestamp, to_date, hour, when, lower


def clean_and_enrich(df):
    return df \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .withColumn("date", to_date("timestamp")) \
        .withColumn("hour", hour("timestamp")) \
        .orderBy(col("timestamp").desc())

def add_error_category(df):
    return df.withColumn(
        "error_category",
    when(col("loggerType") != "ERROR", "No issue")
    .when(lower(col("message")).contains("database"), "DB_ERROR")
    .when(lower(col("message")).contains("timeout"), "TIMEOUT")
    .when(lower(col("message")).contains("failed"), "FAILURE")
    .when(lower(col("message")).contains("authentication failed"), "AUTH_ERROR")
    .when(lower(col("message")).contains("kafka"), "KAFKA_ERROR")
    .when(lower(col("message")).contains("cache"), "CACHE_ERROR")
    .when(lower(col("message")).contains("token expired"), "TOKEN_EXPIRED")
    .otherwise("GENERAL_ERROR")
    )
