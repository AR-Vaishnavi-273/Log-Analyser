from pyspark.sql.functions import col, count

def count_errors(df):
    return df.filter(col("loggerType")=="ERROR") \
             .groupBy("loggerName") \
             .count() \
             .orderBy(col("count").desc())

def latest_errors(df, limit=10):
    return df.filter(col("loggerType")=="ERROR") \
             .orderBy(col("timestamp").desc()) \
             .limit(limit)

def error_trend(df):
    return df.filter(col("loggerType")=="ERROR") \
             .groupBy("hour") \
             .agg(count("*").alias("error_count")) \
             .orderBy(col("hour"))

def full_trend(df):
    return df.groupBy("hour", "loggerType") \
             .agg(count("*").alias("log_count")) \
             .orderBy("hour", "loggerType")
