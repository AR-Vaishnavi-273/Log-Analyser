from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType

from src.parser import parse_json_log
from src.transformer import clean_and_enrich, add_error_category
from src.analytics import count_errors, latest_errors, error_trend, full_trend
from src.io_handlers import read_raw_logs, write_parquet

spark = SparkSession.builder.appName("LogAnalyser").getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("loggerType", StringType(), True),
    StructField("message", StringType(), True),
    StructField("loggerName", StringType(), True),
    StructField("filename", StringType(), True),
])

parse_udf = udf(parse_json_log, schema)

# STEP 1 — Load & Parse
df_raw = read_raw_logs(spark, "data/logs.jsonl")
df = df_raw.withColumn("parsed", parse_udf(col("value"))) \
           .select("parsed.*") \
           .dropna()

# STEP 2 — Transformations
df = clean_and_enrich(df)
df = add_error_category(df)

df.show(truncate=False)

# STEP 3 — Analytics
summary = count_errors(df)
print("Error Summary:")
summary.show()

latest = latest_errors(df)
print("Latest Errors:")
latest.show(truncate=False)

trend_errors = error_trend(df)
trend_all = full_trend(df)

trend_errors.show()
trend_all.show()

# STEP 4 — Save Output
write_parquet(df, "output/clean_logs")
write_parquet(df.filter(col("loggerType").isin("ERROR","WARN")), "output/important_logs")

spark.stop()
