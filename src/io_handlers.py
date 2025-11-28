def read_raw_logs(spark, path):
    return spark.read.text(path)

def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)
