import json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, udf

def parse_json_log(line):
    try:
        data = json.loads(line)
        meta = data.get("Metadata", {})

        return (
            meta.get("Timestamp", "unknown"),
            meta.get("LoggerType", "unknown"),
            data.get("Message", "unknown"),
            meta.get("LoggerName", "unknown"),
            meta.get("Filename", "unknown")
        )
    except:
        return None

