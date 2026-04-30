import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YourAppName") \
    .getOrCreate()

class DeltaTableHandler(logging.Handler):
    def __init__(self, table_name):
        super().__init__()
        self.table_name = 'places.observability.logging'
        # Define a consistent schema for your log table
        self.schema = StructType([
            StructField("created", TimestampType(), True),
            StructField("log_level", StringType(), True),
            StructField("logger", StringType(), True),
            StructField("message", StringType(), True)
        ])

    def emit(self, record):
        try:
            # Create a single-row Spark DataFrame from the log record
            log_entry = [(
                datetime.fromtimestamp(record.created),
                record.levelname,
                record.name,
                self.format(record)
            )]
            df = spark.createDataFrame(log_entry, self.schema)
            
            # Append log to the Delta table
            df.write.format("delta").mode("append").saveAsTable(self.table_name)
        except Exception:
            self.handleError(record)

