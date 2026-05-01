import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Schema for the JSON files - update with actual column names and types
json_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("geometry", StringType(), True),  # Using STRING for VARIANT compatibility
    StructField("geometry_name", StringType(), True),
    StructField("properties", StringType(), True)  # Using STRING for VARIANT compatibility
])

# Define the schema for properties JSON - update with actual field names
properties_schema = StructType([
    StructField("__change__", StringType(), True),
    StructField("property_name", StringType(), True),
    StructField("unit_of_property_id", StringType(), True),
    # Add more fields as needed based on your JSON structure
])

# Define the schema for geometry JSON - update with actual field names
geometry_schema = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", StringType(), True),
    # Add more fields as needed based on your JSON structure
])

# synthetic ess2 Ingest from Google Drive ---
# Auto Loader (cloudFiles) detects new files from your GDrive connection
dp.create_streaming_table(
    name="raw_gdrive_data",
    comment="Incremental raw ingestion from Google Drive with flattened properties"
)

@dp.append_flow(target="raw_gdrive_data")
def ingest_from_gdrive():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("databricks.connection", "gcs_apikle")
        .option("header", "true")
        .schema(json_schema)
        .load("https://drive.google.com/drive/u/0/folders/17NnUwQYh6lvHrk41Cbvju-PgtqwTejKu")
    )
    
    # Parse both JSON columns first, then flatten in a single select
    return (
        df
        .withColumn("properties_parsed", from_json(col("properties"), properties_schema))
        .withColumn("geometry_parsed", from_json(col("geometry"), geometry_schema))
        .select(
            col("type"),
            col("id"),
            col("geometry_name"),
            col("properties_parsed.*"),  # Flatten all fields from properties struct
            col("geometry_parsed.type").alias("geometry_type"),  # Alias to avoid duplicate
            col("geometry_parsed.coordinates")
        )
    )