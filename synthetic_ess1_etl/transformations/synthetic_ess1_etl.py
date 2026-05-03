import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Schema for the JSON files - update with actual column names and types
json_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("geometry", StringType(), True),  # Using STRING for VARIANT compatibility
    StructField("geometry_name", StringType(), True),
    StructField("properties", StringType(), True)  # Using STRING for VARIANT compatibility
])

# Define the schema for geometry JSON - update with actual field names
geometry_schema = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", StringType(), True),
    # Add more fields as needed based on your JSON structure
])

# Define the schema for properties JSON - update with actual field names
properties_schema = StructType([
    StructField("__change__", StringType(), True),  # Contains INSERT, UPDATE, or DELETE
    StructField("property_name", StringType(), True),
    StructField("unit_of_property_id", StringType(), True),
    # Add more fields as needed based on your JSON structure
])


# Temporary view for CDC source - ingests and transforms data from Google Drive
@dp.temporary_view()
def raw_gdrive_data():
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
            col("properties_parsed.*"),  # Flatten all fields from properties struct including __change__
            col("geometry_parsed.type").alias("geometry_type"),  # Alias to avoid duplicate
            col("geometry_parsed.coordinates")
        )
        .withColumn("processing_timestamp", current_timestamp())  # Add timestamp for CDC sequencing
    )


# CDC Target Table - SCD Type 2 for historical tracking (THE ONLY STREAMING TABLE)
dp.create_streaming_table(
    name="places.enterprise_steady_state.linz_property_unit_of_property",
    comment="LINZ property unit of property data with full history tracking (SCD Type 2)"
)

# CDC Flow - Apply changes with SCD Type 2
# __change__ column contains the operation type (INSERT/UPDATE/DELETE)
# processing_timestamp is used to sequence events and determine __START_AT and __END_AT
dp.create_auto_cdc_flow(
    target="places.enterprise_steady_state.linz_property_unit_of_property",
    source="raw_gdrive_data",
    keys=["id"],  # Primary key - update if unit_of_property_id is more appropriate
    sequence_by="processing_timestamp",  # Timestamp column for sequencing - __START_AT and __END_AT will be timestamps
    #apply_as_deletes="__change__ = 'DELETE'",  # Identify DELETE operations to properly set __END_AT
    stored_as_scd_type=2  # SCD Type 2: Enables historical tracking with __START_AT and __END_AT columns
)
