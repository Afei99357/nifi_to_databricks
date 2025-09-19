from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType, BooleanType, ArrayType

processor_schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("type", StringType(), False),
    StructField("worflow_id", StringType(), False),
    StructField("workflow_name", StringType(), False),
    StructField("parent_group_id", StringType(), True),
    StructField("parent_group_name", StringType(), True),
    StructField("properties_id", StringType(), True),
    StructField("created_date", DateType(), False),
    StructField("last_updated_date", DateType(), False)
])

processor_properties_schema = StructType([
    StructField("processor_id", StringType(), False),
    StructField("property_name", StringType(), False),
    StructField("property_value", StringType(), False),
    StructField("property_rank", StringType(), False),
    StructField("created_date", DateType(), False),
    StructField("last_updated_date", DateType(), False)
])

processor_connections_schema = StructType([
    StructField("source_processor_id", StringType(), False),
    StructField("destination_processor_id", StringType(), False),
    StructField("relationships", ArrayType(StringType()), False),
    StructField("created_date", DateType(), False),
    StructField("last_updated_date", DateType(), False)
])