from databricks.sdk.runtime import *

def create_table(catalog, schema, table_name, table_schema):
    # Initialize Spark session
    
    df = spark.createDataFrame([], table_schema)
    # Save the DataFrame as a managed Delta table
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")
