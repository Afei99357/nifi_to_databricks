from pyspark.sql import SparkSession

def create_table(catalog, schema, table_name, table_schema):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CreateTable").getOrCreate()
    df = spark.createDataFrame([], table_schema)

    # Save the DataFrame as a managed Delta table
    df.write.format("delta").mode("overwrite").saveAsTable("${catalog}.${schema}.${table_name}")


    # Stop the SparkSession
    spark.stop()