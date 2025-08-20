-- Initialize Unity Catalog Delta tables for NiFi to Databricks migration patterns
-- Run this in Databricks SQL or notebook to set up the required tables

-- Create catalog and schema if they don't exist
CREATE CATALOG IF NOT EXISTS nifi_migration;
USE CATALOG nifi_migration;
CREATE SCHEMA IF NOT EXISTS patterns;
USE SCHEMA patterns;

-- 1. Migration Patterns Table
-- Stores processor conversion patterns and templates
CREATE TABLE IF NOT EXISTS migration_patterns (
    processor_class STRING NOT NULL,
    databricks_equivalent STRING,
    description STRING,
    best_practices ARRAY<STRING>,
    code_template STRING,
    last_seen_properties MAP<STRING, STRING>,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'NiFi processor to Databricks migration patterns';

-- Add primary key constraint
ALTER TABLE migration_patterns ADD CONSTRAINT pk_migration_patterns PRIMARY KEY (processor_class);

-- 2. Processor Mappings Table  
-- Maps NiFi processors to their Databricks equivalents
CREATE TABLE IF NOT EXISTS processor_mappings (
    nifi_processor STRING NOT NULL,
    databricks_service STRING,
    complexity_level STRING,
    migration_notes STRING,
    example_properties MAP<STRING, STRING>,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
COMMENT 'NiFi to Databricks processor mappings';

-- 3. Migration History Table
-- Tracks migration attempts and results
CREATE TABLE IF NOT EXISTS migration_history (
    migration_id STRING NOT NULL,
    xml_file_path STRING,
    project_name STRING,
    processor_count INTEGER,
    success_count INTEGER,
    failure_count INTEGER,
    migration_type STRING, -- 'standard' or 'chunked'
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    errors ARRAY<STRING>,
    generated_files ARRAY<STRING>
) USING DELTA
COMMENT 'Migration execution history and results';

-- 4. Controller Services Table
-- Stores NiFi controller service mappings
CREATE TABLE IF NOT EXISTS controller_services (
    service_type STRING NOT NULL,
    databricks_equivalent STRING,
    configuration_mapping MAP<STRING, STRING>,
    setup_instructions STRING,
    dependencies ARRAY<STRING>,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) USING DELTA
COMMENT 'NiFi controller service to Databricks mappings';

-- Insert some common processor patterns to get started
INSERT INTO migration_patterns (processor_class, databricks_equivalent, description, best_practices, code_template, last_seen_properties) VALUES
('GetFile', 'Auto Loader', 'File ingestion via Auto Loader with schema evolution', 
 array('Use schemaLocation for schema tracking', 'Enable includeExistingFiles for initial backfill', 'Use cleanSource after successful processing'),
 'from pyspark.sql.functions import *
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "{format}")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .option("cloudFiles.schemaLocation", "{schema_location}")
      .load("{path}"))',
 map('Directory', '/path/to/input', 'File Filter', '.*')),

('ListFile', 'Auto Loader', 'Directory listing via Auto Loader',
 array('Use schemaLocation for schema tracking', 'Consider includeExistingFiles for backfill'),
 'from pyspark.sql.functions import *
df = (spark.readStream
      .format("cloudFiles") 
      .option("cloudFiles.format", "{format}")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("{path}"))',
 map('Input Directory', '/path/to/input')),

('PutHDFS', 'Delta Lake', 'Transactional storage in Delta Lake',
 array('Partition by frequently filtered columns', 'Use OPTIMIZE for small files', 'Consider Z-ORDER for query performance'),
 'df.write.format("delta").mode("{mode}").option("path", "{path}").saveAsTable("{table_name}")',
 map('Directory', '/path/to/output', 'Hadoop Configuration Resources', '')),

('PutFile', 'Delta Lake', 'File output via Delta Lake',
 array('Use appropriate write mode', 'Consider partitioning strategy'),
 'df.write.format("delta").mode("{mode}").save("{path}")',
 map('Directory', '/path/to/output')),

('ConsumeKafka', 'Structured Streaming', 'Kafka consumer via Structured Streaming',
 array('Use checkpointing for fault tolerance', 'Configure appropriate batch size', 'Handle schema evolution'),
 'df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "{bootstrap_servers}")
      .option("subscribe", "{topics}")
      .option("startingOffsets", "latest")
      .load())
df_parsed = df.select(col("value").cast("string").alias("json_data"))',
 map('Kafka Brokers', 'localhost:9092', 'Topic Name(s)', 'my-topic')),

('PublishKafka', 'Structured Streaming', 'Kafka producer via Structured Streaming',
 array('Use appropriate output mode', 'Configure batch size for performance'),
 'df.writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "{bootstrap_servers}")
  .option("topic", "{topic}")
  .option("checkpointLocation", "{checkpoint_location}")
  .start()',
 map('Kafka Brokers', 'localhost:9092', 'Topic Name', 'output-topic'));

-- Insert common processor mappings
INSERT INTO processor_mappings (nifi_processor, databricks_service, complexity_level, migration_notes) VALUES
('GetFile', 'Auto Loader', 'Low', 'Direct mapping to cloudFiles format'),
('ListFile', 'Auto Loader', 'Low', 'Use cloudFiles with directory monitoring'),
('PutHDFS', 'Delta Lake', 'Low', 'Replace with Delta Lake writes'),
('PutFile', 'Delta Lake', 'Low', 'Use Delta format for transactional writes'),
('ConsumeKafka', 'Structured Streaming', 'Medium', 'Requires Kafka cluster configuration'),
('PublishKafka', 'Structured Streaming', 'Medium', 'Configure output streaming to Kafka'),
('RouteOnAttribute', 'DataFrame Operations', 'Medium', 'Convert to filter and split operations'),
('ConvertRecord', 'DataFrame Transformations', 'Medium', 'Use built-in format conversions'),
('ExecuteSQL', 'Spark SQL', 'Low', 'Direct SQL translation'),
('EvaluateJsonPath', 'JSON Functions', 'Medium', 'Use from_json and json extraction functions');

-- Insert common controller services
INSERT INTO controller_services (service_type, databricks_equivalent, configuration_mapping, setup_instructions) VALUES
('DBCPConnectionPool', 'JDBC Connection', map('Database Connection URL', 'spark.conf jdbc.url', 'Database Driver Class Name', 'spark.conf jdbc.driver'), 'Configure JDBC connection in cluster settings'),
('JsonTreeReader', 'JSON Reader Options', map('Schema Access Strategy', 'spark.read.option schema', 'Schema Text', 'spark.read.schema'), 'Use DataFrame read with JSON options'),
('JsonRecordSetWriter', 'JSON Write Options', map('Pretty Print JSON', 'spark.write.option pretty', 'Suppress Null Values', 'spark.write.option ignoreNullFields'), 'Configure JSON write options'),
('AvroReader', 'Avro Format', map('Schema Access Strategy', 'spark.read.format avro'), 'Use built-in Avro support'),
('CSVReader', 'CSV Options', map('Treat First Line as Header', 'spark.read.option header', 'Value Separator', 'spark.read.option sep'), 'Use DataFrame CSV read options');

-- Show created tables
SHOW TABLES;

-- Display table schemas
DESCRIBE EXTENDED migration_patterns;
DESCRIBE EXTENDED processor_mappings;  
DESCRIBE EXTENDED migration_history;
DESCRIBE EXTENDED controller_services;

PRINT 'Delta tables initialized successfully!';
PRINT 'Tables created in: nifi_migration.patterns';
PRINT 'You can now use the PatternRegistryUC class to interact with these tables.';