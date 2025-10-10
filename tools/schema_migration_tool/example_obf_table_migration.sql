-- ============================================================
-- Example Table Migration: obf_schema.obf_table_raw
-- Generated from Hive external table DDL
-- Target: Databricks Delta Lake
-- ============================================================

-- ============================================================
-- OPTION 1: DELTA TABLE (RECOMMENDED FOR PRODUCTION)
-- ============================================================

-- Step 1: Create schema
CREATE SCHEMA IF NOT EXISTS obf_schema
COMMENT 'Schema for obfuscated tables';

-- Step 2: Create Delta table with optimized types
CREATE TABLE obf_schema.obf_table_raw (
  -- Data columns
  col_a STRING,
  col_b_ts TIMESTAMP,          -- Converted from STRING
  col_c_ts TIMESTAMP,          -- Converted from STRING
  col_d INT,
  col_e STRING,
  col_f INT,
  col_g STRING,
  col_h DOUBLE,
  col_i DOUBLE,
  col_j DOUBLE,
  col_k DOUBLE,
  col_l_ts TIMESTAMP,          -- Converted from STRING
  col_m INT,
  col_n STRING,
  col_o STRING,
  col_p_ts TIMESTAMP,          -- Converted from STRING
  col_q_ts TIMESTAMP,          -- Converted from STRING
  col_r STRING,
  col_s STRING,
  col_t DOUBLE,
  col_u DOUBLE,
  col_v DOUBLE,
  col_w STRING,
  col_x_ts TIMESTAMP,          -- Converted from STRING
  col_y INT,
  col_z_ts TIMESTAMP,          -- Converted from STRING

  -- Partition columns (kept as STRING for flexibility)
  part_a_ts STRING,
  part_b_ts STRING,
  part_suffix STRING
)
USING DELTA
PARTITIONED BY (part_a_ts, part_b_ts, part_suffix)
LOCATION 'dbfs:/mnt/warehouse/obf_schema/obf_table_raw'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration' = 'interval 30 days'
)
COMMENT 'Migrated from HDFS external Parquet table';

-- Step 3: Initial verification
DESCRIBE EXTENDED obf_schema.obf_table_raw;

-- Step 4: Check partitions (after data load)
-- SHOW PARTITIONS obf_schema.obf_table_raw;

-- Step 5: Sample query
-- SELECT * FROM obf_schema.obf_table_raw LIMIT 10;

-- ============================================================
-- DATA LOADING (use Databricks notebook for actual migration)
-- ============================================================

-- If migrating from Hive with timestamp conversion:
-- INSERT INTO obf_schema.obf_table_raw
-- SELECT
--   col_a,
--   to_timestamp(col_b_ts, 'yyyy-MM-dd HH:mm:ss') AS col_b_ts,
--   to_timestamp(col_c_ts, 'yyyy-MM-dd HH:mm:ss') AS col_c_ts,
--   col_d,
--   col_e,
--   col_f,
--   col_g,
--   col_h,
--   col_i,
--   col_j,
--   col_k,
--   to_timestamp(col_l_ts, 'yyyy-MM-dd HH:mm:ss') AS col_l_ts,
--   col_m,
--   col_n,
--   col_o,
--   to_timestamp(col_p_ts, 'yyyy-MM-dd HH:mm:ss') AS col_p_ts,
--   to_timestamp(col_q_ts, 'yyyy-MM-dd HH:mm:ss') AS col_q_ts,
--   col_r,
--   col_s,
--   col_t,
--   col_u,
--   col_v,
--   col_w,
--   to_timestamp(col_x_ts, 'yyyy-MM-dd HH:mm:ss') AS col_x_ts,
--   col_y,
--   to_timestamp(col_z_ts, 'yyyy-MM-dd HH:mm:ss') AS col_z_ts,
--   part_a_ts,
--   part_b_ts,
--   part_suffix
-- FROM source_table;

-- ============================================================
-- POST-MIGRATION OPTIMIZATION
-- ============================================================

-- Run OPTIMIZE to compact small files
OPTIMIZE obf_schema.obf_table_raw;

-- Optional: Z-ORDER by frequently filtered columns
-- Adjust these columns based on your query patterns
OPTIMIZE obf_schema.obf_table_raw
ZORDER BY (col_a, part_suffix);

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Check row count
SELECT COUNT(*) as total_rows
FROM obf_schema.obf_table_raw;

-- Check partition distribution
SELECT
  part_a_ts,
  part_b_ts,
  part_suffix,
  COUNT(*) as row_count,
  MIN(col_b_ts) as min_timestamp,
  MAX(col_b_ts) as max_timestamp
FROM obf_schema.obf_table_raw
GROUP BY part_a_ts, part_b_ts, part_suffix
ORDER BY part_a_ts, part_b_ts, part_suffix;

-- Check for null values in key columns
SELECT
  'col_a' as column_name,
  COUNT(*) as null_count
FROM obf_schema.obf_table_raw
WHERE col_a IS NULL
UNION ALL
SELECT
  'col_b_ts' as column_name,
  COUNT(*) as null_count
FROM obf_schema.obf_table_raw
WHERE col_b_ts IS NULL;
-- Add more columns as needed

-- Check table statistics
DESCRIBE DETAIL obf_schema.obf_table_raw;

-- Check table history (Delta feature)
DESCRIBE HISTORY obf_schema.obf_table_raw;

-- ============================================================
-- OPTION 2: EXTERNAL PARQUET TABLE (MINIMAL CHANGES)
-- Use this for quick lift-and-shift migration
-- ============================================================

-- CREATE EXTERNAL TABLE obf_schema.obf_table_raw_parquet (
--   col_a STRING,
--   col_b_ts STRING,
--   col_c_ts STRING,
--   col_d INT,
--   col_e STRING,
--   col_f INT,
--   col_g STRING,
--   col_h DOUBLE,
--   col_i DOUBLE,
--   col_j DOUBLE,
--   col_k DOUBLE,
--   col_l_ts STRING,
--   col_m INT,
--   col_n STRING,
--   col_o STRING,
--   col_p_ts STRING,
--   col_q_ts STRING,
--   col_r STRING,
--   col_s STRING,
--   col_t DOUBLE,
--   col_u DOUBLE,
--   col_v DOUBLE,
--   col_w STRING,
--   col_x_ts STRING,
--   col_y INT,
--   col_z_ts STRING
-- )
-- PARTITIONED BY (
--   part_a_ts STRING,
--   part_b_ts STRING,
--   part_suffix STRING
-- )
-- STORED AS PARQUET
-- LOCATION 'dbfs:/mnt/warehouse/obf_schema/obf_table_raw_parquet';

-- Repair partitions after creation
-- MSCK REPAIR TABLE obf_schema.obf_table_raw_parquet;

-- ============================================================
-- MIGRATION COMPARISON
-- ============================================================

-- If you created both tables, compare them:
-- SELECT
--   'Delta Table' as table_type,
--   COUNT(*) as row_count
-- FROM obf_schema.obf_table_raw
-- UNION ALL
-- SELECT
--   'Parquet Table' as table_type,
--   COUNT(*) as row_count
-- FROM obf_schema.obf_table_raw_parquet;

-- ============================================================
-- MAINTENANCE QUERIES
-- ============================================================

-- Run OPTIMIZE regularly (e.g., daily)
-- OPTIMIZE obf_schema.obf_table_raw;

-- Vacuum old files (after retention period)
-- VACUUM obf_schema.obf_table_raw RETAIN 168 HOURS;  -- 7 days

-- Update table statistics
-- ANALYZE TABLE obf_schema.obf_table_raw COMPUTE STATISTICS;

-- ============================================================
-- GRANT PERMISSIONS (adjust as needed)
-- ============================================================

-- GRANT SELECT ON TABLE obf_schema.obf_table_raw TO `data_analysts`;
-- GRANT ALL PRIVILEGES ON TABLE obf_schema.obf_table_raw TO `data_engineers`;

-- ============================================================
-- NOTES
-- ============================================================
--
-- Original HDFS location:
--   hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw
--
-- New Databricks location:
--   dbfs:/mnt/warehouse/obf_schema/obf_table_raw
--
-- Key differences from original Hive table:
-- 1. Timestamp columns converted from STRING to TIMESTAMP
-- 2. Using Delta format instead of Parquet
-- 3. Auto-optimization enabled
-- 4. ACID transactions supported
-- 5. Time travel enabled (can query historical versions)
--
-- Query historical data (Delta time travel):
--   SELECT * FROM obf_schema.obf_table_raw VERSION AS OF 1;
--   SELECT * FROM obf_schema.obf_table_raw TIMESTAMP AS OF '2024-01-01';
--
-- ============================================================
