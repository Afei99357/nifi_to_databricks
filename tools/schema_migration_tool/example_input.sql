CREATE EXTERNAL TABLE sales.customer_orders (
  order_id INT,
  customer_id INT,
  customer_name STRING,
  order_date STRING,
  order_timestamp_ts STRING,
  total_amount DOUBLE,
  status STRING
)
PARTITIONED BY (
  year INT,
  month INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/sales.db/customer_orders'
