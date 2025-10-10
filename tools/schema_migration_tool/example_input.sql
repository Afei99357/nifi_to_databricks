%sql
CREATE EXTERNAL TABLE obf_schema.obf_table_raw (
  col_a STRING,
  col_b_ts STRING,
  col_c_ts STRING,
  col_d INT,
  col_e STRING,
  col_f INT,
  col_g STRING,
  col_h DOUBLE,
  col_i DOUBLE,
  col_j DOUBLE,
  col_k DOUBLE,
  col_l_ts STRING,
  col_m INT,
  col_n STRING,
  col_o STRING,
  col_p_ts STRING,
  col_q_ts STRING,
  col_r STRING,
  col_s STRING,
  col_t DOUBLE,
  col_u DOUBLE,
  col_v DOUBLE,
  col_w STRING,
  col_x_ts STRING,
  col_y INT,
  col_z_ts STRING
)
PARTITIONED BY (
  part_a_ts STRING,
  part_b_ts STRING,
  part_suffix STRING
)
STORED AS PARQUET
LOCATION 'hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw'
