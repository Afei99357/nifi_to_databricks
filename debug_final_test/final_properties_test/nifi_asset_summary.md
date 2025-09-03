# NiFi Workflow Asset Discovery Report

## Summary
- **Total Processors with Assets**: 21
- **Script Files Found**: 8
- **HDFS Paths Found**: 6
- **Table References Found**: 87
- **SQL Statements Found**: 6
- **Unique Working Directories**: 1
- **Unique Hosts**: 5

## Critical Scripts Requiring Manual Migration
- `/users/hadoop_nifi_svc/.ssh/id_rsa` ← Used by: PutSFTP (org.apache.nifi.processors.standard.PutSFTP)
- `/users/hadoop_nifi_svc/scripts/icn8/Impala_Check.sh` ← Used by: Run Impala check query - PMI-View (org.apache.nifi.processors.standard.ExecuteStreamCommand), Run Impala check query - PMI DQ (org.apache.nifi.processors.standard.ExecuteStreamCommand), Run Impala check query - NiFi Loading (org.apache.nifi.processors.standard.ExecuteStreamCommand), Run Impala check query - PMI DQ (org.apache.nifi.processors.standard.ExecuteStreamCommand)
- `/users/hadoop_nifi_svc/scripts/icn8/PMI-View_logging_to_table.sh` ← Used by: PMI-View logging to table - shell script ()
- `/users/hadoop_nifi_svc/scripts/icn8/clear_not_in_stats_folder.sh` ← Used by: Clear not_in_stats folder (org.apache.nifi.processors.standard.ExecuteStreamCommand)
- `/users/hadoop_nifi_svc/scripts/icn8/run_impala_query.sh` ← Used by: Execute Impala Query (org.apache.nifi.processors.standard.ExecuteStreamCommand)

## HDFS Paths Requiring Unity Catalog Migration
- `/user/hive/warehouse/mfg_icn8/` ← Used by: Derive refresh statement (), Derive refresh statement (), Derive refresh statement ()
- `/warehouse/mfg_icn8/` ← Used by: Derive refresh statement (), Derive refresh statement (), Derive refresh statement ()

## Working Directories
- `/users/hadoop_nifi_svc/scripts/icn8/`

## External Hosts/Connections
- `00:00`
- `nardc02hdpnp011.na-rdc02.nxp.com`
- `nardc02prod-impala.na-rdc02.nxp.com`
- `06:30`
- `nbq.com`
