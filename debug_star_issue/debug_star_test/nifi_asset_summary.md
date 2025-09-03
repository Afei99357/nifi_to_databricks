# NiFi Workflow Asset Discovery Report

## Summary
- **Total Processors with Assets**: 4
- **Script Files Found**: 0
- **HDFS Paths Found**: 3
- **Table References Found**: 7
- **SQL Statements Found**: 0
- **Unique Working Directories**: 0
- **Unique Hosts**: 0

## Critical Scripts Requiring Manual Migration

## HDFS Paths Requiring Unity Catalog Migration
- `/user/nifi/logs/critical/${now():format(` ← Used by: PutHDFS_Critical (org.apache.nifi.processors.hadoop.PutHDFS)
- `/user/nifi/logs/error/${now():format(` ← Used by: PutHDFS_Error (org.apache.nifi.processors.hadoop.PutHDFS)
- `/user/nifi/logs/other/${now():format(` ← Used by: PutHDFS_Other (org.apache.nifi.processors.hadoop.PutHDFS)

## Working Directories

## External Hosts/Connections
