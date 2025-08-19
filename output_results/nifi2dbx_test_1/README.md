# nifi2dbx_test_1

Generated from NiFi flow.

## Contents
- `src/steps/` individual processor translations
- `databricks.yml` Databricks Asset Bundle (jobs-as-code)
- `jobs/` Jobs 2.1 JSON (DAG-based and single-task)
- `conf/` parameter contexts & controller services

## Next steps
1. Review/merge per-step code into your main notebook or DLT pipeline.
2. Use `databricks bundle validate && databricks bundle deploy` (if using Bundles).
3. Or create a job from `jobs/job.json` and run it.