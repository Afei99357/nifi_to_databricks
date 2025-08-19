# EvaluateJsonPath → get_json_object() or from_json()
# Extracts values from JSON using JSONPath

df.withColumn('value', get_json_object(col('json'), '$.{path}'))