# GetFile → Auto Loader
# Continuously monitors and ingests files from a directory

spark.readStream.format('cloudFiles').option('cloudFiles.format', '{format}').load('{path}')

# Best Practices:
# - Use cloudFiles.schemaEvolutionMode for schema changes
# - Set cloudFiles.maxFilesPerTrigger to control batch size
# - Enable cloudFiles.includeExistingFiles for initial load