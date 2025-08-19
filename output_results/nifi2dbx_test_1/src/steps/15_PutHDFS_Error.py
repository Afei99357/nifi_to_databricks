# PutHDFS → Delta Lake Write
# Writes data to HDFS/cloud storage with ACID guarantees

df.write.format('delta').mode('{mode}').save('{path}')

# Best Practices:
# - Use partitioning for query optimization
# - Enable auto-optimization for small files
# - Implement Z-ordering for frequently queried columns