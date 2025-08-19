# Generic processor conversion for: org.apache.nifi.processors.standard.ListenHTTP
# Properties: {
  "Base Path": "ingest",
  "Listening Port": "8091",
  "health-check-port": null,
  "Max Data to Receive per Second": null,
  "SSL Context Service": null,
  "HTTP Protocols": "HTTP_1_1",
  "client-authentication": "AUTO",
  "Authorized DN Pattern": ".*",
  "authorized-issuer-dn-pattern": ".*",
  "Max Unconfirmed Flowfile Time": "60 secs",
  "HTTP Headers to receive as Attributes (Regex)": null,
  "Return Code": "200",
  "multipart-request-max-size": "1 MB",
  "multipart-read-buffer-size": "512 KB",
  "max-thread-pool-size": "200",
  "record-reader": null,
  "record-writer": null
}

# TODO: Implement specific logic for this processor
df = spark.read.format('delta').load('/path/to/data')
# ... your transformations ...
