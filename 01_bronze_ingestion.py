import requests
from pyspark.sql.functions import current_timestamp, col

# API CALL
url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)
data = response.json()

df = spark.createDataFrame(data)

# ADD METADATA
df = df.withColumn("ingestion_time", current_timestamp())

# IDEMPOTENCY: remove exact duplicates before write
df = df.dropDuplicates()

# WRITE (append)
df.write.format("delta") \
  .mode("append") \
  .saveAsTable("bronze_posts")

print("Bronze load completed")