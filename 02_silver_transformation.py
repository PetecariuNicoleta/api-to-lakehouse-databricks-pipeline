from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# READ BRONZE
df = spark.read.table("bronze_posts")

# BUSINESS DEDUPLICATION
window = Window.partitionBy("id").orderBy(col("ingestion_time").desc())

df_clean = df.withColumn("rn", row_number().over(window)) \
             .filter("rn = 1") \
             .drop("rn")

# DATA QUALITY
df_clean = df_clean.filter(col("id").isNotNull())

# CREATE TABLE IF NOT EXISTS
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_posts
USING DELTA
AS SELECT * FROM bronze_posts WHERE 1=0
""")

# IDEMPOTENT MERGE (IMPORTANT)
df_clean.createOrReplaceTempView("updates")

spark.sql("""
MERGE INTO silver_posts t
USING updates s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Silver layer completed")