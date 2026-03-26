from pyspark.sql.functions import count

# READ SILVER TABLE
df = spark.table("silver_posts")

# AGGREGATION
df_gold = df.groupBy("userId") \
            .agg(count("*").alias("total_posts"))

# WRITE GOLD (overwrite = safe, reproducible)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_posts")

# CREATE TABLE
spark.sql(f"""
CREATE TABLE IF NOT EXISTS gold_posts
USING DELTA
AS SELECT * FROM gold_posts
""")

print("Gold layer completed")