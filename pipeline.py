from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BlueskyHashtagPipeline") \
    .getOrCreate()

df = spark.read.json("posts_deduped.jsonl")

df.createOrReplaceTempView("posts")