from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, sum, first

from dotenv import load_dotenv
import os
load_dotenv()

spark = SparkSession.builder \
    .appName("BlueskyHashtagPipeline") \
    .config("spark.jars", "mysql-connector-j-26.7.0.jar") \
    .getOrCreate()

df = spark.read.json("posts_deduped.jsonl")

df.createOrReplaceTempView("posts")

# Extract hashtags
hashtags = spark.sql("""
SELECT
    p.uri,
    p.author.handle AS author_handle,
    p.record.text AS post_text,
    LOWER(
        TRANSLATE(
            TRIM(feature.tag),
            'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
            'aaaaaeeeeiiiiooooouuuucaaaaaeeeeiiiiooooouuuuc'
        )
    ) AS hashtag,
    TO_DATE(p.record.createdAt) AS post_date
FROM posts p
LATERAL VIEW explode(record.facets) AS facet
LATERAL VIEW explode(facet.features) AS feature
WHERE feature.`$type` = 'app.bsky.richtext.facet#tag'
  AND feature.tag IS NOT NULL
""")

hashtags.cache()

# Clean the variants (like ai!) before categorizing them
hashtags_clean = hashtags \
  .withColumn("hashtag", regexp_replace(col("hashtag"), "[^a-zA-Z0-9_]", "")) \
  .withColumn("hashtag", trim(regexp_replace(col("hashtag"), "(^_+|_+$)", "")))
hashtags_clean.createOrReplaceTempView("post_hashtags_clean")