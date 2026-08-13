from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BlueskyHashtagPipeline") \
    .getOrCreate()

df = spark.read.json("posts_deduped.jsonl")

df.createOrReplaceTempView("posts")

# Extract hashtags
hashtags = spark.sql("""
  SELECT 
    p.uri,
    TO_DATE(p.record.createdAt) AS post_date,
    LOWER(feature.tag) AS hashtag
  FROM posts p
  LATERAL VIEW explode(record.facets) AS facet
  LATERAL VIEW explode(facet.features) AS feature
  WHERE feature.`$type` = 'app.bsky.richtext.facet#tag'
""")

hashtags.cache()

# Register as a temporary view
hashtags.createOrReplaceTempView("hashtags_view")

# Hashtag counts per day (daily ranking)
daily_ranking = spark.sql("""
SELECT
    post_date,
    hashtag,
    COUNT(*) AS count
FROM hashtags_view
GROUP BY post_date, hashtag
ORDER BY post_date DESC, count DESC
""")