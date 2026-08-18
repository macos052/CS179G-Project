from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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
    COUNT(*) AS post_count
FROM hashtags_view
GROUP BY post_date, hashtag
ORDER BY post_date DESC, post_count DESC
""")

daily_ranking.write.mode("overwrite").parquet("hashtags_ranked.parquet")

daily_ranking.show(20)

# Total Hashtags Used per Day (Daily Trend)
spark.sql("""
SELECT
    post_date,
    COUNT(hashtag) AS total_hashtags_used,
    COUNT(DISTINCT hashtag) AS unique_hashtags
FROM hashtags_view
GROUP BY post_date
ORDER BY post_date ASC
""").show()

#  Categorization 
daily_ranking.createOrReplaceTempView("daily_ranking")

daily_ranking_categorized = spark.sql("""
SELECT
    post_date,
    hashtag,
    post_count,
    CASE
        WHEN hashtag IN (
            'art','digitalart','illustration','photography',
            'naturephotography','design','architecture','manga','anime'
        ) THEN 'art'

        WHEN hashtag IN (
            'gaming','gamedev','indiegamedev','nintendo',
            'playstation','boardgames'
        ) THEN 'gaming'

        WHEN hashtag IN (
            'tech','ai','programming','python','javascript',
            'cybersecurity','opensource','webdev'
        ) THEN 'tech'

        WHEN hashtag IN (
            'science','space','astronomy','climate'
        ) THEN 'science'

        WHEN hashtag IN (
            'books','booksky','writing','writingcommunity',
            'poetry','history'
        ) THEN 'books_writing'

        WHEN hashtag IN (
            'music','movies','scifi','fantasy','podcasts','vinyl'
        ) THEN 'entertainment'

        WHEN hashtag IN (
            'news','politics','journalism'
        ) THEN 'news_politics'

        WHEN hashtag IN (
            'crypto','finance','economy','realestate','business'
        ) THEN 'finance'

        WHEN hashtag IN (
            'sports','football','basketball','fitness'
        ) THEN 'sports'

        WHEN hashtag IN (
            'cats','dogs','pets','nature','travel','cooking',
            'food','gardening','fashion','beauty','cars'
        ) THEN 'lifestyle'

        WHEN hashtag IN (
            'memes','funny'
        ) THEN 'humor'

        ELSE 'other'
    END AS category
FROM daily_ranking
""")

daily_ranking_categorized.createOrReplaceTempView("categorized_hashtags")
daily_ranking_categorized.show(20)

# --- Write to MySQL ---
mysql_url = "jdbc:mysql://localhost:3306/bluesky_hashtags"
mysql_properties = {
    "user": "root",
    "password": "isabelas",
    "driver": "com.mysql.cj.jdbc.Driver"
}


daily_ranking_categorized.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(url=mysql_url, table="hashtags", properties=mysql_properties)



print("Write to MySQL complete.")
