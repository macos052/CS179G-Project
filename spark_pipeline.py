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
category_map = {
    "politics": ["politics", "election", "senate", "congress", "biden", "trump", "government"],
    "sports": ["sports", "football", "basketball", "nba", "nfl", "soccer"],
    "tech": ["tech", "programming", "python", "javascript", "ai", "cybersecurity", "opensource", "webdev"],
    "entertainment": ["movies", "music", "scifi", "fantasy", "books", "writing", "poetry"],
    "art": ["art", "digitalart", "illustration", "photography", "design", "anime", "manga"],
    "lifestyle": ["food", "cooking", "travel", "fitness", "health", "fashion", "beauty"],
    "business": ["business", "economy", "finance", "crypto", "realestate"],
    "news": ["news", "journalism"],
    "gaming": ["gaming", "gamedev", "indiegamedev", "nintendo", "playstation"],
    "animals": ["cats", "dogs", "pets"],
}

def categorize_expr():
    result_expr = None
    for category, keywords in category_map.items():
        cond = None
        for kw in keywords:
            c = col("hashtag").contains(kw)
            cond = c if cond is None else (cond | c)
        if result_expr is None:
            result_expr = when(cond, category)
        else:
            result_expr = result_expr.when(cond, category)
    return result_expr.otherwise("uncategorized")

daily_ranking_categorized = daily_ranking.withColumn("category", categorize_expr())
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

