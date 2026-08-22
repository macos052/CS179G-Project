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
    LOWER(
        TRANSLATE(
            TRIM(feature.tag),
            'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
            'aaaaaeeeeiiiiooooouuuucaaaaaeeeeiiiiooooouuuuc'
        )
    ) AS hashtag,
    TO_DATE(p.record.createdAt) AS post_date,
    COUNT(DISTINCT p.uri) AS post_count
FROM posts p
LATERAL VIEW explode(record.facets) AS facet
LATERAL VIEW explode(facet.features) AS feature
WHERE feature.`$type` = 'app.bsky.richtext.facet#tag'
  AND feature.tag IS NOT NULL
GROUP BY 
    LOWER(
        TRANSLATE(
            TRIM(feature.tag),
            'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
            'aaaaaeeeeiiiiooooouuuucaaaaaeeeeiiiiooooouuuuc'
        )
    ), 
    TO_DATE(p.record.createdAt)
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
# Clean the variants (like ai!) before categorizing them
daily_ranking_clean = daily_ranking \
  .withColumn("hashtag", regexp_replace(col("hashtag"), "[^a-zA-Z0-9_]", "")) \
  .withColumn("hashtag", trim(regexp_replace(col("hashtag"), "(^_+|_+$)", "")))
daily_ranking_clean.createOrReplaceTempView("daily_ranking")

daily_ranking_categorized = spark.sql("""
SELECT
    hashtag,
    post_date,
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

# Category activity across days
spark.sql("""
SELECT category, post_date, SUM(post_count) AS total_activity
FROM categorized_hashtags
GROUP BY category, post_date
ORDER BY category, post_date
""").show(50)

# --- Write to MySQL ---
mysql_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
mysql_properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Aggregate by (post_date, hashtag) to guarantee unique primary keys for MySQL
final_df = daily_ranking_categorized \
    .groupBy("post_date", "hashtag") \
    .agg(
        sum("post_count").alias("post_count"),
        first("category").alias("category")  # Picks one category if a hashtag spans multiple
    )

# Write to MySQL
final_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(url=mysql_url, table="hashtags", properties=mysql_properties)

print("Write to MySQL complete.")

# Verify by reading it back
verify_df = spark.read.jdbc(url=mysql_url, table="hashtags", properties=mysql_properties)
print(f"Rows in MySQL table: {verify_df.count()}")
verify_df.show(10)
