from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HashtagCategorization") \
    .getOrCreate()

# Read the daily hashtag ranking created by spark_pipeline.py
daily_ranking = spark.read.parquet("hashtags_ranked.parquet")

# Register it as a temporary SQL view
daily_ranking.createOrReplaceTempView("daily_ranking")

# Categorize hashtags using Spark SQL CASE WHEN
daily_ranking_categorized = spark.sql("""
SELECT
    post_date,
    hashtag,
    post_count,

    CASE
        WHEN hashtag IN (
            'art', 'digitalart', 'illustration', 'photography',
            'naturephotography', 'design', 'architecture',
            'manga', 'anime'
        ) THEN 'art'

        WHEN hashtag IN (
            'gaming', 'gamedev', 'indiegamedev',
            'nintendo', 'playstation', 'boardgames'
        ) THEN 'gaming'

        WHEN hashtag IN (
            'tech', 'ai', 'programming', 'python',
            'javascript', 'cybersecurity', 'opensource', 'webdev'
        ) THEN 'tech'

        WHEN hashtag IN (
            'science', 'space', 'astronomy', 'climate'
        ) THEN 'science'

        WHEN hashtag IN (
            'books', 'booksky', 'writing',
            'writingcommunity', 'poetry', 'history'
        ) THEN 'books_writing'

        WHEN hashtag IN (
            'music', 'movies', 'scifi',
            'fantasy', 'podcasts', 'vinyl'
        ) THEN 'entertainment'

        WHEN hashtag IN (
            'news', 'politics', 'journalism'
        ) THEN 'news_politics'

        WHEN hashtag IN (
            'crypto', 'finance', 'economy',
            'realestate', 'business'
        ) THEN 'finance'

        WHEN hashtag IN (
            'sports', 'football', 'basketball', 'fitness'
        ) THEN 'sports'

        WHEN hashtag IN (
            'cats', 'dogs', 'pets', 'nature',
            'travel', 'cooking', 'food', 'gardening',
            'fashion', 'beauty', 'cars'
        ) THEN 'lifestyle'

        WHEN hashtag IN (
            'memes', 'funny'
        ) THEN 'humor'

        ELSE 'other'
    END AS category

FROM daily_ranking
""")

# Temporary view for later SQL queries / Part 3 web app
daily_ranking_categorized.createOrReplaceTempView(
    "categorized_hashtags"
)

print("=== Categorized Hashtags ===")

daily_ranking_categorized.orderBy(
    "post_date",
    "post_count",
    ascending=[False, False]
).show(30, truncate=False)

print("=== Category Summary ===")

spark.sql("""
SELECT
    category,
    SUM(post_count) AS total_hashtag_uses,
    COUNT(DISTINCT hashtag) AS unique_hashtags
FROM categorized_hashtags
GROUP BY category
ORDER BY total_hashtag_uses DESC
""").show(truncate=False)

print("=== Top Unclassified Hashtags ===")

spark.sql("""
SELECT
    hashtag,
    SUM(post_count) AS total_uses
FROM categorized_hashtags
WHERE category = 'other'
GROUP BY hashtag
ORDER BY total_uses DESC
LIMIT 20
""").show(truncate=False)

# Save classified result for later MySQL loading
daily_ranking_categorized.write \
    .mode("overwrite") \
    .parquet("categorized_hashtags.parquet")

spark.stop()
