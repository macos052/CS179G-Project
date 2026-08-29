from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, sum, first
import csv
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

# --- Additional analysis: most active days, ranked ---
daily_activity_rows = spark.sql("""
SELECT
    post_date,
    COUNT(hashtag) AS total_hashtags_used,
    COUNT(DISTINCT hashtag) AS unique_hashtags
FROM hashtags_view
GROUP BY post_date
ORDER BY total_hashtags_used DESC
""").collect()

with open("daily_activity.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["post_date", "total_hashtags_used", "unique_hashtags"])
    for row in daily_activity_rows:
        writer.writerow([row["post_date"], row["total_hashtags_used"], row["unique_hashtags"]])

print(f"Exported {len(daily_activity_rows)} rows to daily_activity.csv (ranked by activity)")

#  Categorization 
# Clean the variants (like ai!) before categorizing them
daily_ranking_clean = daily_ranking \
  .withColumn("hashtag", regexp_replace(col("hashtag"), "[^a-zA-Z0-9_]", "")) \
  .withColumn("hashtag", trim(regexp_replace(col("hashtag"), "(^_+|_+$)", ""))) \
  .filter(col("hashtag") != "")

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

# Sample of hashtags that matched a real category (not 'other')
spark.sql("""
SELECT hashtag, post_date, post_count, category
FROM categorized_hashtags
WHERE category != 'other'
LIMIT 10
""").show()

# Category activity across days
spark.sql("""
SELECT category, post_date, SUM(post_count) AS total_activity
FROM categorized_hashtags
GROUP BY category, post_date
ORDER BY category, post_date
""").show(50)

# --- Export CSVs for reporting and per-category plotting ---
category_activity_rows = spark.sql("""
    SELECT category, post_date, SUM(post_count) AS total_activity
    FROM categorized_hashtags
    GROUP BY category, post_date
    ORDER BY category, post_date
""").collect()

with open("category_activity.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["category", "post_date", "total_activity"])
    for row in category_activity_rows:
        writer.writerow([row["category"], row["post_date"], row["total_activity"]])

print(f"Exported {len(category_activity_rows)} rows to category_activity.csv")

# --- Additional analysis: top category per day (including 'other') ---
from collections import defaultdict

activity_by_day = defaultdict(list)
for row in category_activity_rows:
    activity_by_day[row["post_date"]].append((row["category"], row["total_activity"]))

top_category_rows = []
for post_date, cats in activity_by_day.items():
    top_cat, top_activity = max(cats, key=lambda x: x[1])
    top_category_rows.append((post_date, top_cat, top_activity))

top_category_rows.sort(key=lambda x: x[0])  # chronological order

with open("top_category_per_day.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["post_date", "top_category", "activity_count"])
    for post_date, top_cat, top_activity in top_category_rows:
        writer.writerow([post_date, top_cat, top_activity])

print(f"Exported {len(top_category_rows)} rows to top_category_per_day.csv")

top_hashtags_rows = spark.sql("""
    SELECT hashtag, category, SUM(post_count) AS total_count
    FROM categorized_hashtags
    GROUP BY hashtag, category
    ORDER BY total_count DESC
""").collect()

with open("top_hashtags.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["hashtag", "category", "total_count"])
    for row in top_hashtags_rows:
        writer.writerow([row["hashtag"], row["category"], row["total_count"]])

print(f"Exported {len(top_hashtags_rows)} rows to top_hashtags.csv")

# --- MySQL connection details (needed by both the analysis write below and the main write later) ---
mysql_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
mysql_properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# --- Additional analysis: top 3 categories per day (excluding 'other') ---
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

category_activity_df = spark.sql("""
    SELECT category, post_date, SUM(post_count) AS total_activity
    FROM categorized_hashtags
    WHERE category != 'other'
    GROUP BY category, post_date
""")

rank_window = Window.partitionBy("post_date").orderBy(desc("total_activity"))

top3_per_day_df = category_activity_df \
    .withColumn("rank", row_number().over(rank_window)) \
    .filter(col("rank") <= 3) \
    .orderBy("post_date", "rank")

top3_per_day_df.show(30)

# Export to CSV
top3_rows = top3_per_day_df.collect()
with open("top3_categories_per_day.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["post_date", "rank", "category", "total_activity"])
    for row in top3_rows:
        writer.writerow([row["post_date"], row["rank"], row["category"], row["total_activity"]])

print(f"Exported {len(top3_rows)} rows to top3_categories_per_day.csv")

# Write to a new MySQL table (separate from the main 'hashtags' table)
top3_per_day_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(url=mysql_url, table="top3_categories_per_day", properties=mysql_properties)

# Verify the top3_categories_per_day write succeeded
verify_top3 = spark.read.jdbc(url=mysql_url, table="top3_categories_per_day", properties=mysql_properties)
print(f"Rows in top3_categories_per_day table: {verify_top3.count()}")
verify_top3.show(10)

print("Wrote top3_categories_per_day to MySQL.")

# Aggregate by (post_date, hashtag) to guarantee unique primary keys for MySQL
final_df = daily_ranking_categorized \
    .groupBy("post_date", "hashtag") \
    .agg(
        sum("post_count").alias("post_count"),
        first("category").alias("category")  # Picks one category if a hashtag spans multiple
    )

# --- Write to MySQL (main hashtags table) ---
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