from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, sum, first
from pyspark.sql.functions import coalesce, lit

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

mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT")
mysql_database = os.getenv("MYSQL_DATABASE")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")

mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

category_lookup = spark.read.jdbc(
    url=mysql_url,
    table="(SELECT DISTINCT hashtag, category FROM hashtags) AS lookup", # small DataFrame with just two columns: hashtag and category, one row per unique hashtag
    properties=mysql_properties
)

post_hashtags_final = hashtags_clean.join(
    category_lookup,
    on="hashtag",
    how="left"
).withColumn("category", coalesce(col("category"), lit("other"))) # handles null category result from left join into 'other'

# Write to MySQL
post_hashtags_final.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(url=mysql_url, table="post_hashtags", properties=mysql_properties)

print("Write to post_hashtags complete.")

verify_df = spark.read.jdbc(url=mysql_url, table="post_hashtags", properties=mysql_properties)
print(f"Rows in post_hashtags table: {verify_df.count()}")
verify_df.show(5)