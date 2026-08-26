-- verify.sql
-- Run these against the bluesky_hashtags database to confirm 
-- the Spark pipeline successfully loaded data into MySQL.

-- Total row count
SELECT COUNT(*) AS total_rows FROM hashtags;

-- Sample rows
SELECT * FROM hashtags LIMIT 10;

-- Breakdown by category
SELECT category, COUNT(*) AS num_entries, SUM(post_count) AS total_uses
FROM hashtags
GROUP BY category
ORDER BY total_uses DESC;

-- Date range covered
SELECT MIN(post_date) AS earliest_date, MAX(post_date) AS latest_date
FROM hashtags;

-- Category activity across days (the TA-required analysis)
SELECT category, post_date, SUM(post_count) AS total_activity
FROM hashtags
GROUP BY category, post_date
ORDER BY category, post_date;