DROP TABLE IF EXISTS hashtags;

CREATE TABLE hashtags (
    id INT AUTO_INCREMENT PRIMARY KEY,
    post_date DATE NOT NULL,
    hashtag VARCHAR(255) NOT NULL,
    post_count INT NOT NULL,
    category VARCHAR(100),
    UNIQUE KEY uniq_hashtag_date (hashtag, post_date),
    INDEX idx_category (category),
    INDEX idx_post_date (post_date)
);