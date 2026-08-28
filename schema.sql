CREATE TABLE IF NOT EXISTS hashtags (
    id INT AUTO_INCREMENT PRIMARY KEY,
    post_date DATE NOT NULL,
    hashtag VARCHAR(255) NOT NULL,
    post_count INT NOT NULL,
    category VARCHAR(100),
    UNIQUE KEY uniq_hashtag_date (hashtag, post_date),
    INDEX idx_category (category),
    INDEX idx_post_date (post_date)
);

CREATE TABLE IF NOT EXISTS post_hashtags (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uri VARCHAR(500) NOT NULL,
    hashtag VARCHAR(255) NOT NULL,
    post_date DATE NOT NULL,
    category VARCHAR(100),
    post_text TEXT,
    author_handle VARCHAR(255),
    INDEX idx_hashtag (hashtag),
    INDEX idx_category (category)
);