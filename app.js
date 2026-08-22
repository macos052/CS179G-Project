const db = require('./db');

const path = require('path');

const express = require('express');

const app = express();

// Set EJS as the view engine
app.set('view engine', 'ejs');

// Explicitly set the directory for your view files
app.set('views', path.join(__dirname, 'views'));

app.get('/', async(req, res) => {
    try{
        const [topHashtags] = await db.query(`
            SELECT hashtag, SUM(post_count) AS total_count
            FROM hashtags
            GROUP BY hashtag
            ORDER BY total_count DESC
            LIMIT 10
        `);

        const [categories] = await db.query(`
            SELECT DISTINCT category FROM hashtags ORDER BY category
        `);
        
        res.render('index', {topHashtags, categories});
    }
    catch(err){
        console.error(err);
        res.status(500).send('Something went wrong');
    }
});

app.listen(3000);