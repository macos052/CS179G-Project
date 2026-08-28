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

app.get('/api/trending', async(req, res) => {
    const category = req.query.category;
    const postDate = req.query.postDate;
    const limit = req.query.limit ? parseInt(req.query.limit) : 10;

    try{
        let SQL = `
            SELECT hashtag, post_date, category, SUM(post_count) AS total_count
            FROM hashtags
            WHERE 1=1
        `;
        
        let parameters = [];

        if(category){
            SQL = SQL.concat(" ", "AND category = ?");
            parameters.push(category);
        }
        
        if(postDate){
            SQL = SQL.concat(" ", "AND post_date = ?");
            parameters.push(postDate);
        }

        SQL = SQL.concat("\n", `
            GROUP BY hashtag, post_date, category
            ORDER BY total_count DESC
            LIMIT ?
        `);
        
        parameters.push(limit);

        const [result] = await db.query(SQL, parameters);

        res.json(result);
    }
    catch(err){
        console.error(err);
        res.status(500).send('Something went wrong');
    }
});

app.get('/api/posts', async(req, res) => {
    const hashtag = req.query.hashtag;
    const category = req.query.category;
    const postDate = req.query.postDate;
    const limit = req.query.limit ? parseInt(req.query.limit) : 10;

    if(!hashtag){
        return res.status(400).json({ error: 'hashtag is required' });
    }

    try{
        let SQL = `
            SELECT uri, author_handle, post_text, hashtag, post_date, category
            FROM post_hashtags
            WHERE hashtag = ?
        `;

        let parameters = [hashtag];

        if(category){
            SQL += " AND category = ?";
            parameters.push(category);
        }
        
        if(postDate){
            SQL += " AND post_date = ?";
            parameters.push(postDate);
        }

        SQL += " LIMIT ?";
        parameters.push(limit);

        const [result] = await db.query(SQL, parameters);

        res.json(result);
    }
    catch(err){
        console.error(err);
        res.status(500).send('Something went wrong');
    }
});

app.listen(3000);