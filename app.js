const db = require('./db');

const path = require('path');

const express = require('express');

const app = express();

// Set EJS as the view engine
app.set('view engine', 'ejs');

// Explicitly set the directory for your view files
app.set('views', path.join(__dirname, 'views'));

app.use(express.static(path.join(__dirname, 'public')));

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

        const [dates] = await db.query(`
            SELECT DISTINCT post_date
            FROM hashtags
            ORDER BY post_date DESC
        `);

        res.render('index', {topHashtags, categories, dates});
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

    try {

        const dateColumn = postDate
            ? ", MIN(post_date) AS post_date"
            : ", NULL AS post_date";

        let SQL = `
            SELECT
                hashtag,
                category,
                SUM(post_count) AS total_count
                ${dateColumn}
            FROM hashtags
            WHERE 1=1
        `;

        let parameters = [];

        if (category) {
            SQL += " AND category = ?";
            parameters.push(category);
        }

        if (postDate) {
            SQL += " AND post_date = ?";
            parameters.push(postDate);
        }

        SQL += `
            GROUP BY hashtag, category
            ORDER BY total_count DESC
            LIMIT ?
        `;

        parameters.push(limit);

        const [result] = await db.query(SQL, parameters);

        res.json(result);

    } catch(err) {

        console.error(err);
        res.status(500).send('Something went wrong');

    }

});


// Total activity for each category
app.get('/api/category-totals', async(req, res) => {

    try {

        const [result] = await db.query(`
            SELECT
                category,
                SUM(post_count) AS total_activity
            FROM hashtags
            GROUP BY category
            ORDER BY total_activity DESC
        `);

        res.json(result);

    } catch(err) {

        console.error(err);
        res.status(500).send('Something went wrong');

    }

});


// Daily activity trend for a selected category
app.get('/api/category-trend', async(req, res) => {

    const category = req.query.category;

    if (!category) {
        return res.status(400).json({
            error: 'Category is required'
        });
    }

    try {

        const [result] = await db.query(`
            SELECT
                post_date,
                SUM(post_count) AS total_activity
            FROM hashtags
            WHERE category = ?
            GROUP BY post_date
            ORDER BY post_date ASC
        `, [category]);

        res.json(result);

    } catch(err) {

        console.error(err);
        res.status(500).send('Something went wrong');

    }

});

app.listen(3000);