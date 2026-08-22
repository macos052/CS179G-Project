const db = require('./db');

const express = require('express');

const app = express();

// Set EJS as the view engine
app.set('view engine', 'ejs');

// Explicitly set the directory for your view files
app.set('views', path.join(__dirname, 'views'));