require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs');

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  database: process.env.MYSQL_DATABASE,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  waitForConnections: true,
  connectionLimit: 10,
  multipleStatements: true
});

async function poolSchema(){
    try{
        const schemaSql = fs.readFileSync('schema.sql', 'utf-8');
        await pool.query(schemaSql);

    }
    catch(err){
        console.error(err);
    }
}

poolSchema();

module.exports = pool;