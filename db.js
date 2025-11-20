// db.js

require('dotenv').config();
const mysql = require('mysql2/promise');
const crypto = require('crypto');

const encryptionKey = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');

const pool = mysql.createPool({
  connectionLimit: 10,
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_NAME || "TraderProBOT",
  waitForConnections: true,
  queueLimit: 0
});

async function handleDatabaseConnection() {
  try {
    const connection = await pool.getConnection();
    console.log("[TraderProBot] Database Connected!");
    connection.release();
  } catch (err) {
    console.error('[TraderProBot] Database connection error:', err);
    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
      console.log('[TraderProBot] Reconnecting to database...');
      setTimeout(handleDatabaseConnection, 2000);
    } else {
      throw err;
    }
  }
}

pool.on('error', async (err) => {
  console.error('[TraderProBot] Unexpected database error:', err);
  if (err.code === 'PROTOCOL_CONNECTION_LOST') {
    await handleDatabaseConnection();
  }
});

setInterval(async () => {
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    console.error('[TraderProBot] Keepalive query failed:', err);
  }
}, 60000);

async function dbQuery(sql, params = []) {
  try {
    const [results] = await pool.execute(sql, params);
    return results;
  } catch (error) {
    console.error('Database query error:', error);
    throw error;
  }
}

module.exports = {
  pool,
  handleDatabaseConnection,
  encryptionKey,
  dbQuery
};