const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // NEON COMERCIAL
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS payments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(100) UNIQUE,
      status VARCHAR(50),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  console.log("DB comercial (billing) lista");
}

module.exports = { pool, initDB };