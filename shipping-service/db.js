const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // NEON LOGISTICS
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(100) UNIQUE,
      status VARCHAR(50),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  console.log("DB logística (shipping) lista");
}

module.exports = { pool, initDB };