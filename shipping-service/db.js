const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  // Comercial
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id VARCHAR(50) PRIMARY KEY,
      user_id VARCHAR(50),
      email VARCHAR(100),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS payments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(50) UNIQUE,
      status VARCHAR(20),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Logística
  await pool.query(`
    CREATE TABLE IF NOT EXISTS inventory (
      product_id VARCHAR(50) PRIMARY KEY,
      stock INT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(50) UNIQUE,
      status VARCHAR(20),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Seed
  await pool.query(`
    INSERT INTO inventory (product_id, stock)
    VALUES ('prod1', 10)
    ON CONFLICT DO NOTHING;
  `);

  console.log("✅ DB lista");
}

module.exports = { pool, initDB };