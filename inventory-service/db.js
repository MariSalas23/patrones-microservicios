const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // NEON LOGISTICS
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  // PRODUCTOS
  await pool.query(`
    CREATE TABLE IF NOT EXISTS products (
      id VARCHAR(50) PRIMARY KEY,
      name VARCHAR(100),
      stock INT
    );
  `);

  // IDEMPOTENCIA
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(100) UNIQUE
    );
  `);

  // SEED PRODUCTOS
  await pool.query(`
    INSERT INTO products (id, name, stock) VALUES
    ('prod1', 'Laptop', 10),
    ('prod2', 'Mouse', 25),
    ('prod3', 'Teclado', 15),
    ('prod4', 'Monitor', 8),
    ('prod5', 'Audifonos', 20),
    ('prod6', 'Webcam', 12),
    ('prod7', 'Disco SSD', 18),
    ('prod8', 'Memoria RAM', 30),
    ('prod9', 'Silla Gamer', 5),
    ('prod10', 'Router WiFi', 14)
    ON CONFLICT DO NOTHING;
  `);

  console.log("DB logística (inventory) lista");
}

module.exports = { pool, initDB };