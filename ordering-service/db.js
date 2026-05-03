const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // NEON COMERCIAL
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  // CLIENTES
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customers (
      id VARCHAR(50) PRIMARY KEY,
      name VARCHAR(100),
      email VARCHAR(150)
    );
  `);

  // ÓRDENES
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      order_id VARCHAR(100) PRIMARY KEY,
      user_id VARCHAR(100),
      product_id VARCHAR(100),
      email VARCHAR(150),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // SEED CLIENTES
  await pool.query(`
    INSERT INTO customers (id, name, email) VALUES
    ('user1', 'Cliente Profesor', 'daniel.saavedra.fon@gmail.com'),
    ('user2', 'Cliente Test', 'mari.masagu@gmail.com')
    ON CONFLICT DO NOTHING;
  `);

  console.log("DB comercial (ordering) lista");
}

module.exports = { pool, initDB };