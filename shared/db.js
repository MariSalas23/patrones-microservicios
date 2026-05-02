const { Pool } = require("pg");

const commercialDB = new Pool({
  connectionString: process.env.DB_COMMERCIAL_URL,
  ssl: { rejectUnauthorized: false },
});

const logisticsDB = new Pool({
  connectionString: process.env.DB_LOGISTICS_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  // Comercial
  await commercialDB.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id VARCHAR(50) PRIMARY KEY,
      user_id VARCHAR(50),
      email VARCHAR(100),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await commercialDB.query(`
    CREATE TABLE IF NOT EXISTS payments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(50) UNIQUE, -- idempotencia
      status VARCHAR(20),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Logística
  await logisticsDB.query(`
    CREATE TABLE IF NOT EXISTS inventory (
      product_id VARCHAR(50) PRIMARY KEY,
      stock INT
    );
  `);

  await logisticsDB.query(`
    CREATE TABLE IF NOT EXISTS shipments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(50) UNIQUE, -- idempotencia
      status VARCHAR(20),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Seed productos
  await logisticsDB.query(`
    INSERT INTO inventory (product_id, stock)
    VALUES ('prod1', 10), ('prod2', 10)
    ON CONFLICT (product_id) DO NOTHING;
  `);
}

module.exports = { commercialDB, logisticsDB, initDB };