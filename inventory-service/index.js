const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

const kafka = new Kafka({
  clientId: "inventory",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const consumer = kafka.consumer({ groupId: "inventory-group" });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// =====================
// INIT DB (NO TOCO TUS PRODUCTOS)
// =====================
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS products (
      id VARCHAR(50) PRIMARY KEY,
      name VARCHAR(100),
      stock INT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(100) PRIMARY KEY
    );
  `);

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

  console.log("DB logística lista");
}

// =====================
// START SERVICE
// =====================
async function start() {
  await initDB();
  await consumer.connect();

  await consumer.subscribe({ topic: "payments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      // 🔥 VALIDAR EVENTO
      if (evt.type !== "PaymentProcessed") return;

      // 🔥 IDEMPOTENCIA REAL
      try {
        await pool.query(
          "INSERT INTO processed_events (event_id) VALUES ($1)",
          [evt.eventId]
        );
      } catch {
        console.log("Evento duplicado ignorado");
        return;
      }

      console.log("Validando stock...");

      const result = await pool.query(
        "SELECT stock FROM products WHERE id=$1",
        [evt.productId]
      );

      if (result.rowCount === 0) {
        console.log("Producto no existe");
        return;
      }

      if (result.rows[0].stock <= 0) {
        console.log("Sin stock");
        return;
      }

      await pool.query(
        "UPDATE products SET stock = stock - 1 WHERE id=$1",
        [evt.productId]
      );

      console.log("Stock reservado:", evt.productId);
    },
  });

  app.get("/", (_, res) => res.send("Inventory OK"));
  app.listen(process.env.PORT || 3000);
}

start();