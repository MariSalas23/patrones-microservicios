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

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS products (
      id VARCHAR(50) PRIMARY KEY,
      stock INT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(100) UNIQUE
    );
  `);

  await pool.query(`
    INSERT INTO products VALUES ('prod1',10)
    ON CONFLICT DO NOTHING;
  `);
}

async function start() {
  await initDB();
  await consumer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      try {
        await pool.query(
          "INSERT INTO processed_events VALUES ($1)",
          [evt.orderId]
        );
      } catch {
        return;
      }

      console.log("Validando stock...");

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