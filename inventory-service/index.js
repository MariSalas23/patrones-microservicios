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
    CREATE TABLE IF NOT EXISTS inventory (
      product_id VARCHAR(50),
      stock INT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id VARCHAR(100) UNIQUE
    );
  `);

  await pool.query(`
    INSERT INTO inventory (product_id, stock)
    VALUES ('prod1', 10)
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
          "INSERT INTO processed_events (event_id) VALUES ($1)",
          [evt.orderId]
        );
      } catch {
        return;
      }

      await pool.query(
        "UPDATE inventory SET stock = stock - 1 WHERE product_id = 'prod1'"
      );
    },
  });

  app.get("/", (_, res) => res.send("Inventory OK"));
  app.listen(process.env.PORT || 3000);
}

start();