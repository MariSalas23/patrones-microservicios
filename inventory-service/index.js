const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

const kafka = new Kafka({
  clientId: "inventory-service",
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
      product_id VARCHAR(50) PRIMARY KEY,
      stock INT
    );
  `);

  await pool.query(`
    INSERT INTO inventory (product_id, stock)
    VALUES ('prod1', 10)
    ON CONFLICT DO NOTHING;
  `);

  console.log("DB ready (inventory)");
}

async function start() {
  await initDB();
  await consumer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      await pool.query(
        "UPDATE inventory SET stock = stock - 1 WHERE product_id = 'prod1'"
      );

      console.log("InventoryUpdated:", evt.orderId);
    },
  });

  app.get("/", (req, res) => res.send("Inventory OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Inventory on", PORT));
}

start();