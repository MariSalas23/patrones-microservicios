const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

const kafka = new Kafka({
  clientId: "billing-service",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const consumer = kafka.consumer({ groupId: "billing-group" });
const producer = kafka.producer();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS payments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(50) UNIQUE,
      status VARCHAR(20)
    );
  `);
  console.log("DB ready (billing)");
}

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "orders" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      try {
        await pool.query(
          "INSERT INTO payments (order_id, status) VALUES ($1,$2)",
          [evt.orderId, "PAID"]
        );
      } catch {
        console.log("Duplicate payment ignored");
      }

      await producer.send({
        topic: "payments",
        messages: [{ value: JSON.stringify({ orderId: evt.orderId }) }],
      });

      console.log("PaymentProcessed:", evt.orderId);
    },
  });

  app.get("/", (req, res) => res.send("Billing OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Billing on", PORT));
}

start();