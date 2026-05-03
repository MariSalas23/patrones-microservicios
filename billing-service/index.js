const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

const kafka = new Kafka({
  clientId: "billing",
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
      order_id VARCHAR(100) UNIQUE,
      status VARCHAR(50)
    );
  `);
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
        return; // idempotencia
      }

      await producer.send({
        topic: "payments",
        messages: [{ value: JSON.stringify(evt) }],
      });
    },
  });

  app.get("/", (_, res) => res.send("Billing OK"));
  app.listen(process.env.PORT || 3000);
}

start();