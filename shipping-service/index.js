const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

const kafka = new Kafka({
  clientId: "shipping-service",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const consumer = kafka.consumer({ groupId: "shipping-group" });
const producer = kafka.producer();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipments (
      id SERIAL PRIMARY KEY,
      order_id VARCHAR(50) UNIQUE,
      status VARCHAR(20)
    );
  `);

  console.log("DB ready (shipping)");
}

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      try {
        await pool.query(
          "INSERT INTO shipments (order_id, status) VALUES ($1,$2)",
          [evt.orderId, "CREATED"]
        );
      } catch {}

      await producer.send({
        topic: "shipments",
        messages: [{ value: JSON.stringify({ orderId: evt.orderId }) }],
      });

      console.log("ShipmentCreated:", evt.orderId);
    },
  });

  app.get("/", (req, res) => res.send("Shipping OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Shipping on", PORT));
}

start();