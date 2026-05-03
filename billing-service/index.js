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
      order_id VARCHAR(100) PRIMARY KEY,
      status VARCHAR(50)
    );
  `);
}

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      if (evt.type !== "OrderCreated") return;

      try {
        await pool.query(
          "INSERT INTO payments (order_id, status) VALUES ($1,$2)",
          [evt.orderId, "PAID"]
        );
      } catch (err) {
        if (err.code === "23505") return;
        throw err;
      }

      await producer.send({
        topic: "payments",
        messages: [{
          key: evt.orderId,
          value: JSON.stringify({
            ...evt,
            eventId: evt.eventId,
            type: "PaymentProcessed"
          }),
        }],
      });
    },
  });

  app.listen(process.env.PORT || 3000);
}

start();