const express = require("express");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();

const kafka = new Kafka({
  clientId: "shipping",
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
      order_id VARCHAR(100) UNIQUE,
      status VARCHAR(50)
    );
  `);
}

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      console.log("Validando pago y disponibilidad...");

      try {
        await pool.query(
          "INSERT INTO shipments VALUES ($1,$2)",
          [evt.orderId, "CREATED"]
        );
      } catch {
        return;
      }

      console.log("ShipmentCreated:", evt.orderId);

      await producer.send({
        topic: "shipments",
        messages: [{
          value: JSON.stringify({
            ...evt,
            type: "ShipmentCreated"
          })
        }],
      });
    },
  });

  app.get("/", (_, res) => res.send("Shipping OK"));
  app.listen(process.env.PORT || 3000);
}

start();