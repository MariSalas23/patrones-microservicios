const express = require("express");
const { v4: uuidv4 } = require("uuid");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "ordering",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const producer = kafka.producer();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      order_id VARCHAR(100) PRIMARY KEY,
      user_id VARCHAR(100),
      email VARCHAR(150)
    );
  `);
}

async function start() {
  await initDB();
  await producer.connect();

  app.post("/orders", async (req, res) => {
    const { userId, email } = req.body;
    const orderId = uuidv4();

    await pool.query(
      "INSERT INTO orders (order_id, user_id, email) VALUES ($1,$2,$3)",
      [orderId, userId, email]
    );

    await producer.send({
      topic: "orders",
      messages: [{ value: JSON.stringify({ orderId, userId, email }) }],
    });

    res.json({ orderId });
  });

  app.get("/", (_, res) => res.send("Ordering OK"));

  app.listen(process.env.PORT || 3000);
}

start();