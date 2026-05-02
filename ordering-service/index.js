require("dotenv").config();
const express = require("express");
const { v4: uuidv4 } = require("uuid");
const kafka = require("../shared/kafka");
const { commercialDB, initDB } = require("../shared/db");

const app = express();
app.use(express.json());

const producer = kafka.producer();

async function start() {
  await initDB();
  await producer.connect();

  app.post("/orders", async (req, res) => {
    const { userId, email, items } = req.body;

    const orderId = uuidv4();

    await commercialDB.query(
      "INSERT INTO orders (id, user_id, email) VALUES ($1, $2, $3)",
      [orderId, userId, email]
    );

    const event = { orderId, userId, email, items };

    await producer.send({
      topic: "orders",
      messages: [{ value: JSON.stringify(event) }],
    });

    console.log("OrderCreated:", event);
    res.json({ orderId });
  });

  app.listen(3000, () => console.log("Ordering on 3000"));
}

start();