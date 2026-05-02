const express = require("express");
const { v4: uuidv4 } = require("uuid");
const kafka = require("./kafka");
const { pool, initDB } = require("./db");

const app = express();
app.use(express.json());

const producer = kafka.producer();

async function start() {
  await initDB();
  await producer.connect();

  app.post("/orders", async (req, res) => {
    const { userId, email } = req.body;
    const orderId = uuidv4();

    await pool.query(
      "INSERT INTO orders (id, user_id, email) VALUES ($1, $2, $3)",
      [orderId, userId, email]
    );

    await producer.send({
      topic: "orders",
      messages: [{ value: JSON.stringify({ orderId, userId, email }) }],
    });

    console.log("OrderCreated:", orderId);
    res.json({ orderId });
  });

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Ordering on", PORT));
}

start();