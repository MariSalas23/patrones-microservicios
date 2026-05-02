const kafka = require("../config");
const express = require("express");

const app = express();
app.use(express.json());

const producer = kafka.producer();

async function start() {
  await producer.connect();

  app.post("/order", async (req, res) => {
    const order = {
      orderId: Date.now().toString(),
      userId: req.body.userId,
      items: req.body.items,
    };

    await producer.send({
      topic: "orders",
      messages: [{ value: JSON.stringify(order) }],
    });

    console.log("Orden enviada:", order);
    res.send(order);
  });

  app.listen(3001, () => console.log("orders-service en 3001"));
}

start();