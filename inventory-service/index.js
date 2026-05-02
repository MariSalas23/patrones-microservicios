const kafka = require("./kafka");
const { pool, initDB } = require("./db");
const express = require("express");

const app = express();
const consumer = kafka.consumer({ groupId: "inventory-group" });

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

      console.log("Stock actualizado:", evt.orderId);
    },
  });

  app.get("/", (req, res) => res.send("Inventory OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Inventory on", PORT));
}

start();