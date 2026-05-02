const kafka = require("./kafka");
const express = require("express");

const app = express();
const consumer = kafka.consumer({ groupId: "notification-group" });

async function start() {
  await consumer.connect();

  await consumer.subscribe({ topic: "orders" });
  await consumer.subscribe({ topic: "payments" });
  await consumer.subscribe({ topic: "shipments" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      console.log(`${topic}:`, message.value.toString());
    },
  });

  app.get("/", (req, res) => res.send("Notification OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Notification on", PORT));
}

start();