const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();

const kafka = new Kafka({
  clientId: "notification-service",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const consumer = kafka.consumer({ groupId: "notification-group" });

async function start() {
  await consumer.connect();

  await consumer.subscribe({ topic: "orders" });
  await consumer.subscribe({ topic: "payments" });
  await consumer.subscribe({ topic: "shipments" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      console.log(topic, message.value.toString());
    },
  });

  app.get("/", (req, res) => res.send("Notification OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Notification on", PORT));
}

start();