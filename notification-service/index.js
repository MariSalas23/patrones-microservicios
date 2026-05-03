const express = require("express");
const { Kafka } = require("kafkajs");
const sgMail = require("@sendgrid/mail");

const app = express();

const kafka = new Kafka({
  clientId: "notification",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const consumer = kafka.consumer({ groupId: "notification-group" });

sgMail.setApiKey(process.env.SENDGRID_API_KEY);

const processed = new Set();

async function start() {
  await consumer.connect();

  console.log("Notification iniciado");

  await consumer.subscribe({ topic: "shipments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      if (evt.type !== "ShipmentCreated") return;

      if (processed.has(evt.eventId)) return;
      processed.add(evt.eventId);

      console.log("Notification informa al usuario:", evt.email);

      await sgMail.send({
        to: evt.email,
        from: process.env.EMAIL_USER,
        subject: "Orden completada",
        text: `Orden ${evt.orderId} completada`
      });
    },
  });

  app.get("/", (_, res) => res.send("Notification OK"));

  app.listen(process.env.PORT || 3000);
}

start();