const express = require("express");
const { Kafka } = require("kafkajs");
const nodemailer = require("nodemailer");

const app = express();

// =====================
// KAFKA CONFIG
// =====================
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

// =====================
// EMAIL CONFIG (GMAIL)
// =====================
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS, // App Password
  },
});

// =====================
// SEND EMAIL FUNCTION
// =====================
async function sendEmail(to, subject, text) {
  try {
    await transporter.sendMail({
      from: process.env.EMAIL_USER,
      to,
      subject,
      text,
    });

    console.log("Email enviado a:", to);
  } catch (error) {
    console.error("Error enviando email:", error.message);
  }
}

// =====================
// START SERVICE
// =====================
async function start() {
  await consumer.connect();

  await consumer.subscribe({ topic: "orders" });
  await consumer.subscribe({ topic: "payments" });
  await consumer.subscribe({ topic: "shipments" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const data = JSON.parse(message.value.toString());

      console.log("Evento recibido:", topic, data);

      // EMAIL DESTINO (del evento o fallback)
      const email = data.email || "daniel.saavedra.fon@gmail.com";

      await sendEmail(
        email,
        `Evento: ${topic}`,
        `Se generó el evento "${topic}" para la orden ${data.orderId}`
      );
    },
  });

  app.get("/", (req, res) => res.send("Notification OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log("Notification on", PORT));
}

start();