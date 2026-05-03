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
// EMAIL CONFIG (GMAIL FIXED)
// =====================
const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 587,
  secure: false,
  requireTLS: true,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
  tls: {
    rejectUnauthorized: false,
    family: 4, // 🔥 SOLUCIÓN ERROR ENETUNREACH (Render IPv6)
  },
});

// =====================
// VERIFY EMAIL CONFIG
// =====================
async function verifyEmail() {
  try {
    await transporter.verify();
    console.log("SMTP listo");
  } catch (error) {
    console.error("Error SMTP:", error.message);
  }
}

// =====================
// SEND EMAIL FUNCTION
// =====================
async function sendEmail(to, subject, text) {
  try {
    await transporter.sendMail({
      from: `"Microservices App" <${process.env.EMAIL_USER}>`,
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
  await verifyEmail();

  await consumer.connect();

  await consumer.subscribe({ topic: "orders" });
  await consumer.subscribe({ topic: "payments" });
  await consumer.subscribe({ topic: "shipments" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const data = JSON.parse(message.value.toString());

        console.log("Evento recibido:", topic, data);

        const email = data.email || "daniel.saavedra.fon@gmail.com";

        await sendEmail(
          email,
          `Evento: ${topic}`,
          `Orden ${data.orderId}\nEvento: ${topic}\n\nSistema de microservicios funcionando correctamente.`
        );
      } catch (error) {
        console.error("Error procesando mensaje:", error.message);
      }
    },
  });

  app.get("/", (req, res) => res.send("Notification OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () =>
    console.log(`Notification corriendo en puerto ${PORT}`)
  );
}

start();