require("dotenv").config();
const kafka = require("../shared/kafka");

const consumer = kafka.consumer({ groupId: "notification-group" });

async function start() {
  await consumer.connect();

  await consumer.subscribe({ topic: "orders", fromBeginning: false });
  await consumer.subscribe({ topic: "payments", fromBeginning: false });
  await consumer.subscribe({ topic: "shipments", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const evt = JSON.parse(message.value.toString());
      console.log(`📧 Notificación (${topic}):`, evt);
      // aquí puedes integrar nodemailer si quieres enviar correo real
    },
  });
}

start();