require("dotenv").config();
const kafka = require("./kafka");

const consumer = kafka.consumer({ groupId: "notification-group" });

async function start() {
  await consumer.connect();

  await consumer.subscribe({ topic: "orders" });
  await consumer.subscribe({ topic: "payments" });
  await consumer.subscribe({ topic: "shipments" });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const evt = JSON.parse(message.value.toString());
      console.log(`${topic}:`, evt);
    },
  });
}

start();