const kafka = require("../config");

const consumer = kafka.consumer({ groupId: "notification-group" });

async function start() {
  await consumer.connect();

  await consumer.subscribe({ topic: "shipments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const shipment = JSON.parse(message.value.toString());
      console.log("Notificación enviada al usuario:", shipment);
    },
  });
}

start();