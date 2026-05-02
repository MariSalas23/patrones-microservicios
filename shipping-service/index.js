const kafka = require("../config");

const consumer = kafka.consumer({ groupId: "shipping-group" });
const producer = kafka.producer();

async function start() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "payment", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payment = JSON.parse(message.value.toString());
      console.log("Creando envío:", payment);

      const shipment = {
        orderId: payment.orderId,
        trackingId: "TRK-" + Date.now(),
      };

      await producer.send({
        topic: "shipments",
        messages: [{ value: JSON.stringify(shipment) }],
      });

      console.log("Envío enviado:", shipment);
    },
  });
}

start();