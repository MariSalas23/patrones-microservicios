require("dotenv").config();
const kafka = require("../shared/kafka");
const { logisticsDB, initDB } = require("../shared/db");

const consumer = kafka.consumer({ groupId: "shipping-group" });
const producer = kafka.producer();

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "payments", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      // Idempotencia: UNIQUE(order_id)
      try {
        await logisticsDB.query(
          "INSERT INTO shipments (order_id, status) VALUES ($1, $2)",
          [evt.orderId, "CREATED"]
        );
      } catch (e) {
        console.log("Envío duplicado ignorado:", evt.orderId);
      }

      const shipmentEvt = {
        orderId: evt.orderId,
        trackingId: "TRK-" + Date.now(),
      };

      await producer.send({
        topic: "shipments",
        messages: [{ value: JSON.stringify(shipmentEvt) }],
      });
    },
  });
}

start();