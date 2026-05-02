require("dotenv").config();
const kafka = require("./kafka");
const { logisticsDB, initDB } = require("../shared/db");

const consumer = kafka.consumer({ groupId: "shipping-group" });
const producer = kafka.producer();

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      try {
        await logisticsDB.query(
          "INSERT INTO shipments (order_id, status) VALUES ($1, $2)",
          [evt.orderId, "CREATED"]
        );
      } catch {}

      const shipmentEvt = {
        orderId: evt.orderId,
        trackingId: "TRK-" + Date.now(),
      };

      await producer.send({
        topic: "shipments",
        messages: [{ value: JSON.stringify(shipmentEvt) }],
      });

      console.log("ShipmentCreated:", shipmentEvt);
    },
  });
}

start();