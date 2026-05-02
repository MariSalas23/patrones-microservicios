const kafka = require("../config");
const { pool, initDB } = require("../db");

const consumer = kafka.consumer({ groupId: "inventory-group" });

async function start() {
  await initDB();
  await consumer.connect();

  await consumer.subscribe({ topic: "shipments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const shipment = JSON.parse(message.value.toString());

      await pool.query(
        "INSERT INTO inventory (order_id) VALUES ($1)",
        [shipment.orderId]
      );
    },
  });
}

start();