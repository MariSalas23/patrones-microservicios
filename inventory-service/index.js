require("dotenv").config();
const kafka = require("../shared/kafka");
const { logisticsDB, initDB } = require("../shared/db");

const consumer = kafka.consumer({ groupId: "inventory-group" });

async function start() {
  await initDB();
  await consumer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      const { rows } = await logisticsDB.query(
        "SELECT stock FROM inventory WHERE product_id = $1",
        ["prod1"]
      );

      if (rows[0].stock > 0) {
        await logisticsDB.query(
          "UPDATE inventory SET stock = stock - 1 WHERE product_id = $1",
          ["prod1"]
        );
        console.log("Stock reservado:", evt.orderId);
      }
    },
  });
}

start();