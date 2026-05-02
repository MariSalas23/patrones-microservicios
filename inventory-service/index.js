const kafka = require("./kafka");
const { pool, initDB } = require("./db");

const consumer = kafka.consumer({ groupId: "inventory-group" });

async function start() {
  await initDB();
  await consumer.connect();

  await consumer.subscribe({ topic: "payments" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      await pool.query(
        "UPDATE inventory SET stock = stock - 1 WHERE product_id = 'prod1'"
      );

      console.log("Stock actualizado:", evt.orderId);
    },
  });
}

start();