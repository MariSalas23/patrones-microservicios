const kafka = require("./kafka");
const { pool, initDB } = require("./db");

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
        await pool.query(
          "INSERT INTO shipments (order_id, status) VALUES ($1, $2)",
          [evt.orderId, "CREATED"]
        );
      } catch {}

      await producer.send({
        topic: "shipments",
        messages: [{ value: JSON.stringify({ orderId: evt.orderId }) }],
      });

      console.log("ShipmentCreated:", evt.orderId);
    },
  });
}

start();