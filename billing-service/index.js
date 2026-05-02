const kafka = require("./kafka");
const { pool, initDB } = require("./db");

const consumer = kafka.consumer({ groupId: "billing-group" });
const producer = kafka.producer();

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "orders" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      try {
        await pool.query(
          "INSERT INTO payments (order_id, status) VALUES ($1, $2)",
          [evt.orderId, "PAID"]
        );
      } catch {
        console.log("Pago duplicado");
      }

      await producer.send({
        topic: "payments",
        messages: [{ value: JSON.stringify({ orderId: evt.orderId }) }],
      });

      console.log("PaymentProcessed:", evt.orderId);
    },
  });
}

start();