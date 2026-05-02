const kafka = require("../config");
const { pool, initDB } = require("../db");

const consumer = kafka.consumer({ groupId: "billing-group" });
const producer = kafka.producer();

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());

      await pool.query(
        "INSERT INTO payments (order_id, status) VALUES ($1, $2)",
        [order.orderId, "PAID"]
      );

      const payment = {
        orderId: order.orderId,
        status: "PAID",
      };

      await producer.send({
        topic: "payment",
        messages: [{ value: JSON.stringify(payment) }],
      });
    },
  });
}

start();