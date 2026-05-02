require("dotenv").config();
const kafka = require("../shared/kafka");
const { commercialDB, initDB } = require("../shared/db");

const consumer = kafka.consumer({ groupId: "billing-group" });
const producer = kafka.producer();

async function start() {
  await initDB();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: "orders", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evt = JSON.parse(message.value.toString());

      // Idempotencia: UNIQUE(order_id)
      try {
        await commercialDB.query(
          "INSERT INTO payments (order_id, status) VALUES ($1, $2)",
          [evt.orderId, "PAID"]
        );
      } catch (e) {
        // ya existe → ignorar
        console.log("Pago duplicado ignorado:", evt.orderId);
      }

      const paymentEvt = { orderId: evt.orderId, status: "PAID" };

      await producer.send({
        topic: "payments",
        messages: [{ value: JSON.stringify(paymentEvt) }],
      });
    },
  });
}

start();