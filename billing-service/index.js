require("dotenv").config();
const kafka = require("../shared/kafka");
const { commercialDB, initDB } = require("../shared/db");

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
        await commercialDB.query(
          "INSERT INTO payments (order_id, status) VALUES ($1, $2)",
          [evt.orderId, "PAID"]
        );
      } catch {
        console.log("Pago duplicado ignorado");
      }

      const paymentEvt = { orderId: evt.orderId, status: "PAID" };

      await producer.send({
        topic: "payments",
        messages: [{ value: JSON.stringify(paymentEvt) }],
      });

      console.log("PaymentProcessed:", paymentEvt);
    },
  });
}

start();