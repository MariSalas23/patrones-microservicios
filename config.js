const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "microservices-app",
  brokers: ["<BOOTSTRAP_SERVER>"], // 👈 de Confluent
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: "<API_KEY>",
    password: "<API_SECRET>",
  },
});

module.exports = kafka;