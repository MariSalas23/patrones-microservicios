const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "microservices-app",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

module.exports = kafka;