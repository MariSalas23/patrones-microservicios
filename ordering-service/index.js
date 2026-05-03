const express = require("express");
const { v4: uuidv4 } = require("uuid");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();
app.use(express.json());

// =====================
// KAFKA CONFIG
// =====================
const kafka = new Kafka({
  clientId: "ordering",
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const producer = kafka.producer();

// =====================
// DB CONFIG (COMERCIAL)
// =====================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// =====================
// INIT DB + SEED
// =====================
async function initDB() {
  // Tabla de clientes
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customers (
      id VARCHAR(50) PRIMARY KEY,
      email VARCHAR(150)
    );
  `);

  // Tabla de órdenes
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      order_id VARCHAR(100) PRIMARY KEY,
      user_id VARCHAR(50),
      product_id VARCHAR(50),
      email VARCHAR(150),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Seed clientes (incluye correo del profe)
  await pool.query(`
    INSERT INTO customers (id, email) VALUES
    ('user1', 'daniel.saavedra.fon@gmail.com'),
    ('user2', 'mari.masagu@gmail.com')
    ON CONFLICT DO NOTHING;
  `);

  console.log("DB Comercial lista");
}

// =====================
// START SERVICE
// =====================
async function start() {
  await initDB();
  await producer.connect();

  // =====================
  // CREAR ORDEN
  // =====================
  app.post("/orders", async (req, res) => {
    try {
      const { userId, productId } = req.body;

      if (!userId || !productId) {
        return res.status(400).json({ error: "Faltan datos" });
      }

      // 🔹 Buscar cliente (email parametrizado)
      const customer = await pool.query(
        "SELECT * FROM customers WHERE id=$1",
        [userId]
      );

      if (customer.rowCount === 0) {
        return res.status(404).json({ error: "Cliente no existe" });
      }

      const email = customer.rows[0].email;

      // 🔹 Crear orden
      const orderId = uuidv4();

      await pool.query(
        "INSERT INTO orders (order_id, user_id, product_id, email) VALUES ($1,$2,$3,$4)",
        [orderId, userId, productId, email]
      );

      console.log("OrderCreated:", orderId);

      // 🔹 Publicar evento Kafka
      await producer.send({
        topic: "orders",
        messages: [
          {
            value: JSON.stringify({
              type: "OrderCreated",
              orderId,
              userId,
              productId,
              email,
            }),
          },
        ],
      });

      res.json({ orderId });
    } catch (error) {
      console.error("Error creando orden:", error.message);
      res.status(500).json({ error: "Error interno" });
    }
  });

  // =====================
  // HEALTH CHECK
  // =====================
  app.get("/", (_, res) => res.send("Ordering OK"));

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () =>
    console.log(`Ordering corriendo en puerto ${PORT}`)
  );
}

start();