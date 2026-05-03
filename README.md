**Nombres:** Katherin Juliana Moreno Carvajal, Mariana Salas Gutiérrez

# Microservicios con Kafka - Sistema de Órdenes

## 1. Descripción

Sistema distribuido basado en microservicios que implementa un flujo de órdenes usando Kafka (Confluent) y dos bases de datos (Comercial y Logística). 

Hay cinco microservicios: 

* **ordering-service:** Gestiona la creación de órdenes y publica el evento `OrderCreated`.
* **billing-service:** Procesa el pago y emite el evento `PaymentProcessed`.
* **inventory-service:** Valida disponibilidad y reserva el stock de productos.
* **shipping-service:** Genera el envío de la orden.
* **notification-service:** Envía notificaciones por correo electrónico al usuario.

Los servicios se comunican mediante Kafka usando los siguientes tópicos:

* `orders`
* `payments`
* `shipments`

Acerca de las bases de datos, se tienen dos:

* **DB Comercial**
  * Usada por: Ordering y Billing
  * Contiene: órdenes y pagos
* **DB Logística**
  * Usada por: Inventory y Shipping
  * Contiene: productos y envíos

## 2. Clientes (parametrizados)

| ID    | Email                                                                 |
| ----- | --------------------------------------------------------------------- |
| user1 | [daniel.saavedra.fon@gmail.com](mailto:daniel.saavedra.fon@gmail.com) |
| user2 | [mari.masagu@gmail.com](mailto:mari.masagu@gmail.com)                 |


## 3. Productos disponibles

| ID     | Nombre      |
| ------ | ----------- |
| prod1  | Laptop      |
| prod2  | Mouse       |
| prod3  | Teclado     |
| prod4  | Monitor     |
| prod5  | Audifonos   |
| prod6  | Webcam      |
| prod7  | Disco SSD   |
| prod8  | Memoria RAM |
| prod9  | Silla Gamer |
| prod10 | Router WiFi |

## 4. ¿Cómo probar? (Postman)

### 4.1. Crear orden

**POST**

**Endpoint: ** `/orders`

**URL completa: ** `https://ordering-service-gbw3.onrender.com/orders`

**Body (JSON)**

```json
{
  "userId": "user1",
  "productId": "prod1"
}
```

## 4.2. Resultado esperado (logs)

```
OrderCreated: <orderId>
Billing procesa el pago: <orderId>
PaymentProcessed: <orderId>
Inventory valida y reserva stock: <productId>
Shipping genera el envío: <orderId>
Notification informa al usuario: <email>
```

# 5. Ejemplo

## 6. Tecnologías usadas

* Node.js
* Kafka (Confluent Cloud)
* PostgreSQL (Neon)
* Render (despliegue de los microservicios)
* SendGrid

# 7. Estructura del Proyecto

```
patrones-microservicios/
│
├── ordering-service/
│   ├── index.js
│   ├── package.json
│   └── .env
│
├── billing-service/
│   ├── index.js
│   ├── package.json
│   └── .env
│
├── inventory-service/
│   ├── index.js
│   ├── package.json
│   └── .env
│
├── shipping-service/
│   ├── index.js
│   ├── package.json
│   └── .env
│
├── notification-service/
│   ├── index.js
│   ├── package.json
│   └── .env
│
└── README.md
```
