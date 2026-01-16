import amqp from "amqplib";
import express from "express";
import fs from "fs";
import jwt from "jsonwebtoken";
import path from "path";
import swaggerUi from "swagger-ui-express";
import yaml from "yaml";
import { randomUUID } from "crypto";
import { fileURLToPath } from "url";
import { setTimeout as delay } from "timers/promises";

const cfg = {
  port: parseInt(process.env.ORDER_API_PORT || process.env.PORT || "8080", 10),
  jwtSigningKey: process.env.JWT_SIGNING_KEY || "change_me",
  jwtIssuer: process.env.JWT_ISSUER || "integrahub",
  jwtAudience: process.env.JWT_AUDIENCE || "integrahub-api",
  rabbitHost: process.env.RABBITMQ_HOST || "localhost",
  rabbitPort: process.env.RABBITMQ_PORT || "5672",
  rabbitUser: process.env.RABBITMQ_USER || "integrahub",
  rabbitPass: process.env.RABBITMQ_PASS || "change_me",
  rabbitVhost: process.env.RABBITMQ_VHOST || "/",
  rawExchange: process.env.RAW_EXCHANGE || "integrahub.events.raw",
  rawRoutingKey: process.env.RAW_ROUTING_KEY || "events.raw",
  openApiPath: process.env.OPENAPI_PATH || "/app/contracts/api/order-api.yaml"
};

const orders = new Map();
const rabbit = { connection: null, channel: null, ready: false };

function log(level, message, extra) {
  const base = `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}`;
  if (extra) {
    console.log(base, JSON.stringify(extra));
  } else {
    console.log(base);
  }
}

function buildAmqpUrl() {
  const vhostRaw = cfg.rabbitVhost || "/";
  const vhost = vhostRaw === "/" ? "%2F" : encodeURIComponent(vhostRaw.replace(/^\//, ""));
  const user = encodeURIComponent(cfg.rabbitUser);
  const pass = encodeURIComponent(cfg.rabbitPass);
  return `amqp://${user}:${pass}@${cfg.rabbitHost}:${cfg.rabbitPort}/${vhost}`;
}

async function connectRabbit() {
  let attempt = 0;
  while (true) {
    try {
      const connection = await amqp.connect(buildAmqpUrl());
      const channel = await connection.createChannel();
      await channel.assertExchange(cfg.rawExchange, "topic", { durable: true });
      rabbit.connection = connection;
      rabbit.channel = channel;
      rabbit.ready = true;
      connection.on("close", () => {
        rabbit.ready = false;
        log("error", "RabbitMQ connection closed");
        process.exit(1);
      });
      connection.on("error", (err) => {
        log("error", "RabbitMQ connection error", { error: err.message });
      });
      log("info", "RabbitMQ connected");
      return;
    } catch (err) {
      attempt += 1;
      const waitMs = Math.min(1000 * attempt, 10000);
      log("warn", "RabbitMQ connect failed, retrying", {
        attempt,
        waitMs,
        error: err.message
      });
      await delay(waitMs);
    }
  }
}

function publishRawEvent(event, correlationId) {
  if (!rabbit.channel) {
    throw new Error("rabbitmq_not_ready");
  }
  const payload = Buffer.from(JSON.stringify(event));
  rabbit.channel.publish(cfg.rawExchange, cfg.rawRoutingKey, payload, {
    contentType: "application/json",
    deliveryMode: 2,
    messageId: event.eventId,
    correlationId
  });
}

function requireAuth(req, res, next) {
  const header = req.get("authorization") || "";
  if (!header.toLowerCase().startsWith("bearer ")) {
    return res.status(401).json({ error: "missing_token" });
  }
  const token = header.slice(7).trim();
  try {
    const payload = jwt.verify(token, cfg.jwtSigningKey, {
      issuer: cfg.jwtIssuer,
      audience: cfg.jwtAudience
    });
    req.user = payload;
    return next();
  } catch (err) {
    return res.status(401).json({ error: "invalid_token" });
  }
}

function loadOpenApiSpec() {
  const fallback = {
    openapi: "3.0.3",
    info: { title: "Order API", version: "0.1.0" },
    paths: {}
  };
  try {
    const raw = fs.readFileSync(cfg.openApiPath, "utf8");
    return { raw, spec: yaml.parse(raw) };
  } catch (err) {
    log("warn", "OpenAPI spec not found, using fallback", { error: err.message });
    return { raw: yaml.stringify(fallback), spec: fallback };
  }
}

const app = express();
app.use(express.json({ limit: "1mb" }));

const { raw: openApiRaw, spec: openApiSpec } = loadOpenApiSpec();

app.get("/health", (req, res) => {
  res.json({ status: "ok", rabbitmq: rabbit.ready ? "connected" : "disconnected" });
});

app.get("/openapi.yaml", (req, res) => {
  res.type("text/yaml").send(openApiRaw);
});

app.use("/docs", swaggerUi.serve, swaggerUi.setup(openApiSpec));

app.post("/orders", requireAuth, (req, res) => {
  if (!rabbit.ready) {
    return res.status(503).json({ error: "rabbitmq_not_ready" });
  }

  const body = req.body || {};
  const orderId = randomUUID();
  const correlationId = req.get("x-correlation-id") || randomUUID();
  const createdAt = new Date().toISOString();

  const order = {
    id: orderId,
    status: "CREATED",
    items: body.items || [],
    total: body.total || 0,
    currency: body.currency || "USD",
    createdAt
  };

  const event = {
    eventId: randomUUID(),
    type: "OrderCreated",
    occurredAt: createdAt,
    correlationId,
    orderId,
    order
  };

  try {
    publishRawEvent(event, correlationId);
    orders.set(orderId, order);
    return res.status(201).json({
      order,
      correlationId,
      eventId: event.eventId
    });
  } catch (err) {
    log("error", "failed to publish event", { error: err.message });
    return res.status(503).json({ error: "publish_failed" });
  }
});

app.get("/orders/:id", requireAuth, (req, res) => {
  const order = orders.get(req.params.id);
  if (!order) {
    return res.status(404).json({ error: "not_found" });
  }
  return res.json(order);
});

app.use((err, req, res, next) => {
  if (err && err.type === "entity.parse.failed") {
    return res.status(400).json({ error: "invalid_json" });
  }
  return next(err);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

connectRabbit()
  .then(() => {
    app.listen(cfg.port, () => {
      log("info", "order-api listening", {
        port: cfg.port,
        openApiPath: cfg.openApiPath,
        baseDir: __dirname
      });
    });
  })
  .catch((err) => {
    log("error", "failed to start order-api", { error: err.message });
    process.exit(1);
  });
