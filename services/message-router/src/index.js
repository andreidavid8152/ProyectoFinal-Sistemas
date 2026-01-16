import amqp from "amqplib";
import { randomUUID } from "crypto";
import http from "http";
import { setTimeout as delay } from "timers/promises";

const cfg = {
  host: process.env.RABBITMQ_HOST || "localhost",
  port: process.env.RABBITMQ_PORT || "5672",
  user: process.env.RABBITMQ_USER || "integrahub",
  pass: process.env.RABBITMQ_PASS || "admin",
  vhost: process.env.RABBITMQ_VHOST || "/",
  rawQueue: process.env.RAW_QUEUE || "q.events.raw",
  rawExchange: process.env.RAW_EXCHANGE || "integrahub.events.raw",
  rawBindingKey: process.env.RAW_BINDING_KEY || "events.raw",
  rawRetryQueue: process.env.RAW_RETRY_QUEUE || "q.events.raw.retry",
  rawDlqQueue: process.env.RAW_DLQ_QUEUE || "q.events.raw.dlq",
  eventsExchange: process.env.EVENTS_EXCHANGE || "integrahub.events",
  commandsExchange: process.env.COMMANDS_EXCHANGE || "integrahub.commands",
  dlxExchange: process.env.DLX_EXCHANGE || "integrahub.dlx",
  retryRoutingKey: process.env.RETRY_ROUTING_KEY || "retry.events.raw",
  mainRoutingKey: process.env.MAIN_ROUTING_KEY || "main.events.raw",
  dlqRoutingKey: process.env.DLQ_ROUTING_KEY || "dlq.events.raw",
  retryTtlMs: parseInt(process.env.RETRY_TTL_MS || "5000", 10),
  maxRetries: parseInt(process.env.MAX_RETRIES || "3", 10),
  prefetch: parseInt(process.env.PREFETCH || "1", 10),
  idempotencyTtlMs: parseInt(process.env.IDEMPOTENCY_TTL_MS || "86400000", 10),
  statusPort: parseInt(process.env.STATUS_PORT || "8092", 10)
};

const status = {
  startedAt: new Date().toISOString(),
  rabbitmq: "starting",
  lastProcessedAt: null,
  lastError: null
};

function log(level, message, extra) {
  const base = `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}`;
  if (extra) {
    console.log(base, JSON.stringify(extra));
  } else {
    console.log(base);
  }
}

class IdempotencyStore {
  constructor(ttlMs) {
    this.ttlMs = ttlMs;
    this.items = new Map();
  }

  has(id) {
    this.purge();
    return this.items.has(id);
  }

  add(id) {
    if (!id) return;
    this.items.set(id, Date.now() + this.ttlMs);
  }

  purge() {
    const now = Date.now();
    for (const [id, expiresAt] of this.items) {
      if (expiresAt <= now) {
        this.items.delete(id);
      }
    }
  }
}

const idempotency = new IdempotencyStore(cfg.idempotencyTtlMs);
setInterval(() => idempotency.purge(), Math.min(60000, cfg.idempotencyTtlMs)).unref();

function startStatusServer() {
  const server = http.createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          status: "ok",
          rabbitmq: status.rabbitmq,
          startedAt: status.startedAt,
          lastProcessedAt: status.lastProcessedAt,
          lastError: status.lastError
        })
      );
      return;
    }
    res.writeHead(404);
    res.end();
  });

  server.listen(cfg.statusPort, () => {
    log("info", "status server listening", { port: cfg.statusPort });
  });
}

async function ensureTopology(channel) {
  await channel.assertExchange(cfg.rawExchange, "topic", { durable: true });
  await channel.assertExchange(cfg.eventsExchange, "topic", { durable: true });
  await channel.assertExchange(cfg.commandsExchange, "direct", { durable: true });
  await channel.assertExchange(cfg.dlxExchange, "direct", { durable: true });

  await channel.assertQueue(cfg.rawQueue, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": cfg.dlxExchange,
      "x-dead-letter-routing-key": cfg.retryRoutingKey
    }
  });
  await channel.bindQueue(cfg.rawQueue, cfg.rawExchange, cfg.rawBindingKey);
  await channel.bindQueue(cfg.rawQueue, cfg.dlxExchange, cfg.mainRoutingKey);

  await channel.assertQueue(cfg.rawRetryQueue, {
    durable: true,
    arguments: {
      "x-message-ttl": cfg.retryTtlMs,
      "x-dead-letter-exchange": cfg.dlxExchange,
      "x-dead-letter-routing-key": cfg.mainRoutingKey
    }
  });
  await channel.bindQueue(cfg.rawRetryQueue, cfg.dlxExchange, cfg.retryRoutingKey);

  await channel.assertQueue(cfg.rawDlqQueue, {
    durable: true
  });
  await channel.bindQueue(cfg.rawDlqQueue, cfg.dlxExchange, cfg.dlqRoutingKey);
}

function buildAmqpUrl() {
  const vhostRaw = cfg.vhost || "/";
  const vhost = vhostRaw === "/" ? "%2F" : encodeURIComponent(vhostRaw.replace(/^\//, ""));
  const user = encodeURIComponent(cfg.user);
  const pass = encodeURIComponent(cfg.pass);
  return `amqp://${user}:${pass}@${cfg.host}:${cfg.port}/${vhost}`;
}

function normalizeType(rawType) {
  if (!rawType) return "";
  const normalized = String(rawType).trim().toLowerCase().replace(/[\s_-]+/g, ".");
  const collapsed = normalized.replace(/\./g, "");
  const map = {
    ordercreated: "order.created",
    inventoryreserved: "inventory.reserved",
    inventoryrejected: "inventory.rejected",
    paymentapproved: "payment.approved",
    paymentfailed: "payment.failed",
    orderconfirmed: "order.confirmed",
    orderrejected: "order.rejected"
  };
  return map[collapsed] || normalized;
}

function extractOrderId(payload) {
  return payload.orderId || payload.id || (payload.order && payload.order.id) || null;
}

function extractCorrelationId(payload, msg) {
  return (
    payload.correlationId ||
    msg.properties.correlationId ||
    payload.orderId ||
    payload.id ||
    null
  );
}

function extractEventId(payload, msg) {
  return payload.eventId || msg.properties.messageId || null;
}

function buildCanonicalEvent(type, payload, msg) {
  const orderId = extractOrderId(payload);
  const correlationId = extractCorrelationId(payload, msg) || orderId || randomUUID();
  const eventId = extractEventId(payload, msg) || randomUUID();

  return {
    eventId,
    type,
    occurredAt: new Date().toISOString(),
    correlationId,
    orderId,
    data: payload
  };
}

function buildInventoryCommand(canonical, payload) {
  const items = payload.items || (payload.order && payload.order.items) || [];
  return {
    commandId: randomUUID(),
    type: "inventory.reserve",
    issuedAt: new Date().toISOString(),
    correlationId: canonical.correlationId,
    orderId: canonical.orderId,
    sourceEventId: canonical.eventId,
    items
  };
}

function calculateAmount(payload) {
  const direct =
    payload.amount ??
    payload.total ??
    (payload.order && payload.order.total);
  const directValue = parseFloat(direct);
  if (Number.isFinite(directValue) && directValue > 0) {
    return directValue;
  }

  const items = payload.items || (payload.order && payload.order.items) || [];
  if (!Array.isArray(items) || items.length === 0) {
    return 0;
  }

  let total = 0;
  for (const item of items) {
    const qty = parseFloat(item.qty ?? item.quantity ?? item.count ?? 0);
    const price = parseFloat(item.price ?? item.unitprice ?? item.unit_price ?? 0);
    if (Number.isFinite(qty) && Number.isFinite(price)) {
      total += qty * price;
    }
  }

  return Number.isFinite(total) ? total : 0;
}

function buildPaymentCommand(canonical, payload) {
  const amount = calculateAmount(payload);
  const currency =
    payload.currency ||
    (payload.order && payload.order.currency) ||
    "USD";
  return {
    commandId: randomUUID(),
    type: "payment.process",
    issuedAt: new Date().toISOString(),
    correlationId: canonical.correlationId,
    orderId: canonical.orderId,
    sourceEventId: canonical.eventId,
    amount,
    currency
  };
}

function buildOrderStatusEvent(statusType, canonical, reason) {
  return {
    eventId: randomUUID(),
    type: statusType,
    occurredAt: new Date().toISOString(),
    correlationId: canonical.correlationId,
    orderId: canonical.orderId,
    data: {
      sourceEventId: canonical.eventId,
      reason
    }
  };
}

function publishJson(channel, exchange, routingKey, body, overrides = {}) {
  const payload = Buffer.from(JSON.stringify(body));
  const headers = overrides.headers || {};
  const properties = {
    contentType: "application/json",
    deliveryMode: 2,
    messageId: overrides.messageId || body.eventId || body.commandId,
    correlationId: overrides.correlationId || body.correlationId,
    headers
  };
  channel.publish(exchange, routingKey, payload, properties);
}

function republishRaw(channel, exchange, routingKey, msg, extraHeaders) {
  const headers = { ...(msg.properties.headers || {}), ...extraHeaders };
  const properties = {
    contentType: msg.properties.contentType || "application/json",
    contentEncoding: msg.properties.contentEncoding,
    correlationId: msg.properties.correlationId,
    messageId: msg.properties.messageId,
    deliveryMode: 2,
    headers
  };
  channel.publish(exchange, routingKey, msg.content, properties);
}

function getAttempts(msg) {
  const headers = msg.properties.headers || {};
  const raw = headers["x-attempts"];
  const parsed = parseInt(raw || "0", 10);
  return Number.isNaN(parsed) ? 0 : parsed;
}

async function routeEvent(channel, canonical, payload) {
  publishJson(channel, cfg.eventsExchange, canonical.type, canonical);

  switch (canonical.type) {
    case "order.created": {
      const command = buildInventoryCommand(canonical, payload);
      publishJson(channel, cfg.commandsExchange, "inventory.reserve", command);
      break;
    }
    case "inventory.reserved": {
      const command = buildPaymentCommand(canonical, payload);
      publishJson(channel, cfg.commandsExchange, "payment.process", command);
      break;
    }
    case "inventory.rejected": {
      const status = buildOrderStatusEvent(
        "order.rejected",
        canonical,
        "inventory"
      );
      publishJson(channel, cfg.eventsExchange, status.type, status);
      break;
    }
    case "payment.approved": {
      const status = buildOrderStatusEvent(
        "order.confirmed",
        canonical,
        "payment"
      );
      publishJson(channel, cfg.eventsExchange, status.type, status);
      break;
    }
    case "payment.failed": {
      const status = buildOrderStatusEvent(
        "order.rejected",
        canonical,
        "payment"
      );
      publishJson(channel, cfg.eventsExchange, status.type, status);
      break;
    }
    default:
      throw new Error(`unsupported event type: ${canonical.type}`);
  }
}

async function connectWithRetry() {
  let attempt = 0;
  while (true) {
    try {
      const connection = await amqp.connect(buildAmqpUrl());
      return connection;
    } catch (err) {
      attempt += 1;
      const waitMs = Math.min(1000 * attempt, 10000);
      log("warn", "RabbitMQ connection failed, retrying", {
        attempt,
        waitMs,
        error: err.message
      });
      await delay(waitMs);
    }
  }
}

async function start() {
  startStatusServer();
  log("info", "message-router starting", {
    rawQueue: cfg.rawQueue,
    rawExchange: cfg.rawExchange,
    eventsExchange: cfg.eventsExchange,
    commandsExchange: cfg.commandsExchange
  });

  const connection = await connectWithRetry();
  status.rabbitmq = "connected";
  connection.on("close", () => {
    status.rabbitmq = "disconnected";
    log("error", "RabbitMQ connection closed");
    process.exit(1);
  });
  connection.on("error", (err) => {
    status.lastError = err.message;
    log("error", "RabbitMQ connection error", { error: err.message });
  });

  const channel = await connection.createChannel();
  await channel.prefetch(cfg.prefetch);
  await ensureTopology(channel);

  await channel.consume(cfg.rawQueue, async (msg) => {
    if (!msg) return;

    let payload;
    try {
      payload = JSON.parse(msg.content.toString("utf8"));
    } catch (err) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": "invalid_json" };
      if (attempts >= cfg.maxRetries) {
        republishRaw(channel, cfg.dlxExchange, cfg.dlqRoutingKey, msg, headers);
        channel.ack(msg);
        log("error", "invalid json moved to dlq", { attempts });
        return;
      }
      republishRaw(channel, cfg.dlxExchange, cfg.retryRoutingKey, msg, headers);
      channel.ack(msg);
      log("warn", "invalid json sent to retry", { attempts: attempts + 1 });
      return;
    }

    const rawType = payload.type || payload.eventType || payload.name;
    const type = normalizeType(rawType);
    if (!type) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": "missing_type" };
      if (attempts >= cfg.maxRetries) {
        republishRaw(channel, cfg.dlxExchange, cfg.dlqRoutingKey, msg, headers);
        channel.ack(msg);
        log("error", "missing type moved to dlq", { attempts });
        return;
      }
      republishRaw(channel, cfg.dlxExchange, cfg.retryRoutingKey, msg, headers);
      channel.ack(msg);
      log("warn", "missing type sent to retry", { attempts: attempts + 1 });
      return;
    }

    const eventId = extractEventId(payload, msg);
    if (eventId && idempotency.has(eventId)) {
      channel.ack(msg);
      log("info", "duplicate ignored", { eventId, type });
      return;
    }

    try {
      const canonical = buildCanonicalEvent(type, payload, msg);
      await routeEvent(channel, canonical, payload);
      idempotency.add(canonical.eventId);
      channel.ack(msg);
      status.lastProcessedAt = new Date().toISOString();
      status.lastError = null;
      log("info", "processed", { type, eventId: canonical.eventId });
    } catch (err) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": err.message };
      status.lastError = err.message;

      if (attempts >= cfg.maxRetries) {
        republishRaw(channel, cfg.dlxExchange, cfg.dlqRoutingKey, msg, headers);
        channel.ack(msg);
        log("error", "processing moved to dlq", {
          attempts,
          error: err.message
        });
        return;
      }

      // Ack and republish to retry so the delay queue controls backoff.
      republishRaw(channel, cfg.dlxExchange, cfg.retryRoutingKey, msg, headers);
      channel.ack(msg);
      log("warn", "processing sent to retry", {
        attempts: attempts + 1,
        error: err.message
      });
    }
  });
}

start().catch((err) => {
  log("error", "fatal start error", { error: err.message });
  process.exit(1);
});
