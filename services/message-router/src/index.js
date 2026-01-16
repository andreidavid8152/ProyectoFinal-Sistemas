import amqp from "amqplib";
import { randomUUID } from "crypto";
import { setTimeout as delay } from "timers/promises";

const cfg = {
  host: process.env.RABBITMQ_HOST || "localhost",
  port: process.env.RABBITMQ_PORT || "5672",
  user: process.env.RABBITMQ_USER || "integrahub",
  pass: process.env.RABBITMQ_PASS || "change_me",
  vhost: process.env.RABBITMQ_VHOST || "/",
  rawQueue: process.env.RAW_QUEUE || "q.events.raw",
  rawExchange: process.env.RAW_EXCHANGE || "integrahub.events.raw",
  eventsExchange: process.env.EVENTS_EXCHANGE || "integrahub.events",
  commandsExchange: process.env.COMMANDS_EXCHANGE || "integrahub.commands",
  dlxExchange: process.env.DLX_EXCHANGE || "integrahub.dlx",
  retryRoutingKey: process.env.RETRY_ROUTING_KEY || "retry.events.raw",
  dlqRoutingKey: process.env.DLQ_ROUTING_KEY || "dlq.events.raw",
  maxRetries: parseInt(process.env.MAX_RETRIES || "3", 10),
  prefetch: parseInt(process.env.PREFETCH || "1", 10),
  idempotencyTtlMs: parseInt(process.env.IDEMPOTENCY_TTL_MS || "86400000", 10)
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

function buildPaymentCommand(canonical, payload) {
  const amount =
    payload.amount ||
    payload.total ||
    (payload.order && payload.order.total) ||
    0;
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
  log("info", "message-router starting", {
    rawQueue: cfg.rawQueue,
    rawExchange: cfg.rawExchange,
    eventsExchange: cfg.eventsExchange,
    commandsExchange: cfg.commandsExchange
  });

  const connection = await connectWithRetry();
  connection.on("close", () => {
    log("error", "RabbitMQ connection closed");
    process.exit(1);
  });
  connection.on("error", (err) => {
    log("error", "RabbitMQ connection error", { error: err.message });
  });

  const channel = await connection.createChannel();
  await channel.prefetch(cfg.prefetch);
  await channel.checkQueue(cfg.rawQueue);

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
      log("info", "processed", { type, eventId: canonical.eventId });
    } catch (err) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": err.message };

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
