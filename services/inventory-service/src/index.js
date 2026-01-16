import amqp from "amqplib";
import http from "http";
import { Pool } from "pg";
import { randomUUID } from "crypto";
import { setTimeout as delay } from "timers/promises";

const cfg = {
  rabbitHost: process.env.RABBITMQ_HOST || "localhost",
  rabbitPort: process.env.RABBITMQ_PORT || "5672",
  rabbitUser: process.env.RABBITMQ_USER || "integrahub",
  rabbitPass: process.env.RABBITMQ_PASS || "change_me",
  rabbitVhost: process.env.RABBITMQ_VHOST || "/",
  commandQueue: process.env.INVENTORY_QUEUE || "q.inventory.reserve",
  commandRoutingKey: process.env.INVENTORY_ROUTING_KEY || "inventory.reserve",
  rawExchange: process.env.RAW_EXCHANGE || "integrahub.events.raw",
  commandsExchange: process.env.COMMANDS_EXCHANGE || "integrahub.commands",
  dlxExchange: process.env.DLX_EXCHANGE || "integrahub.dlx",
  retryRoutingKey: process.env.RETRY_ROUTING_KEY || "retry.inventory.reserve",
  mainRoutingKey: process.env.MAIN_ROUTING_KEY || "main.inventory.reserve",
  dlqRoutingKey: process.env.DLQ_ROUTING_KEY || "dlq.inventory.reserve",
  retryTtlMs: parseInt(process.env.RETRY_TTL_MS || "5000", 10),
  maxRetries: parseInt(process.env.MAX_RETRIES || "3", 10),
  prefetch: parseInt(process.env.PREFETCH || "1", 10),
  statusPort: parseInt(process.env.STATUS_PORT || "8095", 10),
  dbHost: process.env.DB_HOST || "localhost",
  dbPort: parseInt(process.env.DB_PORT || "5432", 10),
  dbUser: process.env.DB_USER || "integrahub",
  dbPassword: process.env.DB_PASSWORD || "change_me",
  dbName: process.env.DB_NAME || "integrahub",
  dbStatementTimeoutMs: parseInt(
    process.env.DB_STATEMENT_TIMEOUT_MS || "4000",
    10
  ),
  dbConnectionTimeoutMs: parseInt(
    process.env.DB_CONNECTION_TIMEOUT_MS || "5000",
    10
  )
};

const pool = new Pool({
  host: cfg.dbHost,
  port: cfg.dbPort,
  user: cfg.dbUser,
  password: cfg.dbPassword,
  database: cfg.dbName,
  statement_timeout: cfg.dbStatementTimeoutMs,
  connectionTimeoutMillis: cfg.dbConnectionTimeoutMs
});

const status = {
  startedAt: new Date().toISOString(),
  rabbitmq: "starting",
  lastProcessedAt: null,
  lastOutcome: null,
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

function buildAmqpUrl() {
  const vhostRaw = cfg.rabbitVhost || "/";
  const vhost = vhostRaw === "/" ? "%2F" : encodeURIComponent(vhostRaw.replace(/^\//, ""));
  const user = encodeURIComponent(cfg.rabbitUser);
  const pass = encodeURIComponent(cfg.rabbitPass);
  return `amqp://${user}:${pass}@${cfg.rabbitHost}:${cfg.rabbitPort}/${vhost}`;
}

function sendJson(res, statusCode, body) {
  res.writeHead(statusCode, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

async function handleInventoryRequest(url, res) {
  const skuRaw = url.searchParams.get("sku");
  if (skuRaw) {
    const sku = skuRaw.trim();
    if (!sku) {
      sendJson(res, 400, { error: "missing_sku" });
      return;
    }
    const result = await pool.query(
      "SELECT sku, quantity, price, updated_at FROM inventory_items WHERE sku = $1",
      [sku]
    );
    if (result.rowCount === 0) {
      sendJson(res, 404, { error: "not_found" });
      return;
    }
    const row = result.rows[0];
    sendJson(res, 200, {
      sku: row.sku,
      quantity: Number(row.quantity),
      price: Number(row.price),
      updatedAt: row.updated_at
    });
    return;
  }

  const limitRaw = parseInt(url.searchParams.get("limit") || "200", 10);
  const limit = Number.isFinite(limitRaw) ? Math.min(limitRaw, 500) : 200;
  const result = await pool.query(
    "SELECT sku, quantity, price, updated_at FROM inventory_items ORDER BY sku LIMIT $1",
    [limit]
  );
  sendJson(res, 200, {
    items: result.rows.map((row) => ({
      sku: row.sku,
      quantity: Number(row.quantity),
      price: Number(row.price),
      updatedAt: row.updated_at
    }))
  });
}

function startStatusServer() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", "http://localhost");
    if (url.pathname === "/health") {
      sendJson(res, 200, {
        status: "ok",
        rabbitmq: status.rabbitmq,
        startedAt: status.startedAt,
        lastProcessedAt: status.lastProcessedAt,
        lastOutcome: status.lastOutcome,
        lastError: status.lastError
      });
      return;
    }
    if (url.pathname === "/inventory") {
      handleInventoryRequest(url, res).catch((err) => {
        status.lastError = err.message;
        sendJson(res, 500, { error: "inventory_lookup_failed" });
      });
      return;
    }
    res.writeHead(404);
    res.end();
  });

  server.listen(cfg.statusPort, () => {
    log("info", "status server listening", { port: cfg.statusPort });
  });
}

async function connectPostgres() {
  let attempt = 0;
  while (true) {
    try {
      await pool.query("SELECT 1");
      log("info", "postgres connection ready");
      return;
    } catch (err) {
      attempt += 1;
      const waitMs = Math.min(1000 * attempt, 10000);
      log("warn", "postgres connection failed, retrying", {
        attempt,
        waitMs,
        error: err.message
      });
      await delay(waitMs);
    }
  }
}

async function connectRabbit() {
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

function getAttempts(msg) {
  const headers = msg.properties.headers || {};
  const raw = headers["x-attempts"];
  const parsed = parseInt(raw || "0", 10);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function republish(channel, routingKey, msg, extraHeaders) {
  const headers = { ...(msg.properties.headers || {}), ...extraHeaders };
  channel.publish(cfg.dlxExchange, routingKey, msg.content, {
    contentType: msg.properties.contentType || "application/json",
    contentEncoding: msg.properties.contentEncoding,
    correlationId: msg.properties.correlationId,
    messageId: msg.properties.messageId,
    deliveryMode: 2,
    headers
  });
}

function publishRawEvent(channel, event, correlationId) {
  const payload = Buffer.from(JSON.stringify(event));
  channel.publish(cfg.rawExchange, "events.raw", payload, {
    contentType: "application/json",
    deliveryMode: 2,
    messageId: event.eventId,
    correlationId
  });
}

function normalizeItems(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return { error: "missing_items" };
  }
  const normalized = [];
  for (const item of items) {
    const sku = item.sku ? String(item.sku).trim() : "";
    const qtyRaw = item.qty ?? item.quantity ?? item.count;
    const qty = parseInt(qtyRaw, 10);
    if (!sku) return { error: "missing_sku" };
    if (!Number.isFinite(qty) || qty <= 0) return { error: "invalid_quantity" };
    normalized.push({ sku, qty });
  }
  return { items: normalized };
}

async function checkDuplicate(commandId) {
  if (!commandId) return false;
  const result = await pool.query(
    "SELECT status FROM inventory_reservations WHERE command_id = $1",
    [commandId]
  );
  return result.rowCount > 0;
}

async function recordReservation(commandId, orderId, statusValue, reason) {
  if (!commandId) return;
  await pool.query(
    "INSERT INTO inventory_reservations (command_id, order_id, status, reason) VALUES ($1, $2, $3, $4) " +
      "ON CONFLICT (command_id) DO UPDATE SET status = EXCLUDED.status, reason = EXCLUDED.reason",
    [commandId, orderId || "", statusValue, reason || null]
  );
}

async function reserveInventory(items) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const reserved = [];

    for (const item of items) {
      const row = await client.query(
        "SELECT quantity, price FROM inventory_items WHERE sku = $1 FOR UPDATE",
        [item.sku]
      );
      if (row.rowCount === 0) {
        await client.query("ROLLBACK");
        return { ok: false, reason: "missing_sku", sku: item.sku };
      }
      const available = parseInt(row.rows[0].quantity, 10);
      if (available < item.qty) {
        await client.query("ROLLBACK");
        return { ok: false, reason: "insufficient_stock", sku: item.sku };
      }
      reserved.push({
        sku: item.sku,
        qty: item.qty,
        price: Number(row.rows[0].price || 0)
      });
    }

    for (const item of reserved) {
      await client.query(
        "UPDATE inventory_items SET quantity = quantity - $1, updated_at = now() WHERE sku = $2",
        [item.qty, item.sku]
      );
    }

    await client.query("COMMIT");
    return { ok: true, items: reserved };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function ensureTopology(channel) {
  await channel.assertExchange(cfg.rawExchange, "topic", { durable: true });
  await channel.assertExchange(cfg.commandsExchange, "direct", { durable: true });
  await channel.assertExchange(cfg.dlxExchange, "direct", { durable: true });

  await channel.assertQueue(cfg.commandQueue, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": cfg.dlxExchange,
      "x-dead-letter-routing-key": cfg.retryRoutingKey
    }
  });
  await channel.bindQueue(cfg.commandQueue, cfg.commandsExchange, cfg.commandRoutingKey);
  await channel.bindQueue(cfg.commandQueue, cfg.dlxExchange, cfg.mainRoutingKey);

  await channel.assertQueue(`${cfg.commandQueue}.retry`, {
    durable: true,
    arguments: {
      "x-message-ttl": cfg.retryTtlMs,
      "x-dead-letter-exchange": cfg.dlxExchange,
      "x-dead-letter-routing-key": cfg.mainRoutingKey
    }
  });
  await channel.bindQueue(`${cfg.commandQueue}.retry`, cfg.dlxExchange, cfg.retryRoutingKey);

  await channel.assertQueue(`${cfg.commandQueue}.dlq`, { durable: true });
  await channel.bindQueue(`${cfg.commandQueue}.dlq`, cfg.dlxExchange, cfg.dlqRoutingKey);
}

async function start() {
  startStatusServer();
  await connectPostgres();
  const connection = await connectRabbit();
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

  await channel.consume(cfg.commandQueue, async (msg) => {
    if (!msg) return;

    let payload;
    try {
      payload = JSON.parse(msg.content.toString("utf8"));
    } catch (err) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": "invalid_json" };
      if (attempts >= cfg.maxRetries) {
        republish(channel, cfg.dlqRoutingKey, msg, headers);
        channel.ack(msg);
        return;
      }
      republish(channel, cfg.retryRoutingKey, msg, headers);
      channel.ack(msg);
      return;
    }

    const commandId = payload.commandId || msg.properties.messageId || "";
    const orderId = payload.orderId || payload.id || "";
    const correlationId = payload.correlationId || msg.properties.correlationId || "";

    try {
      const isDuplicate = await checkDuplicate(commandId);
      if (isDuplicate) {
        channel.ack(msg);
        log("info", "duplicate ignored", { commandId });
        return;
      }

      const normalized = normalizeItems(payload.items);
      if (normalized.error) {
        const event = {
          eventId: randomUUID(),
          type: "InventoryRejected",
          occurredAt: new Date().toISOString(),
          correlationId,
          orderId,
          reason: normalized.error,
          sourceCommandId: commandId
        };
        publishRawEvent(channel, event, correlationId);
        await recordReservation(commandId, orderId, "rejected", normalized.error);
        channel.ack(msg);
        status.lastProcessedAt = new Date().toISOString();
        status.lastOutcome = "rejected";
        status.lastError = null;
        return;
      }

      const result = await reserveInventory(normalized.items);
      if (!result.ok) {
        const event = {
          eventId: randomUUID(),
          type: "InventoryRejected",
          occurredAt: new Date().toISOString(),
          correlationId,
          orderId,
          reason: result.reason,
          sku: result.sku || "",
          sourceCommandId: commandId
        };
        publishRawEvent(channel, event, correlationId);
        await recordReservation(commandId, orderId, "rejected", result.reason);
        channel.ack(msg);
        status.lastProcessedAt = new Date().toISOString();
        status.lastOutcome = "rejected";
        status.lastError = null;
        return;
      }

      const event = {
        eventId: randomUUID(),
        type: "InventoryReserved",
        occurredAt: new Date().toISOString(),
        correlationId,
        orderId,
        items: result.items,
        sourceCommandId: commandId
      };
      publishRawEvent(channel, event, correlationId);
      await recordReservation(commandId, orderId, "reserved", "ok");
      channel.ack(msg);
      status.lastProcessedAt = new Date().toISOString();
      status.lastOutcome = "reserved";
      status.lastError = null;
      log("info", "inventory reserved", { orderId, commandId });
    } catch (err) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": err.message };
      status.lastError = err.message;

      if (attempts >= cfg.maxRetries) {
        republish(channel, cfg.dlqRoutingKey, msg, headers);
        channel.ack(msg);
        log("error", "message moved to dlq", { commandId, error: err.message });
        return;
      }

      republish(channel, cfg.retryRoutingKey, msg, headers);
      channel.ack(msg);
      log("warn", "message sent to retry", { commandId, error: err.message });
    }
  });
}

start().catch((err) => {
  log("error", "fatal error", { error: err.message });
  process.exit(1);
});
