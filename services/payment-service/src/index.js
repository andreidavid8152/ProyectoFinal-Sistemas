import amqp from "amqplib";
import http from "http";
import { Pool } from "pg";
import { randomUUID } from "crypto";
import { setTimeout as delay } from "timers/promises";

const cfg = {
  rabbitHost: process.env.RABBITMQ_HOST || "localhost",
  rabbitPort: process.env.RABBITMQ_PORT || "5672",
  rabbitUser: process.env.RABBITMQ_USER || "integrahub",
  rabbitPass: process.env.RABBITMQ_PASS || "admin",
  rabbitVhost: process.env.RABBITMQ_VHOST || "/",
  commandQueue: process.env.PAYMENT_QUEUE || "q.payment.process",
  commandRoutingKey: process.env.PAYMENT_ROUTING_KEY || "payment.process",
  rawExchange: process.env.RAW_EXCHANGE || "integrahub.events.raw",
  commandsExchange: process.env.COMMANDS_EXCHANGE || "integrahub.commands",
  dlxExchange: process.env.DLX_EXCHANGE || "integrahub.dlx",
  retryRoutingKey: process.env.RETRY_ROUTING_KEY || "retry.payment.process",
  mainRoutingKey: process.env.MAIN_ROUTING_KEY || "main.payment.process",
  dlqRoutingKey: process.env.DLQ_ROUTING_KEY || "dlq.payment.process",
  retryTtlMs: parseInt(process.env.RETRY_TTL_MS || "5000", 10),
  maxRetries: parseInt(process.env.MAX_RETRIES || "3", 10),
  prefetch: parseInt(process.env.PREFETCH || "1", 10),
  statusPort: parseInt(process.env.STATUS_PORT || "8096", 10),
  failRate: parseFloat(process.env.PAYMENT_FAIL_RATE || "0"),
  forceFail: parseBool(process.env.PAYMENT_FORCE_FAIL, false),
  simulateError: parseBool(process.env.PAYMENT_SIMULATE_ERROR, false),
  dbHost: process.env.DB_HOST || "localhost",
  dbPort: parseInt(process.env.DB_PORT || "5432", 10),
  dbUser: process.env.DB_USER || "integrahub",
  dbPassword: process.env.DB_PASSWORD || "admin",
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

function parseBool(value, defaultValue) {
  if (value === undefined || value === null) return defaultValue;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(normalized)) return true;
  if (["0", "false", "no", "n"].includes(normalized)) return false;
  return defaultValue;
}

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
          lastOutcome: status.lastOutcome,
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

async function checkDuplicate(commandId) {
  if (!commandId) return false;
  const result = await pool.query(
    "SELECT status FROM payment_transactions WHERE command_id = $1",
    [commandId]
  );
  return result.rowCount > 0;
}

async function recordTransaction(commandId, orderId, statusValue) {
  if (!commandId) return;
  await pool.query(
    "INSERT INTO payment_transactions (command_id, order_id, status) VALUES ($1, $2, $3) " +
      "ON CONFLICT (command_id) DO UPDATE SET status = EXCLUDED.status",
    [commandId, orderId || "", statusValue]
  );
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

function shouldFail(amount) {
  if (cfg.forceFail) return true;
  if (amount <= 0) return true;
  if (cfg.failRate > 0 && Math.random() < cfg.failRate) return true;
  return false;
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
    const amount = parseFloat(payload.amount ?? payload.total ?? 0);
    const currency = payload.currency || "USD";

    try {
      const isDuplicate = await checkDuplicate(commandId);
      if (isDuplicate) {
        channel.ack(msg);
        log("info", "duplicate ignored", { commandId });
        return;
      }

      if (cfg.simulateError) {
        throw new Error("simulated_error");
      }

      const failed = shouldFail(amount);
      const event = {
        eventId: randomUUID(),
        type: failed ? "PaymentFailed" : "PaymentApproved",
        occurredAt: new Date().toISOString(),
        correlationId,
        orderId,
        amount,
        currency,
        reason: failed ? "payment_declined" : "approved",
        sourceCommandId: commandId
      };

      publishRawEvent(channel, event, correlationId);
      await recordTransaction(commandId, orderId, failed ? "failed" : "approved");

      channel.ack(msg);
      status.lastProcessedAt = new Date().toISOString();
      status.lastOutcome = failed ? "failed" : "approved";
      status.lastError = null;
      log("info", "payment processed", { orderId, commandId, result: event.type });
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
