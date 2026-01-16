import amqp from "amqplib";
import http from "http";
import { setTimeout as delay } from "timers/promises";

const cfg = {
  rabbitHost: process.env.RABBITMQ_HOST || "localhost",
  rabbitPort: process.env.RABBITMQ_PORT || "5672",
  rabbitUser: process.env.RABBITMQ_USER || "integrahub",
  rabbitPass: process.env.RABBITMQ_PASS || "admin",
  rabbitVhost: process.env.RABBITMQ_VHOST || "/",
  eventQueue: process.env.NOTIFICATION_QUEUE || "q.notifications",
  eventRoutingKey: process.env.NOTIFICATION_ROUTING_KEY || "order.#",
  eventsExchange: process.env.EVENTS_EXCHANGE || "integrahub.events",
  dlxExchange: process.env.DLX_EXCHANGE || "integrahub.dlx",
  retryRoutingKey: process.env.RETRY_ROUTING_KEY || "retry.notifications",
  mainRoutingKey: process.env.MAIN_ROUTING_KEY || "main.notifications",
  dlqRoutingKey: process.env.DLQ_ROUTING_KEY || "dlq.notifications",
  retryTtlMs: parseInt(process.env.RETRY_TTL_MS || "5000", 10),
  maxRetries: parseInt(process.env.MAX_RETRIES || "3", 10),
  prefetch: parseInt(process.env.PREFETCH || "1", 10),
  statusPort: parseInt(process.env.STATUS_PORT || "8097", 10),
  discordWebhookUrl: process.env.DISCORD_WEBHOOK_URL || "",
  discordWebhookUsername: process.env.DISCORD_WEBHOOK_USERNAME || "",
  discordWebhookAvatarUrl: process.env.DISCORD_WEBHOOK_AVATAR_URL || "",
  discordWebhookTimeoutMs: parseInt(process.env.DISCORD_WEBHOOK_TIMEOUT_MS || "4000", 10),
  discordCircuitFailureThreshold: parseInt(
    process.env.DISCORD_CB_FAILURE_THRESHOLD || "3",
    10
  ),
  discordCircuitResetMs: parseInt(
    process.env.DISCORD_CB_RESET_TIMEOUT_MS || "20000",
    10
  )
};

const status = {
  startedAt: new Date().toISOString(),
  rabbitmq: "starting",
  discord: cfg.discordWebhookUrl ? "starting" : "disabled",
  discordCircuitState: cfg.discordWebhookUrl ? "closed" : "disabled",
  discordCircuitFailures: 0,
  discordCircuitOpenedAt: null,
  lastDiscordError: null,
  lastEventType: null,
  lastCorrelationId: null,
  lastOrderId: null,
  totalNotifications: 0,
  lastError: null
};

const discordCircuit = {
  state: cfg.discordWebhookUrl ? "closed" : "disabled",
  failureCount: 0,
  openedAt: null,
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

function formatDiscordMessage({ type, orderId, correlationId }) {
  const safeType = type || "order.unknown";
  const safeOrderId = orderId || "n/a";
  const safeCorrelationId = correlationId || "n/a";
  return `Order event: ${safeType}\nOrder ID: ${safeOrderId}\nCorrelation: ${safeCorrelationId}`;
}

function openDiscordCircuit(reason) {
  discordCircuit.state = "open";
  discordCircuit.openedAt = Date.now();
  discordCircuit.lastError = reason || "discord_failure";
  status.discord = "open";
  status.discordCircuitState = "open";
  status.discordCircuitOpenedAt = new Date(discordCircuit.openedAt).toISOString();
  status.discordCircuitFailures = discordCircuit.failureCount;
  status.lastDiscordError = discordCircuit.lastError;
}

function markDiscordSuccess() {
  discordCircuit.state = "closed";
  discordCircuit.failureCount = 0;
  discordCircuit.openedAt = null;
  discordCircuit.lastError = null;
  status.discord = "ok";
  status.discordCircuitState = "closed";
  status.discordCircuitOpenedAt = null;
  status.discordCircuitFailures = 0;
  status.lastDiscordError = null;
}

function markDiscordFailure(errorMessage) {
  discordCircuit.failureCount += 1;
  discordCircuit.lastError = errorMessage;
  status.discord = "error";
  status.discordCircuitState = discordCircuit.state;
  status.discordCircuitFailures = discordCircuit.failureCount;
  status.lastDiscordError = errorMessage;

  if (
    discordCircuit.state === "half-open" ||
    discordCircuit.failureCount >= cfg.discordCircuitFailureThreshold
  ) {
    openDiscordCircuit(errorMessage);
  }
}

function ensureDiscordCircuitReady() {
  if (discordCircuit.state === "disabled") {
    status.discord = "disabled";
    status.discordCircuitState = "disabled";
    status.lastDiscordError = null;
    return;
  }

  if (discordCircuit.state !== "open") {
    return;
  }

  const elapsed = Date.now() - (discordCircuit.openedAt || 0);
  if (elapsed < cfg.discordCircuitResetMs) {
    status.discord = "open";
    status.discordCircuitState = "open";
    status.discordCircuitOpenedAt = new Date(discordCircuit.openedAt).toISOString();
    status.lastDiscordError = discordCircuit.lastError || "discord_circuit_open";
    throw new Error("discord_circuit_open");
  }

  discordCircuit.state = "half-open";
  status.discordCircuitState = "half-open";
}

async function sendDiscordNotification(event) {
  if (!cfg.discordWebhookUrl) {
    status.discord = "disabled";
    status.discordCircuitState = "disabled";
    status.discordCircuitFailures = 0;
    status.discordCircuitOpenedAt = null;
    status.lastDiscordError = null;
    return;
  }

  ensureDiscordCircuitReady();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), cfg.discordWebhookTimeoutMs);
  const payload = { content: formatDiscordMessage(event) };

  if (cfg.discordWebhookUsername) {
    payload.username = cfg.discordWebhookUsername;
  }
  if (cfg.discordWebhookAvatarUrl) {
    payload.avatar_url = cfg.discordWebhookAvatarUrl;
  }

  try {
    const response = await fetch(cfg.discordWebhookUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal
    });

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      const detail = body ? `: ${body}` : "";
      throw new Error(`discord webhook error ${response.status}${detail}`);
    }

    markDiscordSuccess();
  } catch (err) {
    markDiscordFailure(err.message);
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

function startStatusServer() {
  const server = http.createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          status: "ok",
          rabbitmq: status.rabbitmq,
          discord: status.discord,
          discordCircuitState: status.discordCircuitState,
          discordCircuitFailures: status.discordCircuitFailures,
          discordCircuitOpenedAt: status.discordCircuitOpenedAt,
          startedAt: status.startedAt,
          lastEventType: status.lastEventType,
          lastCorrelationId: status.lastCorrelationId,
          lastOrderId: status.lastOrderId,
          totalNotifications: status.totalNotifications,
          lastDiscordError: status.lastDiscordError,
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

async function ensureTopology(channel) {
  await channel.assertExchange(cfg.eventsExchange, "topic", { durable: true });
  await channel.assertExchange(cfg.dlxExchange, "direct", { durable: true });

  await channel.assertQueue(cfg.eventQueue, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": cfg.dlxExchange,
      "x-dead-letter-routing-key": cfg.retryRoutingKey
    }
  });
  await channel.bindQueue(cfg.eventQueue, cfg.eventsExchange, cfg.eventRoutingKey);
  await channel.bindQueue(cfg.eventQueue, cfg.dlxExchange, cfg.mainRoutingKey);

  await channel.assertQueue(`${cfg.eventQueue}.retry`, {
    durable: true,
    arguments: {
      "x-message-ttl": cfg.retryTtlMs,
      "x-dead-letter-exchange": cfg.dlxExchange,
      "x-dead-letter-routing-key": cfg.mainRoutingKey
    }
  });
  await channel.bindQueue(`${cfg.eventQueue}.retry`, cfg.dlxExchange, cfg.retryRoutingKey);

  await channel.assertQueue(`${cfg.eventQueue}.dlq`, { durable: true });
  await channel.bindQueue(`${cfg.eventQueue}.dlq`, cfg.dlxExchange, cfg.dlqRoutingKey);
}

async function start() {
  startStatusServer();
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

  await channel.consume(cfg.eventQueue, async (msg) => {
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

    try {
      const type = payload.type || payload.eventType || "";
      const correlationId = payload.correlationId || msg.properties.correlationId || "";
      const orderId = payload.orderId || payload.id || "";

      log("info", "notify operations", { type, orderId, correlationId });
      log("info", "notify customer", { type, orderId, correlationId });
      await sendDiscordNotification({ type, orderId, correlationId });

      status.lastEventType = type;
      status.lastCorrelationId = correlationId;
      status.lastOrderId = orderId;
      status.totalNotifications += 1;
      status.lastError = null;

      channel.ack(msg);
    } catch (err) {
      const attempts = getAttempts(msg);
      const headers = { "x-attempts": attempts + 1, "x-error": err.message };
      status.lastError = err.message;

      if (attempts >= cfg.maxRetries) {
        republish(channel, cfg.dlqRoutingKey, msg, headers);
        channel.ack(msg);
        return;
      }

      republish(channel, cfg.retryRoutingKey, msg, headers);
      channel.ack(msg);
    }
  });
}

start().catch((err) => {
  log("error", "fatal error", { error: err.message });
  process.exit(1);
});
