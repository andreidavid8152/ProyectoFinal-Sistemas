import express from "express";
import path from "path";
import { fileURLToPath } from "url";

const cfg = {
  port: parseInt(
    process.env.DEMO_PORTAL_PORT || process.env.PORT || "8081",
    10
  ),
  orderApiUrl: process.env.ORDER_API_URL || "http://order-api:8080",
  authApiUrl: process.env.AUTH_API_URL || "http://auth-service:8082",
  messageRouterUrl: process.env.MESSAGE_ROUTER_URL || "",
  fileIngestUrl: process.env.FILE_INGEST_URL || "",
  analyticsUrl: process.env.ANALYTICS_URL || "",
  inventoryUrl: process.env.INVENTORY_URL || "",
  paymentUrl: process.env.PAYMENT_URL || "",
  notificationUrl: process.env.NOTIFICATION_URL || "",
  statusTimeoutMs: parseInt(process.env.STATUS_TIMEOUT_MS || "1500", 10),
  rabbitmqUiPublicUrl: process.env.RABBITMQ_UI_PUBLIC_URL || "",
  swaggerPublicUrl: process.env.SWAGGER_PUBLIC_URL || "",
  authPublicUrl: process.env.AUTH_PUBLIC_URL || ""
};

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, "..", "public");

app.use(express.static(publicDir));

function log(level, message, extra) {
  const base = `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}`;
  if (extra) {
    console.log(base, JSON.stringify(extra));
  } else {
    console.log(base);
  }
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function readJsonSafe(response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

async function checkTarget(name, url) {
  if (!url) {
    return { name, status: "disabled" };
  }
  const started = Date.now();
  try {
    const response = await fetchWithTimeout(
      url,
      { headers: { accept: "application/json" } },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    return {
      name,
      status: response.ok ? "up" : "down",
      code: response.status,
      ms: Date.now() - started,
      detail: data
    };
  } catch (err) {
    return {
      name,
      status: "down",
      error: err.message,
      ms: Date.now() - started
    };
  }
}

app.get("/api/config", (req, res) => {
  res.json({
    orderApiUrl: cfg.orderApiUrl,
    authApiUrl: cfg.authApiUrl,
    links: {
      rabbitmqUi: cfg.rabbitmqUiPublicUrl,
      swaggerUi: cfg.swaggerPublicUrl,
      authToken: cfg.authPublicUrl
    }
  });
});

app.get("/api/status", async (req, res) => {
  const targets = [
    { name: "order-api", url: `${cfg.orderApiUrl}/health` },
    { name: "auth-service", url: `${cfg.authApiUrl}/health` },
    { name: "message-router", url: cfg.messageRouterUrl ? `${cfg.messageRouterUrl}/health` : "" },
    { name: "file-ingest", url: cfg.fileIngestUrl ? `${cfg.fileIngestUrl}/health` : "" },
    { name: "analytics-service", url: cfg.analyticsUrl ? `${cfg.analyticsUrl}/health` : "" },
    { name: "inventory-service", url: cfg.inventoryUrl ? `${cfg.inventoryUrl}/health` : "" },
    { name: "payment-service", url: cfg.paymentUrl ? `${cfg.paymentUrl}/health` : "" },
    { name: "notification-service", url: cfg.notificationUrl ? `${cfg.notificationUrl}/health` : "" }
  ];

  const services = await Promise.all(
    targets.map((target) => checkTarget(target.name, target.url))
  );

  res.json({
    updatedAt: new Date().toISOString(),
    services
  });
});

app.get("/api/catalog", async (req, res) => {
  if (!cfg.inventoryUrl) {
    res.status(503).json({ error: "inventory_unavailable" });
    return;
  }
  const sku = (req.query.sku || "").toString().trim();
  const target = sku
    ? `${cfg.inventoryUrl}/inventory?sku=${encodeURIComponent(sku)}`
    : `${cfg.inventoryUrl}/inventory`;
  try {
    const response = await fetchWithTimeout(
      target,
      { headers: { accept: "application/json" } },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    res.status(response.status).json(data || {});
  } catch (err) {
    res.status(502).json({ error: "inventory_unavailable" });
  }
});

app.get("/api/payment-control", async (req, res) => {
  if (!cfg.paymentUrl) {
    res.status(503).json({ error: "payment_unavailable" });
    return;
  }
  try {
    const response = await fetchWithTimeout(
      `${cfg.paymentUrl}/control`,
      { headers: { accept: "application/json" } },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    res.status(response.status).json(data || {});
  } catch (err) {
    res.status(502).json({ error: "payment_unavailable" });
  }
});

app.post("/api/payment-control", async (req, res) => {
  if (!cfg.paymentUrl) {
    res.status(503).json({ error: "payment_unavailable" });
    return;
  }
  try {
    const response = await fetchWithTimeout(
      `${cfg.paymentUrl}/control`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(req.body || {})
      },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    res.status(response.status).json(data || {});
  } catch (err) {
    res.status(502).json({ error: "payment_unavailable" });
  }
});

app.post("/api/token", async (req, res) => {
  try {
    const response = await fetchWithTimeout(
      `${cfg.authApiUrl}/token`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(req.body || {})
      },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    res.status(response.status).json(data || {});
  } catch (err) {
    res.status(502).json({ error: "auth_unavailable" });
  }
});

app.post("/api/orders", async (req, res) => {
  try {
    const correlationId = req.get("x-correlation-id") || "";
    const response = await fetchWithTimeout(
      `${cfg.orderApiUrl}/orders`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: req.get("authorization") || "",
          "x-correlation-id": correlationId
        },
        body: JSON.stringify(req.body || {})
      },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    res.status(response.status).json(data || {});
  } catch (err) {
    res.status(502).json({ error: "order_api_unavailable" });
  }
});

app.get("/api/orders/:id", async (req, res) => {
  try {
    const response = await fetchWithTimeout(
      `${cfg.orderApiUrl}/orders/${req.params.id}`,
      {
        headers: {
          authorization: req.get("authorization") || ""
        }
      },
      cfg.statusTimeoutMs
    );
    const data = await readJsonSafe(response);
    res.status(response.status).json(data || {});
  } catch (err) {
    res.status(502).json({ error: "order_api_unavailable" });
  }
});

app.get("*", (req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});

app.listen(cfg.port, () => {
  log("info", "demo-portal listening", { port: cfg.port });
});
