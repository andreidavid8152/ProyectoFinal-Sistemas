import http from "http";
import { Pool } from "pg";
import { setTimeout as delay } from "timers/promises";

const cfg = {
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
  ),
  etlIntervalMs: parseInt(process.env.ETL_INTERVAL_MS || "15000", 10),
  statusPort: parseInt(process.env.STATUS_PORT || "8094", 10)
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

let isRunning = false;
const status = {
  startedAt: new Date().toISOString(),
  lastRunAt: null,
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

async function connectWithRetry() {
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

function startStatusServer() {
  const server = http.createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          status: "ok",
          startedAt: status.startedAt,
          lastRunAt: status.lastRunAt,
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

async function runEtl() {
  if (isRunning) return;
  isRunning = true;
  status.lastRunAt = new Date().toISOString();

  try {
    const result = await pool.query(
      "SELECT COUNT(*) AS total_skus, " +
        "COALESCE(SUM(quantity), 0) AS total_quantity, " +
        "COALESCE(SUM(quantity * price), 0) AS total_value " +
        "FROM inventory_items"
    );
    const row = result.rows[0] || {};
    const totalSkus = Number(row.total_skus || 0);
    const totalQuantity = Number(row.total_quantity || 0);
    const totalValue = Number(row.total_value || 0);

    await pool.query(
      "INSERT INTO analytics.inventory_summary (run_at, total_skus, total_quantity, total_value) VALUES (now(), $1, $2, $3)",
      [totalSkus, totalQuantity, totalValue]
    );

    status.lastError = null;
    log("info", "etl summary stored", {
      totalSkus,
      totalQuantity,
      totalValue
    });
  } catch (err) {
    status.lastError = err.message;
    log("error", "etl run failed", { error: err.message });
  } finally {
    isRunning = false;
  }
}

async function start() {
  await connectWithRetry();
  startStatusServer();
  log("info", "analytics-service ready", {
    intervalMs: cfg.etlIntervalMs
  });

  while (true) {
    await runEtl();
    await delay(cfg.etlIntervalMs);
  }
}

start().catch((err) => {
  log("error", "fatal error", { error: err.message });
  process.exit(1);
});
