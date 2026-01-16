import fs from "fs";
import http from "http";
import path from "path";
import { parse } from "csv-parse/sync";
import { Pool } from "pg";
import { setTimeout as delay } from "timers/promises";

const cfg = {
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
  ),
  inboxPath: process.env.INBOX_PATH || "/data/inbox",
  processedPath: process.env.PROCESSED_PATH || "/data/processed",
  errorPath: process.env.ERROR_PATH || "/data/errors",
  pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || "5000", 10),
  csvDelimiter: process.env.CSV_DELIMITER || ",",
  csvHasHeader: parseBool(process.env.CSV_HAS_HEADER, true),
  fileExtension: ".csv",
  statusPort: parseInt(process.env.STATUS_PORT || "8093", 10)
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

let isProcessing = false;
const status = {
  startedAt: new Date().toISOString(),
  lastScanAt: null,
  lastFile: null,
  lastResult: null,
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
          lastScanAt: status.lastScanAt,
          lastFile: status.lastFile,
          lastResult: status.lastResult,
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

async function ensureDirs() {
  await fs.promises.mkdir(cfg.inboxPath, { recursive: true });
  await fs.promises.mkdir(cfg.processedPath, { recursive: true });
  await fs.promises.mkdir(cfg.errorPath, { recursive: true });
}

function normalizeRow(row) {
  if (!cfg.csvHasHeader) {
    const sku = row[0] ? String(row[0]).trim() : "";
    const qtyRaw = row[1];
    const priceRaw = row[2];
    return validateFields(sku, qtyRaw, priceRaw);
  }

  const normalized = {};
  for (const [key, value] of Object.entries(row)) {
    normalized[String(key).trim().toLowerCase()] = value;
  }

  const sku = normalized.sku ? String(normalized.sku).trim() : "";
  const qtyRaw =
    normalized.qty ?? normalized.quantity ?? normalized.stock ?? normalized.onhand;
  const priceRaw =
    normalized.price ?? normalized.unitprice ?? normalized.unit_price;

  return validateFields(sku, qtyRaw, priceRaw);
}

function validateFields(sku, qtyRaw, priceRaw) {
  if (!sku) {
    return { error: "missing_sku" };
  }

  const quantity = parseInt(qtyRaw, 10);
  if (!Number.isFinite(quantity) || quantity < 0) {
    return { error: "invalid_quantity" };
  }

  const price = parseFloat(priceRaw);
  if (!Number.isFinite(price) || price < 0) {
    return { error: "invalid_price" };
  }

  return { sku, quantity, price };
}

async function insertError(fileName, rowNumber, row, reason) {
  const rawRow = typeof row === "string" ? row : JSON.stringify(row);
  await pool.query(
    "INSERT INTO inventory_errors (file_name, row_number, raw_row, reason) VALUES ($1, $2, $3, $4)",
    [fileName, rowNumber, rawRow, reason]
  );
}

async function upsertItem(sku, quantity, price) {
  await pool.query(
    "INSERT INTO inventory_items (sku, quantity, price, updated_at) VALUES ($1, $2, $3, now()) " +
      "ON CONFLICT (sku) DO UPDATE SET quantity = EXCLUDED.quantity, price = EXCLUDED.price, updated_at = now()",
    [sku, quantity, price]
  );
}

async function recordRun(fileName, status, totals) {
  await pool.query(
    "INSERT INTO inventory_ingest_runs (file_name, status, total_rows, valid_rows, error_rows) VALUES ($1, $2, $3, $4, $5)",
    [fileName, status, totals.totalRows, totals.validRows, totals.errorRows]
  );
}

async function moveFile(sourcePath, targetDir, fileName) {
  const targetPath = path.join(targetDir, fileName);
  await fs.promises.rename(sourcePath, targetPath);
}

async function processFile(fileName) {
  status.lastFile = fileName;
  const sourcePath = path.join(cfg.inboxPath, fileName);
  const processingPath = path.join(cfg.inboxPath, `${fileName}.processing`);

  try {
    await fs.promises.rename(sourcePath, processingPath);
  } catch (err) {
    log("warn", "file already moved, skipping", { fileName, error: err.message });
    return;
  }

  const totals = { totalRows: 0, validRows: 0, errorRows: 0 };

  try {
    const content = await fs.promises.readFile(processingPath, "utf8");
    const records = parse(content, {
      columns: cfg.csvHasHeader,
      delimiter: cfg.csvDelimiter,
      skip_empty_lines: true,
      trim: true
    });

    for (let i = 0; i < records.length; i += 1) {
      const row = records[i];
      totals.totalRows += 1;
      const rowNumber = i + 1;
      const normalized = normalizeRow(row);

      if (normalized.error) {
        totals.errorRows += 1;
        await insertError(fileName, rowNumber, row, normalized.error);
        continue;
      }

      await upsertItem(normalized.sku, normalized.quantity, normalized.price);
      totals.validRows += 1;
    }

    await recordRun(fileName, "processed", totals);
    await moveFile(processingPath, cfg.processedPath, fileName);
    status.lastResult = "processed";
    status.lastError = null;
    log("info", "file processed", { fileName, ...totals });
  } catch (err) {
    totals.errorRows = totals.totalRows || totals.errorRows;
    status.lastResult = "failed";
    status.lastError = err.message;
    try {
      await recordRun(fileName, "failed", totals);
    } catch (recordErr) {
      log("error", "failed to record run", { error: recordErr.message });
    }

    try {
      await moveFile(processingPath, cfg.errorPath, fileName);
    } catch (moveErr) {
      log("error", "failed to move file to error", { error: moveErr.message });
    }

    log("error", "file processing failed", { fileName, error: err.message });
  }
}

async function processInbox() {
  if (isProcessing) return;
  isProcessing = true;
  status.lastScanAt = new Date().toISOString();

  try {
    const files = await fs.promises.readdir(cfg.inboxPath);
    const csvFiles = files.filter((file) =>
      file.toLowerCase().endsWith(cfg.fileExtension)
    );

    for (const fileName of csvFiles) {
      await processFile(fileName);
    }
  } catch (err) {
    log("error", "failed to read inbox", { error: err.message });
  } finally {
    isProcessing = false;
  }
}

async function start() {
  await ensureDirs();
  await connectWithRetry();
  startStatusServer();
  log("info", "file-ingest ready", {
    inbox: cfg.inboxPath,
    processed: cfg.processedPath,
    errors: cfg.errorPath,
    pollIntervalMs: cfg.pollIntervalMs
  });

  while (true) {
    await processInbox();
    await delay(cfg.pollIntervalMs);
  }
}

start().catch((err) => {
  log("error", "fatal error", { error: err.message });
  process.exit(1);
});
