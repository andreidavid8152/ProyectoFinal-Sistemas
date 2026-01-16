import express from "express";
import jwt from "jsonwebtoken";

const cfg = {
  port: parseInt(process.env.AUTH_SERVICE_PORT || process.env.PORT || "8082", 10),
  jwtSigningKey: process.env.JWT_SIGNING_KEY || "change_me",
  jwtIssuer: process.env.JWT_ISSUER || "integrahub",
  jwtAudience: process.env.JWT_AUDIENCE || "integrahub-api",
  user: process.env.AUTH_USER || "demo",
  password: process.env.AUTH_PASSWORD || "demo",
  ttlSeconds: parseInt(process.env.AUTH_TOKEN_TTL_SECONDS || "3600", 10)
};

function log(level, message, extra) {
  const base = `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}`;
  if (extra) {
    console.log(base, JSON.stringify(extra));
  } else {
    console.log(base);
  }
}

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

app.post("/token", (req, res) => {
  const body = req.body || {};
  if (!body.username || !body.password) {
    return res.status(400).json({ error: "missing_credentials" });
  }

  if (body.username !== cfg.user || body.password !== cfg.password) {
    return res.status(401).json({ error: "invalid_credentials" });
  }

  const token = jwt.sign(
    { sub: body.username, role: "demo" },
    cfg.jwtSigningKey,
    {
      issuer: cfg.jwtIssuer,
      audience: cfg.jwtAudience,
      expiresIn: cfg.ttlSeconds
    }
  );

  return res.json({
    access_token: token,
    token_type: "Bearer",
    expires_in: cfg.ttlSeconds
  });
});

app.use((err, req, res, next) => {
  if (err && err.type === "entity.parse.failed") {
    return res.status(400).json({ error: "invalid_json" });
  }
  return next(err);
});

app.listen(cfg.port, () => {
  log("info", "auth-service listening", { port: cfg.port });
});
