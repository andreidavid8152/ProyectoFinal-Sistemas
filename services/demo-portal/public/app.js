const state = {
  token: "",
  orders: [],
  config: {},
  statusTimer: null
};

const priceCache = new Map();

const el = (id) => document.getElementById(id);

const tokenInput = el("token-input");
const tokenStatus = el("token-status");
const ordersList = el("orders-list");
const orderStatus = el("order-status");
const statusList = el("status-list");
const statusUpdated = el("status-updated");
const metricOrders = el("metric-orders");
const metricSystem = el("metric-system");

function loadState() {
  try {
    state.token = localStorage.getItem("integrahub_token") || "";
    const rawOrders = localStorage.getItem("integrahub_orders") || "[]";
    state.orders = JSON.parse(rawOrders);
  } catch {
    state.orders = [];
  }
  tokenInput.value = state.token;
  updateMetrics();
}

function saveOrders() {
  localStorage.setItem("integrahub_orders", JSON.stringify(state.orders));
  updateMetrics();
}

function saveToken(token) {
  state.token = token;
  localStorage.setItem("integrahub_token", token);
  tokenInput.value = token;
}

function updateMetrics() {
  metricOrders.textContent = String(state.orders.length);
}

function setHint(target, message, isError) {
  if (!target) return;
  target.textContent = message || "";
  target.style.color = isError ? "#7b2d1a" : "";
}

async function fetchSkuPrice(sku) {
  if (!sku) return null;
  const key = sku.toUpperCase();
  if (priceCache.has(key)) {
    return priceCache.get(key);
  }
  const result = await fetchJson(`/api/catalog?sku=${encodeURIComponent(key)}`);
  if (result.ok && result.data && result.data.price !== undefined) {
    const price = Number(result.data.price);
    if (Number.isFinite(price)) {
      priceCache.set(key, price);
      return price;
    }
  }
  return null;
}

async function updatePriceForRow(row) {
  const skuInput = row.querySelector(".item-sku");
  const priceInput = row.querySelector(".item-price");
  const rawSku = skuInput.value.trim();
  if (!rawSku) {
    priceInput.value = "0";
    recalcTotal();
    return;
  }
  const sku = rawSku.toUpperCase();
  skuInput.value = sku;
  row.dataset.lookupSku = sku;
  const price = await fetchSkuPrice(sku);
  if (row.dataset.lookupSku !== sku) {
    return;
  }
  if (price !== null) {
    priceInput.value = price.toFixed(2);
    priceInput.dataset.source = "catalog";
  } else {
    priceInput.value = "0";
    priceInput.dataset.source = "missing";
  }
  recalcTotal();
}

function addItemRow(item = {}) {
  const row = document.createElement("div");
  row.className = "item-row";
  row.innerHTML = `
    <input class="item-sku" type="text" placeholder="SKU" value="${item.sku || ""}" />
    <input class="item-qty" type="number" min="0" step="1" value="${item.qty || 1}" />
    <input class="item-price" type="number" min="0" step="0.01" value="${item.price || 0}" readonly />
    <button type="button" class="item-remove">X</button>
  `;
  const skuInput = row.querySelector(".item-sku");
  const qtyInput = row.querySelector(".item-qty");
  const priceInput = row.querySelector(".item-price");
  priceInput.readOnly = true;

  let skuTimer = null;
  const scheduleLookup = () => {
    if (skuTimer) clearTimeout(skuTimer);
    skuTimer = setTimeout(() => {
      updatePriceForRow(row);
    }, 400);
  };
  skuInput.addEventListener("input", scheduleLookup);
  skuInput.addEventListener("change", () => updatePriceForRow(row));
  qtyInput.addEventListener("input", recalcTotal);

  row.querySelector(".item-remove").addEventListener("click", () => {
    row.remove();
    recalcTotal();
  });
  el("items").appendChild(row);
  updatePriceForRow(row);
}

function collectItems() {
  const rows = Array.from(document.querySelectorAll(".item-row"));
  const items = [];
  rows.forEach((row) => {
    const sku = row.querySelector(".item-sku").value.trim();
    const qty = parseInt(row.querySelector(".item-qty").value, 10);
    const price = parseFloat(row.querySelector(".item-price").value);
    if (!sku) return;
    if (!Number.isFinite(qty) || qty <= 0) return;
    if (!Number.isFinite(price) || price < 0) return;
    items.push({ sku, qty, price });
  });
  return items;
}

function recalcTotal() {
  const items = collectItems();
  const total = items.reduce((sum, item) => sum + item.qty * item.price, 0);
  el("total").value = total.toFixed(2);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { raw: text };
  }
  return { ok: response.ok, status: response.status, data };
}

function renderOrders() {
  ordersList.innerHTML = "";
  if (!state.orders.length) {
    ordersList.innerHTML = `<div class="hint">No hay pedidos aun.</div>`;
    return;
  }

  state.orders.forEach((order) => {
    const card = document.createElement("div");
    card.className = "order-card";
    card.innerHTML = `
      <h3>${order.status || "UNKNOWN"}</h3>
      <div class="order-meta">
        <span><strong>Order ID:</strong> ${order.id}</span>
        <span><strong>Correlation:</strong> ${order.correlationId || "--"}</span>
        <span><strong>Ultimo evento:</strong> ${order.eventId || "--"}</span>
        <span><strong>Creado:</strong> ${order.createdAt || "--"}</span>
      </div>
    `;
    ordersList.appendChild(card);
  });
}

async function refreshOrderStatuses() {
  if (!state.token) {
    setHint(orderStatus, "Falta token para consultar estados.", true);
    return;
  }
  const updates = await Promise.all(
    state.orders.map(async (order) => {
      const result = await fetchJson(`/api/orders/${order.id}`, {
        headers: { Authorization: `Bearer ${state.token}` }
      });
      if (result.ok && result.data) {
        return { ...order, status: result.data.status || order.status };
      }
      return order;
    })
  );
  state.orders = updates;
  saveOrders();
  renderOrders();
}

async function createOrder(event) {
  event.preventDefault();
  if (!state.token) {
    setHint(orderStatus, "Ingresa un token JWT antes de crear pedidos.", true);
    return;
  }

  const items = collectItems();
  if (!items.length) {
    setHint(orderStatus, "Agrega al menos un item valido.", true);
    return;
  }

  const totalInput = parseFloat(el("total").value);
  const total = Number.isFinite(totalInput)
    ? totalInput
    : items.reduce((sum, item) => sum + item.qty * item.price, 0);
  const currency = el("currency").value.trim() || "USD";
  const correlationId = crypto.randomUUID();

  const result = await fetchJson("/api/orders", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${state.token}`,
      "X-Correlation-Id": correlationId
    },
    body: JSON.stringify({ items, total, currency })
  });

  if (!result.ok) {
    setHint(orderStatus, `Error al crear pedido (${result.status}).`, true);
    return;
  }

  const order = result.data.order || {};
  state.orders.unshift({
    id: order.id || "--",
    status: order.status || "CREATED",
    correlationId: result.data.correlationId || correlationId,
    eventId: result.data.eventId || "--",
    createdAt: order.createdAt || new Date().toISOString()
  });
  saveOrders();
  renderOrders();
  setHint(orderStatus, "Pedido creado y evento enviado.");
}

async function getToken(event) {
  event.preventDefault();
  const username = el("auth-user").value.trim();
  const password = el("auth-pass").value.trim();
  if (!username || !password) {
    setHint(tokenStatus, "Completa usuario y password.", true);
    return;
  }
  const result = await fetchJson("/api/token", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password })
  });
  if (!result.ok) {
    setHint(tokenStatus, "Credenciales invalidas.", true);
    return;
  }
  saveToken(result.data.access_token || "");
  setHint(tokenStatus, "Token generado.");
}

function renderStatus(services) {
  statusList.innerHTML = "";
  if (!services || !services.length) {
    statusList.innerHTML = `<div class="hint">Sin datos de estado.</div>`;
    return;
  }
  const upCount = services.filter((service) => service.status === "up").length;
  metricSystem.textContent = `${upCount}/${services.length} UP`;

  services.forEach((service) => {
    const pillClass = service.status || "disabled";
    const card = document.createElement("div");
    card.className = "status-card";
    card.innerHTML = `
      <div class="inline-actions">
        <span class="status-pill ${pillClass}">${service.status || "na"}</span>
        <strong>${service.name}</strong>
      </div>
      <div class="order-meta">
        <span>Latencia: ${service.ms ?? "--"} ms</span>
        <span>Codigo: ${service.code ?? "--"}</span>
      </div>
    `;
    statusList.appendChild(card);
  });
}

async function refreshStatus() {
  const result = await fetchJson("/api/status");
  if (!result.ok) {
    setHint(statusUpdated, "No se pudo obtener status.", true);
    metricSystem.textContent = "--";
    return;
  }
  renderStatus(result.data.services || []);
  statusUpdated.textContent = `Actualizado: ${new Date(
    result.data.updatedAt
  ).toLocaleTimeString()}`;
}

function setupLinks(config) {
  const links = config.links || {};
  const swagger = el("link-swagger");
  const auth = el("link-auth");
  const rabbit = el("link-rabbit");
  if (swagger) swagger.href = links.swaggerUi || "#";
  if (auth) auth.href = links.authToken || "#";
  if (rabbit) rabbit.href = links.rabbitmqUi || "#";
}

function setupAutoStatus() {
  const toggle = el("status-auto");
  const start = () => {
    if (state.statusTimer) return;
    state.statusTimer = setInterval(refreshStatus, 10000);
  };
  const stop = () => {
    if (state.statusTimer) {
      clearInterval(state.statusTimer);
      state.statusTimer = null;
    }
  };
  toggle.addEventListener("change", (event) => {
    if (event.target.checked) {
      start();
    } else {
      stop();
    }
  });
  if (toggle.checked) start();
}

async function bootstrap() {
  loadState();
  addItemRow({ sku: "SKU-100", qty: 2, price: 9.99 });
  addItemRow({ sku: "SKU-200", qty: 1, price: 15.5 });
  recalcTotal();
  renderOrders();

  const configResult = await fetchJson("/api/config");
  if (configResult.ok) {
    state.config = configResult.data || {};
    setupLinks(state.config);
  }

  await refreshStatus();
  setupAutoStatus();

  el("token-form").addEventListener("submit", getToken);
  el("save-token").addEventListener("click", () => saveToken(tokenInput.value.trim()));
  el("clear-token").addEventListener("click", () => saveToken(""));
  el("order-form").addEventListener("submit", createOrder);
  el("add-item").addEventListener("click", () => addItemRow());
  el("refresh-orders").addEventListener("click", refreshOrderStatuses);
  el("clear-orders").addEventListener("click", () => {
    state.orders = [];
    saveOrders();
    renderOrders();
  });
  el("refresh-status").addEventListener("click", refreshStatus);

  el("build-time").textContent = `Build: ${new Date().toISOString()}`;
}

bootstrap();
