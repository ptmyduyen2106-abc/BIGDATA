"use strict";
const $ = (id) => document.getElementById(id);
const state = { run: null, version: null, latencies: [] };
const format = (value) => typeof value === "number" && Number.isFinite(value) ? new Intl.NumberFormat("vi-VN").format(value) : "—";
const percent = (value) => typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : "—";
const el = (name, cls, text) => { const node = document.createElement(name); if (cls) node.className = cls; if (text !== undefined) node.textContent = text; return node; };
function notice(message) { $("notice").hidden = !message; $("notice").textContent = message || ""; }
function clearModelView() {
  state.run = null; state.version = null; state.latencies = [];
  for (const id of ["kpi-events", "kpi-users", "kpi-items", "kpi-ratio", "model-version", "trained-at", "metric-model-version", "exported-users"]) $(id).textContent = "—";
  $("event-bars").replaceChildren(el("p", "muted", "Chưa có KPI của run đang chọn."));
  $("model-metrics").replaceChildren(el("p", "muted", "Chưa có model đánh giá của run đang chọn."));
  $("sample-users").replaceChildren(el("option", "", "Chưa có user của run đang chọn")); $("sample-users").firstChild.value = "";
  $("recommend-results").className = "results empty";
  $("recommend-results").replaceChildren(el("p", "muted", "Model chưa sẵn sàng. Không hiển thị kết quả của run trước."));
  $("recommend-meta").textContent = "Đợi model hợp lệ cho run đã chọn.";
  $("updated-at").textContent = "Chưa có dữ liệu của run này";
  chart();
}
async function get(path) {
  const response = await fetch(path, { cache: "no-store" });
  const body = await response.json();
  if (!response.ok) { const error = new Error(typeof body.detail === "string" ? body.detail : "Run hiện tại chưa có model bundle hợp lệ. Hãy chạy pipeline và thử cập nhật lại."); error.status = response.status; throw error; }
  return body;
}
function first(source, keys) { for (const key of keys) { if (typeof source?.[key] === "number") return source[key]; } return null; }
function showHealth(health) {
  const ready = health.model?.ready === true;
  $("health-dot").className = `status-dot ${ready ? "ok" : "bad"}`;
  $("health-text").textContent = !ready ? "Chưa sẵn sàng" : health.status === "degraded" ? "Sẵn sàng · có giới hạn" : "Sẵn sàng phục vụ";
  $("dependencies").replaceChildren();
  const model = el("span", `dependency ${ready ? "" : "bad"}`, `Model ${ready ? "sẵn sàng" : "chưa có"}`);
  $("dependencies").append(model);
  for (const [name, value] of Object.entries(health.dependencies || {})) {
    const label = !value.enabled ? "không bật" : value.status === "ok" ? "kết nối" : "tạm mất kết nối";
    $("dependencies").append(el("span", `dependency ${!value.enabled ? "off" : value.status === "ok" ? "" : "bad"}`, `${name === "mongodb" ? "MongoDB" : "Redis"} · ${label}`));
  }
  if (health.model?.stale) notice("Bản cập nhật model cùng run chưa hợp lệ. API đang giữ bản đã xác thực trước đó; xem phiên bản đang phục vụ bên dưới.");
  else if (!ready) notice("Chưa có model bundle dùng được cho run đã chọn. Liveness vẫn hoạt động; hãy chạy pipeline Phase 1–7 để tạo model và KPI.");
  else if (health.status === "degraded") notice("Một dịch vụ online đang không khả dụng. API vẫn dùng model bundle và lịch sử cục bộ của đúng run; lịch sử mới trong dịch vụ bị gián đoạn có thể chưa được đồng bộ.");
  else notice(null);
}
function showKpis(data) {
  const kpis = data.kpis || {};
  const counts = kpis.event_counts || kpis.events || kpis.counts_by_event || {};
  const values = { view: first(counts, ["view", "page_view"]), addtocart: first(counts, ["addtocart", "add_to_cart"]), transaction: first(counts, ["transaction", "purchase"]), review: first(counts, ["review"]) };
  $("kpi-events").textContent = format(first(kpis, ["silver_events", "total_events", "event_count", "clean_events", "clean_event_count", "total_rows", "silver_rows"]));
  $("kpi-users").textContent = format(first(kpis, ["visitors", "unique_visitors", "distinct_visitors", "unique_users", "visitor_count"]));
  $("kpi-items").textContent = format(first(kpis, ["items", "unique_items", "distinct_items", "item_count"]));
  const ratio = first(kpis, ["purchase_per_view", "event_purchase_per_view"]) ?? (values.view > 0 && values.transaction !== null ? values.transaction / values.view : null);
  $("kpi-ratio").textContent = percent(ratio);
  $("event-bars").replaceChildren();
  if (Object.values(values).every((value) => value === null)) { $("event-bars").append(el("p", "muted", "KPI của run chưa được xuất. Dashboard không điền số liệu giả.")); return; }
  const max = Math.max(...Object.values(values).map((value) => value || 0), 1);
  const labels = [["view", "Xem sản phẩm", "#377f65"], ["addtocart", "Thêm giỏ hàng", "#8eaf72"], ["transaction", "Mua hàng", "#c5dc91"], ["review", "Đánh giá", "#c5cec5"]];
  for (const [key, label, color] of labels) {
    const value = values[key]; if (value === null) continue;
    const row = el("div", "event-row"); const name = el("div", "event-name"); const dot = el("span"); dot.style.background = color; name.append(dot, document.createTextNode(label));
    const track = el("div", "bar-track"); const fill = el("div", "bar-fill"); fill.style.background = color; fill.style.width = `${value / max * 100}%`; track.append(fill); row.append(name, track, el("div", "event-number", format(value))); $("event-bars").append(row);
  }
  const visitorRatio = first(kpis, ["visitor_purchase_conversion"]);
  if (visitorRatio !== null) $("event-bars").append(el("p", "panel-note", `Visitor có mua / tổng visitor quan sát: ${percent(visitorRatio)} · đếm visitor phân biệt.`));
}
function showModel(data) {
  $("model-version").textContent = data.model_version;
  $("trained-at").textContent = data.trained_at_utc;
  $("metric-model-version").textContent = data.metric_model_version || "Chưa công bố";
  $("exported-users").textContent = format(data.exported_recommendation_users);
  const metrics = data.model_metrics?.heldout_test || data.model_metrics?.test || data.model_metrics;
  $("model-metrics").replaceChildren();
  for (const [keys, label] of [[["precision_at_k", "precision@k"], "Precision@K"], [["recall_at_k", "recall@k"], "Recall@K"], [["ndcg_at_k", "ndcg@k"], "NDCG@K"]]) {
    const value = first(metrics, keys); const node = el("div", "metric-small"); node.append(el("span", "", label), el("strong", "", percent(value))); $("model-metrics").append(node);
  }
  if (data.metric_model_version && data.metric_model_version !== data.model_version) $("model-metrics").title = `Metric của model đánh giá ${data.metric_model_version}; model phục vụ được refit sau đánh giá.`;
}
async function refresh() {
  $("refresh").disabled = true;
  try {
    const healthResponse = await fetch("/health/ready", { cache: "no-store" }); const health = await healthResponse.json(); showHealth(health);
    if (!health.model?.ready) { clearModelView(); $("run-id").textContent = health.model?.run_id || "chưa chọn"; return; }
    const data = await get("/api/metrics");
    if (state.run !== data.run_id || state.version !== data.model_version) { state.latencies = []; chart(); $("recommend-results").replaceChildren(el("p", "muted", "Model đã cập nhật. Chọn user để lấy gợi ý của đúng phiên bản này.")); $("recommend-meta").textContent = "Mỗi kết quả luôn ghi rõ run và model version."; }
    state.run = data.run_id; state.version = data.model_version;
    $("run-id").textContent = data.run_id; showKpis(data); showModel(data);
    $("updated-at").textContent = `Cập nhật ${new Date().toLocaleTimeString("vi-VN")}`;
    const users = await get("/api/users?limit=100");
    if (users.run_id !== state.run || users.model_version !== state.version) { notice("Run vừa thay đổi trong lúc tải. Bấm cập nhật để xem một phiên bản thống nhất."); return; }
    $("sample-users").replaceChildren(el("option", "", "Chọn một người dùng…")); $("sample-users").firstChild.value = "";
    for (const id of users.user_ids) { const option = el("option", "", `Visitor ${id}`); option.value = String(id); $("sample-users").append(option); }
    if (!$("visitor-id").value && users.user_ids.length) { $("visitor-id").value = String(users.user_ids[0]); $("sample-users").value = String(users.user_ids[0]); }
  } catch (error) { notice(`Không tải được dữ liệu: ${error.message}`); $("health-dot").className = "status-dot bad"; $("health-text").textContent = "Mất kết nối · xem thời điểm tải"; } finally { $("refresh").disabled = false; }
}
function chart() {
  const canvas = $("latency-chart"); const size = canvas.getBoundingClientRect(); const dpr = window.devicePixelRatio || 1; canvas.width = Math.max(1, size.width * dpr); canvas.height = 140 * dpr; const ctx = canvas.getContext("2d"); ctx.scale(dpr, dpr);
  const values = state.latencies; $("chart-empty").hidden = !!values.length; if (!values.length) { $("latency-current").replaceChildren(document.createTextNode("—"), el("small", "", " ms")); return; }
  $("latency-current").replaceChildren(document.createTextNode(values.at(-1).toFixed(1)), el("small", "", " ms"));
  const width = size.width; const left = 37; const right = width - 8; const top = 12; const bottom = 113; const peak = Math.max(...values, 1) * 1.16;
  ctx.font = "9px system-ui"; ctx.strokeStyle = "#eaf0eb"; ctx.fillStyle = "#99aaa1"; ctx.lineWidth = 1;
  for (let i = 0; i < 3; i++) { const y = top + (bottom - top) * i / 2; ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(right, y); ctx.stroke(); ctx.fillText((peak * (1 - i / 2)).toFixed(0), 0, y + 3); }
  const point = (v, i) => [values.length === 1 ? (left + right) / 2 : left + (right - left) * i / (values.length - 1), bottom - v / peak * (bottom - top)];
  ctx.beginPath(); values.forEach((v, i) => { const [x, y] = point(v, i); if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); }); ctx.strokeStyle = "#2b805a"; ctx.lineWidth = 2; ctx.stroke();
  values.forEach((v, i) => { const [x, y] = point(v, i); ctx.beginPath(); ctx.arc(x, y, 3, 0, Math.PI * 2); ctx.fillStyle = "#fff"; ctx.fill(); ctx.strokeStyle = "#2b805a"; ctx.stroke(); });
  ctx.fillStyle = "#a1afa7"; ctx.fillText(`Truy vấn 1`, left, 136); if (values.length > 1) ctx.fillText(`Truy vấn ${values.length}`, Math.max(left + 65, right - 56), 136);
}
async function recommend(event) {
  event.preventDefault(); $("recommend-error").hidden = true; const input = $("visitor-id").value.trim();
  if (!/^[0-9]+$/.test(input) || input.length > 10 || Number(input) > 2147483647) { $("recommend-error").textContent = "Visitor ID phải là số nguyên từ 0 đến 2.147.483.647."; $("recommend-error").hidden = false; return; }
  $("recommend-button").disabled = true; const started = performance.now();
  try {
    const data = await get(`/api/recommendations/${encodeURIComponent(input)}?k=${$("top-k").value}`); const elapsed = performance.now() - started;
    if (data.run_id !== state.run || data.model_version !== state.version) {
      await refresh();
      if (data.run_id !== state.run || data.model_version !== state.version) throw new Error("Run thay đổi trong lúc truy vấn. Hãy gửi lại để lấy đúng model đang hiển thị.");
    }
    state.latencies.push(elapsed); state.latencies = state.latencies.slice(-20); chart();
    const source = data.source === "popular_fallback" ? "Baseline phổ biến · user chưa có gợi ý xuất sẵn" : "ALS cá nhân hóa · có thể bổ sung baseline khi thiếu ứng viên";
    $("recommend-meta").textContent = `${source} · ${data.returned}/${data.requested_k} sản phẩm · loại ${format(data.seen_items_excluded)} item đã thấy · ${data.cache_hit ? "Redis cache" : "model bundle"} · API ${data.request_ms.toFixed(1)} ms · ${data.run_id} / ${data.model_version}`;
    if (data.warnings?.length || data.model_stale) { $("recommend-error").textContent = data.model_stale ? "Đang dùng bản model hợp lệ trước đó của cùng run vì bản cập nhật chưa hợp lệ." : "Dịch vụ lịch sử online tạm gián đoạn; đã dùng lịch sử còn khả dụng của cùng run."; $("recommend-error").hidden = false; }
    $("recommend-results").className = data.items.length ? "results" : "results empty"; $("recommend-results").replaceChildren();
    if (!data.items.length) $("recommend-results").append(el("strong", "", "Không còn ứng viên chưa tương tác trong tập sản phẩm đã xuất."));
    data.items.forEach((item, index) => { const card = el("article", "result-card"); card.append(el("span", "result-rank", `GỢI Ý ${String(index + 1).padStart(2, "0")}`), el("div", "product-icon", "▧"), el("h3", "", `Item ${item.itemid}`), el("p", "", "Mã sản phẩm RetailRocket"), el("div", "result-score", `Score ${Number(item.score).toPrecision(4)}`), el("small", "", "Điểm xếp hạng · không phải xác suất")); $("recommend-results").append(card); });
  } catch (error) { $("recommend-error").textContent = error.message; $("recommend-error").hidden = false; } finally { $("recommend-button").disabled = false; }
}
$("sample-users").addEventListener("change", (event) => { if (event.target.value) $("visitor-id").value = event.target.value; });
$("refresh").addEventListener("click", refresh); $("recommend-form").addEventListener("submit", recommend); window.addEventListener("resize", chart); refresh();
