"""The monitoring page, served with the task API."""

DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark">
<title>Queue Monitor · Retry Window Lab</title>
<style>
:root{--bg:#101714;--panel:#171f1b;--raised:#1c2822;--line:#2a3a31;--text:#eef4ee;--muted:#a8b9ae;--accent:#a8e5a5;--warn:#f3bd79;--bad:#f5a8a0;--blue:#aecbf6}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 system-ui,sans-serif}
button,input{font:inherit}
a{color:inherit}
header{border-bottom:1px solid var(--line)}
.nav,main,footer{max-width:1360px;margin:auto;padding-inline:32px}
.nav{height:66px;display:flex;align-items:center;justify-content:space-between;gap:16px}
.brand,.nav-end,.title-row,.panel-head,.form,.worker-row,.attempt-head{display:flex;align-items:center;gap:12px}
.brand{font-weight:720;letter-spacing:.01em;text-decoration:none}
.mark{display:grid;place-items:center;border-radius:9px;width:32px;height:32px;background:var(--accent);color:#13241a}
.nav-end a{color:var(--muted);text-decoration:none}
.nav-end a:hover{color:var(--text)}
.pill{display:inline-flex;align-items:center;gap:7px;color:var(--accent);background:#23372a;border:1px solid #355241;border-radius:20px;padding:5px 10px;font-size:12px;white-space:nowrap}
.pill:before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor}
.pill[data-status="stale"]{color:var(--warn);background:#33291d;border-color:#5c4930}
.pill[data-status="loading"]{color:var(--muted);background:var(--raised);border-color:var(--line)}
main{padding-block:34px 28px}
.title-row{justify-content:space-between;align-items:flex-end;margin-bottom:24px}
.eyebrow{font:650 11px/1.4 ui-monospace,monospace;letter-spacing:.14em;text-transform:uppercase;color:var(--accent);margin:0 0 6px}
h1,h2,h3,p{margin:0}
h1{font-size:clamp(30px,4vw,44px);line-height:1.06;letter-spacing:-.045em;margin-bottom:8px}
h2{font-size:17px;letter-spacing:-.015em}
h3{font-size:14px}
.muted,.description{color:var(--muted)}
.description{font-size:14px}
.small{font-size:12px}
button,.control,input{color:var(--text);background:var(--raised);border:1px solid var(--line);border-radius:8px}
button,.control{padding:8px 12px;cursor:pointer;text-decoration:none;white-space:nowrap}
button:hover,.control:hover{border-color:var(--accent)}
button:disabled{opacity:.6;cursor:wait}
:focus-visible{outline:2px solid var(--accent);outline-offset:3px}
.cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:14px}
.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:13px;min-width:0}
.card{padding:15px 17px;min-height:117px}
.card-label{font-size:12px;color:var(--muted);font-weight:650}
.metric{font-size:30px;font-weight:720;line-height:1.25;letter-spacing:-.045em;margin:6px 0 4px;font-variant-numeric:tabular-nums}
.metric small{font-size:15px;font-weight:500;color:var(--muted);letter-spacing:0}
.bar{height:5px;background:#293a31;border-radius:6px;overflow:hidden}
.bar span{display:block;height:100%;background:var(--accent);border-radius:6px;width:0}
.card[data-full="true"] .bar span{background:var(--warn)}
.card[data-full="true"] #capacity-caption{color:var(--warn)}
.overview{display:grid;grid-template-columns:1.4fr 1fr;gap:14px;margin-bottom:14px}
.panel{padding:16px 17px}
.panel-head{justify-content:space-between;align-items:flex-start;margin-bottom:12px}
.panel-head p{margin-top:3px}
.breakdown{display:grid;gap:7px}
.state-row{display:grid;grid-template-columns:110px minmax(0,1fr) 32px;align-items:center;gap:10px;font-size:12px}
.state-row b,.signal b{font-variant-numeric:tabular-nums;text-align:right}
.state-row .bar span{background:var(--blue)}
.state-row[data-state="SUCCEEDED"] .bar span{background:var(--accent)}
.state-row[data-state="NEEDS_REVIEW"] .bar span{background:var(--warn)}
.state-row[data-state="FAILED"] .bar span{background:var(--bad)}
.signal{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:9px 0;border-bottom:1px solid var(--line)}
.signal:last-child{border-bottom:0}
.signal b{font-size:18px}
.work-area{display:grid;grid-template-columns:minmax(0,2fr) minmax(0,1fr);gap:14px;margin-bottom:14px}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;text-align:left;white-space:nowrap;font-size:12px}
th{font-weight:650;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);font-size:10px}
th,td{padding:8px 7px;border-bottom:1px solid var(--line)}
tr:last-child td{border-bottom:0}
.task-link{font-family:ui-monospace,monospace;font-size:12px;background:transparent;border-color:transparent;color:var(--accent);padding:4px 5px;margin-left:-5px}
.task-link[aria-current="true"]{background:#23372a;border-color:#355241}
.form{gap:6px;flex-wrap:wrap}
.form input{min-width:0;width:min(232px,100%);padding:7px 9px}
.form button{padding:7px 9px}
.tag{font-size:11px;display:inline-block;background:#28382f;color:#d4e3da;border-radius:5px;padding:3px 7px;white-space:nowrap}
.tag[data-state="SUCCEEDED"]{background:#254332;color:var(--accent)}
.tag[data-state="NEEDS_REVIEW"],.tag[data-state="RETRY_WAIT"],.tag[data-state="UNKNOWN"]{background:#3c3023;color:var(--warn)}
.tag[data-state="FAILED"],.tag[data-state="KNOWN_FAILURE"]{background:#412d2a;color:var(--bad)}
.tag[data-state="RUNNING"],.tag[data-state="RECONCILING"]{background:#25374a;color:var(--blue)}
.worker-row{justify-content:space-between;align-items:flex-start;padding:9px 0;border-bottom:1px solid var(--line)}
.worker-row:last-child{border-bottom:0}
.worker-name{font:600 12px/1.5 ui-monospace,monospace;overflow-wrap:anywhere}
.empty{color:var(--muted);padding:15px 0}
.notice{padding:9px 12px;margin-bottom:14px;border:1px solid #655136;background:#32291e;color:var(--warn);border-radius:8px}
[hidden]{display:none!important}
#history{scroll-margin-top:16px}
#history-id{overflow-wrap:anywhere}
.attempt{padding:12px;border-radius:9px;border:1px solid var(--line);margin-top:9px}
.attempt-head{justify-content:space-between;flex-wrap:wrap}
.timeline{margin:8px 0 0;padding-left:20px}
.timeline li{padding:4px 0;overflow-wrap:anywhere}
code{font:12px ui-monospace,monospace;overflow-wrap:anywhere}
footer{padding-block:0 24px;color:var(--muted);font-size:12px}
@media(max-width:1020px){.work-area{grid-template-columns:1fr}.work-area .panel-head{flex-wrap:wrap}}
@media(max-width:660px){.nav,main,footer{padding-inline:16px}.nav{height:56px}.nav-end{gap:8px}.nav-end a{font-size:12px}main{padding-top:22px}.title-row{align-items:flex-start}.description{font-size:12px}.cards{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.card{padding:10px 11px}.metric{font-size:24px}.metric small{font-size:12px}.overview{grid-template-columns:1fr}.panel{padding:13px}.panel-head{flex-wrap:wrap}}
</style>
</head>
<body>
<header><div class="nav">
<a class="brand" href="/dashboard"><span class="mark" aria-hidden="true">
<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><path d="M4 5h12M4 10h9M4 15h6M15 13l2 2-2 2" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>
</span>Retry Window Lab</a>
<div class="nav-end"><a href="/docs">API docs</a><span class="pill" id="live" data-status="loading" role="status">Connecting</span></div>
</div></header>
<main>
<div class="title-row"><div><p class="eyebrow">Operations</p><h1>Queue monitor</h1>
<p class="description">Follow task execution, provider evidence and recovery decisions.</p></div>
<button type="button" id="refresh">Refresh</button></div>
<p id="notice" class="notice" role="alert" hidden></p>
<section class="cards" aria-label="Queue metrics">
<div class="card" id="capacity-card"><p class="card-label">Outstanding tasks</p><p class="metric" id="capacity">—</p>
<div class="bar" aria-hidden="true"><span id="capacity-bar"></span></div><p class="muted small" id="capacity-caption">Queue capacity</p></div>
<div class="card"><p class="card-label">Ready to execute</p><p class="metric" id="ready">—</p><p class="muted small" id="ready-caption">Queued work</p></div>
<div class="card"><p class="card-label">Active leases</p><p class="metric" id="leases">—</p><p class="muted small">Execution and reconciliation</p></div>
<div class="card"><p class="card-label">Needs review</p><p class="metric" id="review">—</p><p class="muted small">Unresolved task outcomes</p></div>
</section>
<div class="overview">
<section class="panel"><div class="panel-head"><div><h2>Task states</h2><p class="muted small">All durable tasks</p></div></div>
<div class="breakdown" id="states"><p class="empty">Loading task states…</p></div></section>
<section class="panel"><div class="panel-head"><div><h2>Recovery signals</h2><p class="muted small">Totals across stored history</p></div></div>
<div class="signal"><span>Retry decisions</span><b id="retries">—</b></div>
<div class="signal"><span>Reconciliation attempts</span><b id="reconciliations">—</b></div>
<div class="signal"><span>Expired leases</span><b id="expired">—</b></div></section>
</div>
<div class="work-area">
<section class="panel"><div class="panel-head"><div><h2>Recent tasks</h2><p class="muted small">Latest 24 updates</p></div>
<form class="form" id="lookup"><input id="task-input" aria-label="Task UUID" placeholder="Find task by UUID" maxlength="36" required><button type="submit">Find task</button></form></div>
<p class="notice" id="task-error" role="alert" hidden></p>
<div class="table-wrap" tabindex="0" aria-label="Scrollable recent tasks"><table aria-label="Recent tasks">
<thead><tr><th scope="col">Task ID</th><th scope="col">Kind</th><th scope="col">State</th><th scope="col">Attempts</th><th scope="col">Updated (UTC)</th></tr></thead>
<tbody id="tasks"><tr><td colspan="5" class="empty">Loading tasks…</td></tr></tbody>
</table></div></section>
<section class="panel"><div class="panel-head"><div><h2>Worker activity</h2><p class="muted small">Active leases and activity in the last 15 minutes</p></div></div>
<div id="workers"><p class="empty">Loading worker activity…</p></div></section>
</div>
<section class="panel" id="history" aria-labelledby="history-heading">
<div class="panel-head"><div><h2 id="history-heading" tabindex="-1">Task history</h2><p id="history-id" class="muted small">Select a task to inspect attempts and decisions.</p></div><span id="history-state"></span></div>
<div id="history-body"></div>
</section>
</main>
<footer>Read-only view · PostgreSQL snapshots every 4 seconds · <span id="as-of">Waiting for first snapshot</span></footer>
<script>
"use strict";
const el = id => document.getElementById(id);
const fmt = new Intl.NumberFormat("en");
const utc = value => value ? new Date(value).toLocaleTimeString("en-GB", {timeZone: "UTC"}) : "—";
const title = value => value.toLowerCase().replaceAll("_", " ").replace(/^./, c => c.toUpperCase());
const uuid = /^[\da-f]{8}-(?:[\da-f]{4}-){3}[\da-f]{12}$/i;
let selected = "";
let historyRequest = 0;
let refreshing = false;
let recentSignature = "";

function node(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = text;
  if (className) item.className = className;
  return item;
}
function badge(state) {
  const item = node("span", title(state), "tag");
  item.dataset.state = state;
  return item;
}
function problem(id, message = "") {
  el(id).textContent = message;
  el(id).hidden = !message;
}
async function read(url) {
  const response = await fetch(url, {cache: "no-store", signal: AbortSignal.timeout(10000)});
  if (!response.ok) throw new Error(String(response.status));
  return response.json();
}
function activeRow() {
  for (const button of el("tasks").querySelectorAll(".task-link")) {
    button.setAttribute("aria-current", String(button.dataset.taskId === selected));
  }
}
function renderSummary(data) {
  const capacity = data.capacity;
  el("capacity").replaceChildren(document.createTextNode(fmt.format(capacity.outstanding) + " "),
    node("small", "/ " + fmt.format(capacity.limit)));
  el("capacity-card").dataset.full = capacity.at_capacity;
  el("capacity-bar").style.width = Math.min(100, 100 * capacity.outstanding / capacity.limit) + "%";
  el("capacity-caption").textContent = capacity.at_capacity ? "At capacity · new work returns 429" : "Capacity available";
  el("ready").textContent = fmt.format(data.queue.ready);
  el("ready-caption").textContent = data.queue.pending_publication + " awaiting publish · " +
    data.queue.awaiting_context + " awaiting context";
  el("leases").textContent = fmt.format(data.active_leases);
  el("review").textContent = fmt.format(data.states.NEEDS_REVIEW);
  el("retries").textContent = fmt.format(data.recovery.retry_decisions);
  el("reconciliations").textContent = fmt.format(data.recovery.reconciliation_attempts);
  el("expired").textContent = fmt.format(data.recovery.expired_leases);

  const total = Object.values(data.states).reduce((a, b) => a + b, 0);
  el("states").replaceChildren(...Object.entries(data.states).map(([state, count]) => {
    const row = node("div", undefined, "state-row");
    row.dataset.state = state;
    const bar = node("div", undefined, "bar");
    bar.setAttribute("aria-hidden", "true");
    const fill = node("span");
    fill.style.width = (total ? 100 * count / total : 0) + "%";
    bar.append(fill);
    row.append(node("span", title(state)), bar, node("b", fmt.format(count)));
    return row;
  }));

  const signature = JSON.stringify(data.recent_tasks);
  if (signature !== recentSignature) {
    recentSignature = signature;
    const rows = data.recent_tasks.map(task => {
      const tr = node("tr");
      const id = node("td");
      const link = node("button", task.task_id.slice(0, 8), "task-link");
      link.type = "button";
      link.title = task.task_id;
      link.dataset.taskId = task.task_id;
      link.setAttribute("aria-label", "Inspect task " + task.task_id);
      link.addEventListener("click", () => openTask(task.task_id));
      id.append(link);
      const state = node("td");
      state.append(badge(task.state));
      tr.append(id, node("td", title(task.kind)), state, node("td", task.attempt_count),
        node("td", utc(task.updated_at)));
      return tr;
    });
    if (!rows.length) {
      const row = node("tr");
      const empty = node("td", "No tasks recorded yet.", "empty");
      empty.colSpan = 5;
      row.append(empty);
      rows.push(row);
    }
    el("tasks").replaceChildren(...rows);
    activeRow();
  }

  const workers = data.workers.map(worker => {
    const row = node("div", undefined, "worker-row");
    const info = node("div");
    info.append(node("p", worker.worker_id, "worker-name"),
      node("p", title(worker.purpose) + " · " + worker.recent_attempts + " recent", "muted small"));
    row.append(info, node("span", worker.active_leases ? worker.active_leases + " active" : "Recent", "tag"));
    return row;
  });
  el("workers").replaceChildren(...(workers.length ? workers : [node("p", "No recent attempts.", "empty")]));
  el("as-of").textContent = "Updated " + utc(data.captured_at) + " UTC";
}
function renderHistory(history) {
  const task = history.task;
  el("history-id").textContent = task.task_id;
  el("history-state").replaceChildren(badge(task.state));
  const meta = node("p", title(task.kind) + " · Generation " + task.delivery_generation +
    " · " + task.attempt_count + " attempts", "muted small");
  const operation = node("p", "Operation " + task.operation_id, "muted small");
  const articles = history.attempts.map(attempt => {
    const article = node("article", undefined, "attempt");
    const head = node("div", undefined, "attempt-head");
    head.append(node("h3", "Attempt " + attempt.number + " · " + title(attempt.purpose)), badge(attempt.outcome));
    article.append(head,
      node("p", attempt.worker_id + " · " + utc(attempt.started_at) + " UTC · Lease " + attempt.lease_token, "muted small"),
      node("code", attempt.attempt_id));
    if (attempt.error_code) article.append(node("p", title(attempt.error_code), "muted small"));
    const timeline = [
      ...history.dispatches.filter(item => item.attempt_id === attempt.attempt_id)
        .map(item => [item.started_at, "Provider dispatch", item.dispatch_id]),
      ...history.observations.filter(item => item.attempt_id === attempt.attempt_id)
        .map(item => [item.received_at, title(item.source) + " observation" +
          (item.http_status ? " · HTTP " + item.http_status : "") +
          (item.error_code ? " · " + title(item.error_code) : ""), item.observation_id]),
      ...history.decisions.filter(item => item.attempt_id === attempt.attempt_id)
        .map(item => [item.created_at, "Decision · " + title(item.reason), item.decision_id]),
    ].sort((a,b) => Date.parse(a[0]) - Date.parse(b[0]));
    if (timeline.length) {
      const list = node("ol", undefined, "timeline");
      for (const [time, text, id] of timeline) {
        const item = node("li", utc(time) + " UTC · " + text);
        item.append(node("br"), node("code", id, "muted"));
        list.append(item);
      }
      article.append(list);
    }
    return article;
  });
  const taskDecisions = history.decisions.filter(item => !item.attempt_id)
    .map(item => node("p", "Decision · " + title(item.reason) + " · " + item.decision_id, "attempt"));
  el("history-body").replaceChildren(meta, operation,
    ...(articles.length ? articles : [node("p", "No attempts recorded for this task.", "empty")]), ...taskDecisions);
}
async function loadHistory(id) {
  const request = ++historyRequest;
  try {
    const data = await read("/monitoring/tasks/" + encodeURIComponent(id));
    if (request === historyRequest) renderHistory(data);
  } catch (error) {
    if (request === historyRequest) {
      el("history-body").replaceChildren(node("p",
        error.message === "404" ? "Task not found." : "Task history is unavailable. Refresh to retry.", "empty"));
      el("history-state").replaceChildren();
    }
  }
}
function openTask(id) {
  if (!uuid.test(id)) {
    problem("task-error", "Enter a complete task UUID.");
    return;
  }
  problem("task-error");
  selected = id.toLowerCase();
  el("task-input").value = selected;
  el("history-id").textContent = selected;
  el("history-state").replaceChildren();
  el("history-body").replaceChildren(node("p", "Loading task history…", "empty"));
  activeRow();
  el("history-heading").focus();
  el("history").scrollIntoView({block: "start"});
  loadHistory(selected);
}
async function refresh() {
  if (refreshing) return;
  refreshing = true;
  el("refresh").disabled = true;
  try {
    renderSummary(await read("/monitoring/summary"));
    problem("notice");
    el("live").dataset.status = "live";
    el("live").textContent = "Live";
    if (selected) await loadHistory(selected);
  } catch {
    problem("notice", "Queue snapshot unavailable. The last successful values remain on screen.");
    el("live").dataset.status = "stale";
    el("live").textContent = "Unavailable";
  } finally {
    refreshing = false;
    el("refresh").disabled = false;
  }
}
el("lookup").addEventListener("submit", event => {
  event.preventDefault();
  openTask(el("task-input").value.trim());
});
el("refresh").addEventListener("click", refresh);
refresh();
setInterval(refresh, 4000);
</script>
</body>
</html>
"""
