// QueueFlow dashboard: hash-routed, read-only views over /api/v1.

import * as api from "./api.js";
import {
  html,
  raw,
  time,
  status,
  statusLabel,
  json,
  duration,
  num,
  shortId,
  dlqReason,
  isEmptyObject,
  localToIso,
  isoToLocal,
  exact,
} from "./fmt.js";

const MERMAID_URL = "https://cdn.jsdelivr.net/npm/mermaid@11.17.2/dist/mermaid.esm.min.mjs";
const PAGE_SIZE = 50;
const OVERVIEW_REFRESH_MS = 5000;
const POLL_MS = 3000;

const main = document.getElementById("main");
const gate = document.getElementById("gate");
const gateForm = document.getElementById("gate-form");
const gateInput = document.getElementById("gate-token");
const gateMsg = document.getElementById("gate-msg");

const JOB_TERMINAL = new Set(["completed", "failed", "cancelled"]);
const WORKFLOW_TERMINAL = new Set(["completed", "failed", "partially_failed", "cancelled"]);

const enc = encodeURIComponent;

// ---- Routing -----------------------------------------------------------------

let current = { cleanup: null, token: 0 };

function parseHash() {
  const h = location.hash.replace(/^#/, "") || "/queues";
  const qi = h.indexOf("?");
  const path = qi >= 0 ? h.slice(0, qi) : h;
  const query = new URLSearchParams(qi >= 0 ? h.slice(qi + 1) : "");
  const parts = path.split("/").filter(Boolean).map(decodeURIComponent);
  return { parts, query };
}

function hashFor(path, params) {
  return `#${path}${api.buildQuery(params)}`;
}

function setNav(section) {
  document.querySelectorAll(".top nav a").forEach((a) => {
    if (a.dataset.nav === section) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
}

const ROUTES = {
  queues: { list: overview },
  jobs: { list: jobsList, detail: jobDetail, title: "Jobs" },
  workflows: { list: workflowsList, detail: workflowDetail, title: "Workflows" },
  dlq: { list: dlqList, detail: dlqDetail, title: "Dead letters" },
  cron: { list: cronList, title: "Cron" },
  tasks: { list: tasksList, title: "Tasks" },
};

async function route() {
  if (!api.auth.get()) {
    showGate("");
    return;
  }
  if (current.cleanup) current.cleanup();
  const token = ++current.token;
  const { parts, query } = parseHash();
  const section = parts[0] || "queues";
  const def = ROUTES[section];
  if (!def) {
    location.replace("#/queues");
    return;
  }
  setNav(section);
  const ctx = {
    query,
    cleanups: [],
    alive: () => current.token === token,
    set(content) {
      if (current.token === token) main.innerHTML = String(content);
    },
  };
  current.cleanup = () => {
    ctx.cleanups.forEach((f) => {
      try {
        f();
      } catch {
        // cleanup is best effort
      }
    });
    ctx.cleanups = [];
  };
  const view = parts[1] && def.detail ? () => def.detail(ctx, parts[1]) : () => def.list(ctx);
  try {
    await view();
  } catch (e) {
    if (!ctx.alive() || (e && e.name === "AbortError")) return;
    if (e instanceof api.ApiError && e.status === 401) return; // gate is showing
    ctx.set(errorState(`Could not load ${def.title ? def.title.toLowerCase() : "the overview"}`, e));
  }
}

window.addEventListener("hashchange", route);

// ---- Token gate ---------------------------------------------------------------

function showGate(message) {
  if (current.cleanup) current.cleanup();
  gateMsg.textContent = message || "";
  gate.hidden = false;
  main.innerHTML = "";
  setTimeout(() => gateInput.focus(), 0);
}

gateForm.addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const token = gateInput.value.trim();
  if (!token) return;
  gateMsg.textContent = "";
  api.auth.set(token);
  try {
    await api.get("/tasks");
  } catch (e) {
    api.auth.clear();
    gate.hidden = false;
    gateMsg.textContent =
      e.status === 401
        ? "The server rejected that token. Use a tenant API key or JWT, not the worker token."
        : e.message || "The server could not be reached.";
    gateInput.focus();
    return;
  }
  gateInput.value = "";
  gate.hidden = true;
  route();
});

window.addEventListener(api.UNAUTHORIZED_EVENT, () => {
  const had = api.auth.get();
  api.auth.clear();
  showGate(had ? "The server rejected the token. Enter a tenant token to continue." : "");
});

document.getElementById("change-token").addEventListener("click", () => {
  api.auth.clear();
  showGate("");
});

// ---- Shared pieces --------------------------------------------------------------

function errorState(title, err, retryHash) {
  const msg = err instanceof api.ApiError ? err.message : err && err.message ? err.message : String(err);
  const retry = retryHash === null ? "" : html`<button type="button" class="btn" data-retry>Try again</button>`;
  return html`<div class="state error"><strong>${title}</strong>${msg} ${retry}</div>`;
}

main.addEventListener("click", (ev) => {
  if (ev.target.closest("[data-retry]")) route();
});

function emptyState(title, hint) {
  return html`<div class="state"><strong>${title}</strong>${hint}</div>`;
}

function loadingState(what) {
  return html`<div class="state">Loading ${what}</div>`;
}

function viewHead(title, meta) {
  document.title = `${title} | QueueFlow`;
  return html`<div class="view-head"><h1>${title}</h1><div class="meta">${meta || ""}</div></div>`;
}

function crumbs(section, label) {
  return html`<div class="crumbs"><a href="#/${section}">${label}</a></div>`;
}

function pager(resp, path, params, cursorParam) {
  const items = [];
  if (cursorParam) items.push(html`<a href="${hashFor(path, { ...params, cursor: "" })}">First page</a>`);
  if (resp.has_more && resp.next_cursor) {
    items.push(html`<a class="btn" href="${hashFor(path, { ...params, cursor: resp.next_cursor })}">Next page</a>`);
  } else if (cursorParam) {
    items.push(html`<span>End of list</span>`);
  }
  if (!items.length) return raw("");
  return html`<div class="pager">${items}</div>`;
}

function table(headers, rows) {
  return html`<div class="panel table-wrap"><table>
    <thead><tr>${headers.map((h) => html`<th${h.num ? raw(' class="num"') : ""}>${h.label ?? h}</th>`)}</tr></thead>
    <tbody>${rows}</tbody></table></div>`;
}

function dl(pairs) {
  return html`<dl class="panel facts">${pairs
    .filter((p) => p)
    .map(([k, v]) => html`<dt>${k}</dt><dd>${v}</dd>`)}</dl>`;
}

function filtersFrom(query, keys) {
  const out = {};
  for (const k of keys) {
    const v = query.get(k);
    if (v) out[k] = v;
  }
  return out;
}

function tenantFact(obj) {
  return obj.tenant_id ? ["Tenant", obj.tenant_id] : null;
}

// ---- Overview --------------------------------------------------------------------

async function overview(ctx) {
  let paused = false;
  let lastOk = null;
  ctx.set(html`${viewHead(
    "Overview",
    html`<span id="ov-updated">Loading</span><button type="button" class="btn" id="ov-pause" aria-pressed="false">Pause refresh</button>`,
  )}
  <h2>Queues</h2>
  <p class="note">Live backlog for this tenant. Queues with no pending, scheduled, or running jobs are not listed. Refreshes every 5 seconds.</p>
  <section id="ov-queues">${loadingState("queues")}</section>
  <h2>Totals</h2>
  <p class="note">Durable counts read from the store. Retention deletes history, so they can go down.</p>
  <section id="ov-totals">${loadingState("totals")}</section>`);

  const pauseBtn = document.getElementById("ov-pause");
  pauseBtn.addEventListener("click", () => {
    paused = !paused;
    pauseBtn.setAttribute("aria-pressed", String(paused));
    pauseBtn.textContent = paused ? "Resume refresh" : "Pause refresh";
    setUpdated();
    if (!paused) tick(true);
  });

  function setUpdated(err) {
    const el = document.getElementById("ov-updated");
    if (!el) return;
    const when = lastOk ? `Updated ${exact(lastOk).slice(11, 19)}` : "Not loaded yet";
    el.textContent = err ? `${when}, refresh failed` : paused ? `${when}, paused` : when;
  }

  function renderQueues(queues) {
    const el = document.getElementById("ov-queues");
    if (!el) return;
    if (!queues.length) {
      el.innerHTML = String(
        emptyState("No live jobs on any queue", "Queues appear here as soon as a job is pending, scheduled, or running."),
      );
      return;
    }
    const maxAge = Math.max(1, ...queues.map((q) => q.oldest_pending_age_secs || 0));
    const rows = queues.map((q) => {
      const age = q.oldest_pending_age_secs;
      let ageCell;
      if (age == null) {
        ageCell = html`<span class="zero">nothing waiting</span>`;
      } else {
        const pct = Math.max(2, Math.round((age / maxAge) * 100));
        const cls = age >= 3600 ? "age critical" : age >= 600 ? "age hot" : "age";
        ageCell = html`<span class="${cls}" title="Oldest claimable job has waited ${duration(age)}"><span class="bar"><i style="width:${pct}%"></i></span>${duration(age)}</span>`;
      }
      const n = (v) => (v ? html`${num(v)}` : html`<span class="zero">0</span>`);
      return html`<tr>
        <td><a href="${hashFor("/jobs", { queue: q.queue })}">${q.queue}</a></td>
        <td class="num">${n(q.pending)}</td>
        <td class="num">${n(q.scheduled)}</td>
        <td class="num">${n(q.running)}</td>
        <td>${ageCell}</td>
      </tr>`;
    });
    el.innerHTML = String(
      table(
        ["Queue", { label: "Pending", num: true }, { label: "Scheduled", num: true }, { label: "Running", num: true }, "Oldest pending"],
        rows,
      ),
    );
  }

  function renderTotals(s) {
    const el = document.getElementById("ov-totals");
    if (!el) return;
    const cells = [
      ["Jobs in store", s.jobs_created, "#/jobs"],
      ["Completed", s.jobs_completed, hashFor("/jobs", { status: "completed" })],
      ["Failed", s.jobs_failed, hashFor("/jobs", { status: "failed" })],
      ["Retries", s.jobs_retried, null],
      ["Dead letters", s.jobs_dead_lettered, "#/dlq"],
      ["Workflows", s.workflows_created, "#/workflows"],
      ["Workflows completed", s.workflows_completed, hashFor("/workflows", { status: "completed" })],
      ["Workflows failed", s.workflows_failed, hashFor("/workflows", { status: "failed" })],
    ];
    el.innerHTML = String(
      html`<div class="ledger">${cells.map(
        ([l, n, href]) =>
          html`<div><span class="n">${num(n)}</span>${href ? html`<a class="l" href="${href}">${l}</a>` : html`<span class="l">${l}</span>`}</div>`,
      )}</div>`,
    );
  }

  // The periodic refresh is skipped while the tab is hidden (nobody is
  // looking); the first load and a resume always run.
  async function tick(force) {
    if (!ctx.alive() || paused || (document.hidden && !force)) return;
    try {
      const [q, s] = await Promise.all([api.get("/queues"), api.get("/stats")]);
      if (!ctx.alive()) return;
      lastOk = new Date().toISOString();
      renderQueues(q.queues || []);
      renderTotals(s);
      setUpdated(false);
    } catch (e) {
      if (!ctx.alive() || (e instanceof api.ApiError && e.status === 401)) return;
      if (!lastOk) {
        const el = document.getElementById("ov-queues");
        if (el) el.innerHTML = String(errorState("Could not load queues", e, null));
        const t = document.getElementById("ov-totals");
        if (t) t.innerHTML = String(errorState("Could not load totals", e, null));
      }
      setUpdated(true);
    }
  }

  await tick(true);
  const timer = setInterval(() => tick(false), OVERVIEW_REFRESH_MS);
  ctx.cleanups.push(() => clearInterval(timer));
  const onVisible = () => {
    if (!document.hidden) tick(true);
  };
  document.addEventListener("visibilitychange", onVisible);
  ctx.cleanups.push(() => document.removeEventListener("visibilitychange", onVisible));
}

// ---- Jobs -------------------------------------------------------------------------

const JOB_STATUSES = ["pending", "running", "retrying", "completed", "failed", "cancelled"];

function statusSelect(name, options, value) {
  return html`<select name="${name}"><option value="">Any status</option>${options.map(
    (s) => html`<option value="${s}"${s === value ? " selected" : ""}>${statusLabel(s)}</option>`,
  )}</select>`;
}

async function jobsList(ctx) {
  const keys = ["status", "queue", "created_after", "created_before", "cursor"];
  const f = filtersFrom(ctx.query, keys);
  const { cursor, ...filters } = f;
  ctx.set(html`${viewHead("Jobs", html`<span>Newest first</span>`)}
  <form class="filters" id="job-filters">
    <label>Status ${statusSelect("status", JOB_STATUSES, filters.status || "")}</label>
    <label>Queue <input type="text" name="queue" value="${filters.queue || ""}" placeholder="any queue"></label>
    <label>Created after <input type="datetime-local" name="created_after" value="${isoToLocal(filters.created_after)}"></label>
    <label>Created before <input type="datetime-local" name="created_before" value="${isoToLocal(filters.created_before)}"></label>
    <button type="submit" class="btn">Apply filters</button>
    ${Object.keys(filters).length ? html`<a href="#/jobs" class="btn">Clear</a>` : ""}
  </form>
  <section id="job-list">${loadingState("jobs")}</section>`);

  document.getElementById("job-filters").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const fd = new FormData(ev.target);
    location.hash = hashFor("/jobs", {
      status: fd.get("status"),
      queue: String(fd.get("queue") || "").trim(),
      created_after: localToIso(fd.get("created_after")),
      created_before: localToIso(fd.get("created_before")),
    });
  });

  const resp = await api.get("/jobs", { ...filters, cursor, limit: PAGE_SIZE });
  if (!ctx.alive()) return;
  const el = document.getElementById("job-list");
  if (!resp.jobs.length) {
    el.innerHTML = String(
      emptyState(
        Object.keys(filters).length || cursor ? "No jobs match these filters" : "No jobs yet",
        Object.keys(filters).length || cursor
          ? "Widen the time range or clear a filter."
          : "Enqueue one with POST /api/v1/jobs and it will show up here.",
      ),
    );
    return;
  }
  const rows = resp.jobs.map(
    (j) => html`<tr>
      <td>${status(j.status)}</td>
      <td><a href="#/jobs/${enc(j.id)}">${j.task_name}</a></td>
      <td>${j.queue_name}</td>
      <td class="num">${j.retry_count}<span class="zero"> / ${j.config ? j.config.max_retries : ""}</span></td>
      <td>${time(j.created_at)}</td>
      <td>${j.scheduled_at && Date.parse(j.scheduled_at) > Date.now() + 1000 ? time(j.scheduled_at) : html`<span class="zero">now</span>`}</td>
      <td>${j.workflow_id ? html`<a class="id" href="#/workflows/${enc(j.workflow_id)}" title="${j.workflow_id}">${j.workflow_step_id || shortId(j.workflow_id)}</a>` : html`<span class="zero">none</span>`}</td>
      <td><a class="id" href="#/jobs/${enc(j.id)}" title="${j.id}">${shortId(j.id)}</a></td>
    </tr>`,
  );
  el.innerHTML = String(
    html`${table(["Status", "Task", "Queue", { label: "Retries", num: true }, "Created", "Runs", "Workflow", "Id"], rows)}
    ${pager(resp, "/jobs", filters, cursor)}`,
  );
}

function backoffLabel(c) {
  if (!c) return "";
  const b = c.retry_backoff || "exponential";
  const jitter = c.jitter_factor ? `, jitter ${Math.round(c.jitter_factor * 100)}%` : "";
  return `${b}, base ${duration(c.retry_delay_secs)}, cap ${duration(c.retry_max_delay_secs)}${jitter}`;
}

async function jobDetail(ctx, id) {
  ctx.set(html`${crumbs("jobs", "Jobs")}${loadingState("job")}`);
  let job = await api.get(`/jobs/${enc(id)}`);
  if (!ctx.alive()) return;

  let mode = JOB_TERMINAL.has(job.status) ? "done" : "connecting";

  function render() {
    const c = job.config || {};
    const liveLabel = {
      connecting: ["Connecting", false],
      live: ["Live", true],
      polling: ["Polling every 3s", true],
      done: ["Finished", false],
    }[mode];
    document.title = `${job.task_name} | QueueFlow`;
    ctx.set(html`${crumbs("jobs", "Jobs")}
    <div class="view-head">
      <div class="title-row"><h1>${job.task_name}</h1>${status(job.status)}<span class="mono">${job.id}</span></div>
      <div class="meta"><span class="live" data-on="${liveLabel[1]}">${liveLabel[0]}</span></div>
    </div>
    <div class="detail">
      <div class="stack">
        ${dl([
          ["Queue", html`<a href="${hashFor("/jobs", { queue: job.queue_name })}">${job.queue_name}</a>`],
          ["Status", status(job.status)],
          ["Created", time(job.created_at)],
          ["Runs at", time(job.scheduled_at)],
          ["Started", time(job.started_at)],
          ["Completed", time(job.completed_at)],
          ["Retries", html`${job.retry_count} of ${c.max_retries ?? ""}`],
          ["Deliveries", html`${job.delivery_count ?? 0}${(job.delivery_count ?? 0) > job.retry_count + 1 ? html` <span class="zero">(a lease expired without a report)</span>` : ""}`],
          ["Next retry", time(job.next_retry_at)],
          job.workflow_id
            ? ["Workflow", html`<a href="#/workflows/${enc(job.workflow_id)}">${shortId(job.workflow_id)}</a>${job.workflow_step_id ? html` <span class="zero">step</span> ${job.workflow_step_id}` : ""}`]
            : null,
          ["Priority", String(c.priority ?? 0)],
          ["Timeout", duration(c.timeout_secs)],
          ["Backoff", backoffLabel(c)],
          job.idempotency_key ? ["Idempotency key", html`<span class="mono">${job.idempotency_key}</span>`] : null,
          tenantFact(job),
        ])}
      </div>
      <div class="stack">
        ${job.error_message ? html`<section><h2>Error</h2><pre class="panel err">${job.error_message}</pre></section>` : ""}
        <section><h2>Payload</h2>${json(job.payload)}</section>
        <section><h2>Result</h2>${job.result == null ? html`<p class="note">No result recorded${JOB_TERMINAL.has(job.status) ? "" : " yet"}.</p>` : json(job.result)}</section>
        <section><h2>Config</h2>${json(job.config)}</section>
        ${isEmptyObject(job.metadata) ? "" : html`<section><h2>Metadata</h2>${json(job.metadata)}</section>`}
      </div>
    </div>`);
  }

  render();
  if (mode === "done") return;

  // Live updates: SSE over fetch, then polling if the stream is unavailable.
  const ac = new AbortController();
  ctx.cleanups.push(() => ac.abort());
  let pollTimer = null;
  ctx.cleanups.push(() => clearInterval(pollTimer));

  const onJob = (j) => {
    job = j;
    if (JOB_TERMINAL.has(job.status)) mode = "done";
    render();
  };

  function startPolling() {
    if (!ctx.alive() || mode === "done") return;
    mode = "polling";
    render();
    pollTimer = setInterval(async () => {
      try {
        const j = await api.get(`/jobs/${enc(id)}`, null, { signal: ac.signal });
        if (!ctx.alive()) return;
        onJob(j);
        if (mode === "done") clearInterval(pollTimer);
      } catch (e) {
        if (!ctx.alive() || (e && e.name === "AbortError")) clearInterval(pollTimer);
      }
    }, POLL_MS);
  }

  try {
    await api.streamJob(
      id,
      (j) => {
        if (mode !== "done") mode = "live";
        onJob(j);
      },
      ac.signal,
    );
    // Stream closed. If the job is still running, the server's stream
    // lifetime ran out: keep following by polling.
    if (ctx.alive() && mode !== "done") startPolling();
  } catch (e) {
    if (!ctx.alive() || (e && e.name === "AbortError")) return;
    if (e instanceof api.ApiError && e.status === 401) return;
    startPolling();
  }
}

// ---- Workflows ----------------------------------------------------------------------

const WORKFLOW_STATUSES = ["created", "running", "completed", "failed", "partially_failed", "cancelled"];

async function workflowsList(ctx) {
  const f = filtersFrom(ctx.query, ["status", "cursor"]);
  const { cursor, ...filters } = f;
  ctx.set(html`${viewHead("Workflows", html`<span>Newest first</span>`)}
  <form class="filters" id="wf-filters">
    <label>Status ${statusSelect("status", WORKFLOW_STATUSES, filters.status || "")}</label>
    <button type="submit" class="btn">Apply filter</button>
    ${filters.status ? html`<a href="#/workflows" class="btn">Clear</a>` : ""}
  </form>
  <section id="wf-list">${loadingState("workflows")}</section>`);
  document.getElementById("wf-filters").addEventListener("submit", (ev) => {
    ev.preventDefault();
    location.hash = hashFor("/workflows", { status: new FormData(ev.target).get("status") });
  });

  const resp = await api.get("/workflows", { ...filters, cursor, limit: PAGE_SIZE });
  if (!ctx.alive()) return;
  const el = document.getElementById("wf-list");
  if (!resp.workflows.length) {
    el.innerHTML = String(
      emptyState(
        filters.status || cursor ? "No workflows match this filter" : "No workflows yet",
        filters.status || cursor ? "Clear the status filter to see every workflow." : "Create one with POST /api/v1/workflows and it will show up here.",
      ),
    );
    return;
  }
  const rows = resp.workflows.map(
    (w) => html`<tr>
      <td>${status(w.status)}</td>
      <td><a href="#/workflows/${enc(w.id)}">${w.name}</a></td>
      <td class="num">${w.steps ? w.steps.length : 0}</td>
      <td>${time(w.created_at)}</td>
      <td>${time(w.started_at)}</td>
      <td>${time(w.completed_at)}</td>
      <td><a class="id" href="#/workflows/${enc(w.id)}" title="${w.id}">${shortId(w.id)}</a></td>
    </tr>`,
  );
  el.innerHTML = String(
    html`${table(["Status", "Name", { label: "Steps", num: true }, "Created", "Started", "Completed", "Id"], rows)}
    ${pager(resp, "/workflows", filters, cursor)}`,
  );
}

// Mirror of the server's node-id sanitizer (workflow/dag.rs): ASCII
// alphanumerics pass through, anything else becomes "_", and an id that
// would start with a digit (or be empty) gets an "n" prefix.
function mermaidNodeId(name) {
  let s = Array.from(name, (ch) => (/^[A-Za-z0-9]$/.test(ch) ? ch : "_")).join("");
  if (!s || /^[0-9]/.test(s)) s = `n${s}`;
  return s;
}

let mermaidLoad = null;
function loadMermaid() {
  if (!mermaidLoad) {
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error("timed out")), 10000));
    mermaidLoad = Promise.race([import(MERMAID_URL), timeout])
      .then((m) => m.default)
      .catch((e) => {
        mermaidLoad = null;
        throw e;
      });
  }
  return mermaidLoad;
}

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

const STEP_STATUSES = ["pending", "running", "completed", "failed", "cancelled", "skipped"];

function decorateDiagram(source, steps) {
  const ink = cssVar("--ink");
  const lines = [source.trimEnd()];
  for (const s of STEP_STATUSES) {
    const dash = s === "skipped" || s === "cancelled" ? ",stroke-dasharray:4 3" : "";
    lines.push(`classDef ${s} fill:${cssVar(`--st-${s}-soft`)},stroke:${cssVar(`--st-${s}`)},color:${ink}${dash}`);
  }
  const byStatus = new Map();
  for (const st of steps) {
    const list = byStatus.get(st.status) || [];
    list.push(mermaidNodeId(st.name));
    byStatus.set(st.status, list);
  }
  for (const [st, ids] of byStatus) lines.push(`class ${ids.join(",")} ${st}`);
  return lines.join("\n");
}

let diagramSeq = 0;

async function renderDiagram(el, source, steps) {
  if (!el) return;
  el.innerHTML = String(html`<div class="state">Rendering diagram</div>`);
  let mermaid;
  try {
    mermaid = await loadMermaid();
  } catch (e) {
    if (!el.isConnected) return;
    el.innerHTML = String(html`<div class="state">
        <strong>Diagram library unavailable</strong>
        mermaid could not be loaded from cdn.jsdelivr.net. The Mermaid source is shown instead.
        <div><button type="button" class="btn" data-diagram-retry>Try again</button></div>
      </div>
      <pre class="json" style="border-top:1px solid var(--rule-soft)">${source}</pre>`);
    return;
  }
  try {
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: "strict",
      theme: "base",
      fontFamily: cssVar("--sans"),
      themeVariables: {
        primaryColor: cssVar("--surface-2"),
        primaryTextColor: cssVar("--ink"),
        primaryBorderColor: cssVar("--rule"),
        lineColor: cssVar("--muted"),
        fontSize: "13px",
      },
    });
    const { svg } = await mermaid.render(`qf-dag-${++diagramSeq}`, decorateDiagram(source, steps));
    if (!el.isConnected) return;
    el.innerHTML = svg;
    const svgEl = el.querySelector("svg");
    if (svgEl) {
      svgEl.setAttribute("role", "img");
      svgEl.setAttribute("aria-label", "Workflow dependency graph; step states are listed in the table below");
    }
  } catch (e) {
    if (!el.isConnected) return;
    el.innerHTML = String(html`<div class="state error"><strong>Diagram could not be rendered</strong>${e && e.message ? e.message : String(e)}</div>
      <pre class="json" style="border-top:1px solid var(--rule-soft)">${source}</pre>`);
  }
}

async function workflowDetail(ctx, id) {
  ctx.set(html`${crumbs("workflows", "Workflows")}${loadingState("workflow")}`);
  let [wf, stepsResp, diagram] = await Promise.all([
    api.get(`/workflows/${enc(id)}`),
    api.get(`/workflows/${enc(id)}/steps`),
    api.get(`/workflows/${enc(id)}/diagram`).catch((e) => ({ error: e })),
  ]);
  if (!ctx.alive()) return;

  let steps = stepsResp.steps || [];
  let live = !WORKFLOW_TERMINAL.has(wf.status);

  function stepRows() {
    const byName = new Map(steps.map((s) => [s.name, s]));
    return wf.steps.map((def) => {
      const st = byName.get(def.name) || {};
      return html`<tr>
        <td>${st.status ? status(st.status) : html`<span class="zero">unknown</span>`}</td>
        <td>${def.name}</td>
        <td>${def.task_name}</td>
        <td class="wrap">${def.depends_on && def.depends_on.length ? def.depends_on.join(", ") : html`<span class="zero">none</span>`}</td>
        <td>${def.on_failure || "halt"}</td>
        <td>${st.job_id ? html`<a class="id" href="#/jobs/${enc(st.job_id)}" title="${st.job_id}">${shortId(st.job_id)}</a>` : html`<span class="zero">not scheduled</span>`}</td>
      </tr>`;
    });
  }

  function render() {
    document.title = `${wf.name} | QueueFlow`;
    ctx.set(html`${crumbs("workflows", "Workflows")}
    <div class="view-head">
      <div class="title-row"><h1>${wf.name}</h1>${status(wf.status)}<span class="mono">${wf.id}</span></div>
      <div class="meta"><span class="live" data-on="${live}">${live ? "Following every 3s" : "Finished"}</span></div>
    </div>
    <div class="detail">
      <div class="stack">
        ${dl([
          ["Status", status(wf.status)],
          ["Steps", String(wf.steps.length)],
          ["Created", time(wf.created_at)],
          ["Started", time(wf.started_at)],
          ["Completed", time(wf.completed_at)],
          tenantFact(wf),
        ])}
        <section><h2>Context</h2>${isEmptyObject(wf.context) ? html`<p class="note">Empty. Step results are merged here as they complete.</p>` : json(wf.context)}</section>
        ${isEmptyObject(wf.metadata) ? "" : html`<section><h2>Metadata</h2>${json(wf.metadata)}</section>`}
      </div>
      <div class="stack">
        <section>
          <h2>Dependency graph</h2>
          <div class="panel">
            <div class="diagram" id="wf-diagram"></div>
            <div class="legend">${STEP_STATUSES.map((s) => status(s))}</div>
          </div>
        </section>
        <section>
          <h2>Steps</h2>
          ${table(["Status", "Step", "Task", "Depends on", "On failure", "Job"], stepRows())}
        </section>
      </div>
    </div>`);
    const dEl = document.getElementById("wf-diagram");
    if (diagram && diagram.diagram) {
      renderDiagram(dEl, diagram.diagram, steps);
      dEl.addEventListener("click", (ev) => {
        if (ev.target.closest("[data-diagram-retry]")) renderDiagram(dEl, diagram.diagram, steps);
      });
    } else {
      dEl.innerHTML = String(errorState("Diagram unavailable", diagram && diagram.error ? diagram.error : "No diagram returned", null));
    }
  }

  render();
  if (!live) return;

  const timer = setInterval(async () => {
    try {
      const [w, s] = await Promise.all([api.get(`/workflows/${enc(id)}`), api.get(`/workflows/${enc(id)}/steps`)]);
      if (!ctx.alive()) return;
      const changed = w.status !== wf.status || JSON.stringify(s.steps) !== JSON.stringify(steps) || JSON.stringify(w.context) !== JSON.stringify(wf.context);
      wf = w;
      steps = s.steps || [];
      if (WORKFLOW_TERMINAL.has(wf.status)) {
        live = false;
        clearInterval(timer);
      }
      if (changed || !live) render();
    } catch {
      // transient; try again on the next tick
    }
  }, POLL_MS);
  ctx.cleanups.push(() => clearInterval(timer));
}

// ---- Dead letters ---------------------------------------------------------------------

async function dlqList(ctx) {
  const cursor = ctx.query.get("cursor") || "";
  ctx.set(html`${viewHead("Dead letters", html`<span>Newest first</span>`)}
  <p class="note">Jobs that failed for good: retries exhausted, a permanent failure, or no handler for the task. Replay them with POST /api/v1/dlq/{id}/replay.</p>
  <section id="dlq-list">${loadingState("dead letters")}</section>`);
  const resp = await api.get("/dlq", { cursor, limit: PAGE_SIZE });
  if (!ctx.alive()) return;
  const el = document.getElementById("dlq-list");
  if (!resp.dead_letters.length) {
    el.innerHTML = String(emptyState("No dead letters", "Nothing has failed terminally for this tenant."));
    return;
  }
  const rows = resp.dead_letters.map(
    (d) => html`<tr>
      <td><a href="#/dlq/${d.id}">${dlqReason(d.reason)}</a></td>
      <td>${d.task_name || html`<span class="zero">unknown</span>`}</td>
      <td>${d.queue_name || html`<span class="zero">unknown</span>`}</td>
      <td class="trunc" title="${d.error_message || ""}">${d.error_message || html`<span class="zero">no message</span>`}</td>
      <td>${time(d.created_at)}</td>
      <td>${d.replayed_at ? html`${time(d.replayed_at)}${d.replay_job_id ? html` <a class="id" href="#/jobs/${enc(d.replay_job_id)}">${shortId(d.replay_job_id)}</a>` : ""}` : html`<span class="zero">no</span>`}</td>
      <td><a class="id" href="#/jobs/${enc(d.job_id)}" title="${d.job_id}">${shortId(d.job_id)}</a></td>
    </tr>`,
  );
  el.innerHTML = String(
    html`${table(["Reason", "Task", "Queue", "Error", "Dead-lettered", "Replayed", "Original job"], rows)}
    ${pager(resp, "/dlq", {}, cursor)}`,
  );
}

async function dlqDetail(ctx, id) {
  ctx.set(html`${crumbs("dlq", "Dead letters")}${loadingState("dead letter")}`);
  const d = await api.get(`/dlq/${enc(id)}`);
  if (!ctx.alive()) return;
  document.title = `Dead letter ${d.id} | QueueFlow`;
  ctx.set(html`${crumbs("dlq", "Dead letters")}
  <div class="view-head">
    <div class="title-row"><h1>${dlqReason(d.reason)}</h1>${status("failed")}<span class="mono">dead letter ${d.id}</span></div>
  </div>
  <div class="detail">
    <div class="stack">
      ${dl([
        ["Reason", html`${dlqReason(d.reason)} <span class="zero mono">${d.reason}</span>`],
        ["Original job", html`<a class="id" href="#/jobs/${enc(d.job_id)}">${d.job_id}</a>`],
        ["Task", d.task_name || html`<span class="zero">unknown</span>`],
        ["Queue", d.queue_name ? html`<a href="${hashFor("/jobs", { queue: d.queue_name })}">${d.queue_name}</a>` : html`<span class="zero">unknown</span>`],
        ["Dead-lettered", time(d.created_at)],
        ["Replayed", d.replayed_at ? time(d.replayed_at) : html`<span class="zero">not yet</span>`],
        d.replay_job_id ? ["Replay job", html`<a class="id" href="#/jobs/${enc(d.replay_job_id)}">${d.replay_job_id}</a>`] : null,
        tenantFact(d),
      ])}
    </div>
    <div class="stack">
      <section><h2>Error</h2>${d.error_message ? html`<pre class="panel err">${d.error_message}</pre>` : html`<p class="note">No error message was recorded.</p>`}</section>
      <p class="note">The original job keeps its payload, result, and attempt history. Open it for the full record.</p>
    </div>
  </div>`);
}

// ---- Cron -----------------------------------------------------------------------------

async function cronList(ctx) {
  const cursor = ctx.query.get("cursor") || "";
  ctx.set(html`${viewHead("Cron", html`<span>Schedules evaluated in UTC</span>`)}
  <section id="cron-list">${loadingState("schedules")}</section>`);
  const resp = await api.get("/cron", { cursor, limit: PAGE_SIZE });
  if (!ctx.alive()) return;
  const el = document.getElementById("cron-list");
  if (!resp.crons.length) {
    el.innerHTML = String(emptyState("No cron schedules", "Create one with POST /api/v1/cron and it will show up here."));
    return;
  }
  const rows = resp.crons.map(
    (c) => html`<tr>
      <td>${c.enabled ? html`<span class="status" data-status="running" style="--st-color: var(--st-completed); --st-soft: var(--st-completed-soft)">Enabled</span>` : html`<span class="status" data-status="cancelled">Paused</span>`}</td>
      <td>${c.name}</td>
      <td><code>${c.cron_expr}</code></td>
      <td>${c.task_name}</td>
      <td>${c.queue_name || html`<span class="zero">server default</span>`}</td>
      <td>${c.enabled ? time(c.next_run_at) : html`<span class="zero">paused</span>`}</td>
      <td>${c.last_enqueued_at ? time(c.last_enqueued_at) : html`<span class="zero">never</span>`}</td>
      <td class="mono" title="${c.id}">${shortId(c.id)}</td>
    </tr>`,
  );
  el.innerHTML = String(
    html`${table(["State", "Name", "Schedule", "Task", "Queue", "Next run", "Last enqueued", "Id"], rows)}
    ${pager(resp, "/cron", {}, cursor)}`,
  );
}

// ---- Tasks ----------------------------------------------------------------------------

async function tasksList(ctx) {
  ctx.set(html`${viewHead("Tasks")}
  <p class="note">Handlers compiled into this server. Remote workers that lease jobs over HTTP are not listed here.</p>
  <section id="task-list">${loadingState("tasks")}</section>`);
  const resp = await api.get("/tasks");
  if (!ctx.alive()) return;
  const el = document.getElementById("task-list");
  const tasks = (resp.tasks || []).slice().sort();
  if (!tasks.length) {
    el.innerHTML = String(emptyState("No in-process handlers", "This server runs no handlers of its own; jobs are executed by remote workers."));
    return;
  }
  el.innerHTML = String(table(["Task"], tasks.map((t) => html`<tr><td><code>${t}</code></td></tr>`)));
}

// ---- Boot -------------------------------------------------------------------------------

route();
