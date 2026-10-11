// Formatting helpers: HTML escaping, times, durations, status chips, JSON.

export function esc(v) {
  return String(v ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** Tagged template that escapes interpolations unless wrapped in raw(). */
export function html(strings, ...values) {
  let out = "";
  strings.forEach((s, i) => {
    out += s;
    if (i < values.length) {
      const v = values[i];
      if (v instanceof Raw) out += v.s;
      else if (Array.isArray(v)) out += v.map((x) => (x instanceof Raw ? x.s : esc(x))).join("");
      else out += esc(v);
    }
  });
  return new Raw(out);
}

class Raw {
  constructor(s) {
    this.s = s;
  }
  toString() {
    return this.s;
  }
}

export function raw(s) {
  return new Raw(String(s));
}

/** 0 -> "0s", 95 -> "1m 35s", 4000 -> "1h 6m", 200000 -> "2d 7h". */
export function duration(secs) {
  if (secs == null || !isFinite(secs)) return "";
  secs = Math.max(0, Math.round(secs));
  if (secs < 60) return `${secs}s`;
  const parts = [];
  let rest = secs;
  const divs = [86400, 3600, 60, 1];
  const names = ["d", "h", "m", "s"];
  for (let i = 0; i < divs.length; i++) {
    const n = Math.floor(rest / divs[i]);
    rest -= n * divs[i];
    if (n > 0) parts.push(`${n}${names[i]}`);
    if (parts.length === 2) break;
  }
  return parts.join(" ");
}

export function relative(iso, now = Date.now()) {
  if (!iso) return "";
  const t = Date.parse(iso);
  if (isNaN(t)) return iso;
  const diff = (now - t) / 1000;
  const abs = Math.abs(diff);
  if (abs < 5) return "just now";
  const d = duration(abs);
  return diff >= 0 ? `${d} ago` : `in ${d}`;
}

export function exact(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  // Local time with the UTC offset so an operator can match server logs.
  const pad = (n) => String(n).padStart(2, "0");
  const off = -d.getTimezoneOffset();
  const sign = off >= 0 ? "+" : "-";
  const oh = pad(Math.floor(Math.abs(off) / 60));
  const om = pad(Math.abs(off) % 60);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())} ${sign}${oh}:${om}`;
}

/** <time> with the relative form visible and the exact instant on hover. */
export function time(iso) {
  if (!iso) return raw('<span class="zero">none</span>');
  return html`<time datetime="${iso}" title="${exact(iso)} (${iso})">${relative(iso)}</time>`;
}

const STATUS_LABELS = {
  pending: "Pending",
  created: "Created",
  scheduled: "Scheduled",
  running: "Running",
  retrying: "Retrying",
  completed: "Completed",
  failed: "Failed",
  partially_failed: "Partially failed",
  cancelled: "Cancelled",
  skipped: "Skipped",
};

export function statusLabel(s) {
  return STATUS_LABELS[s] || String(s || "unknown");
}

export function status(s) {
  return html`<span class="status" data-status="${s}">${statusLabel(s)}</span>`;
}

export function json(value) {
  if (value === undefined || value === null) return raw('<span class="zero">none</span>');
  let text;
  try {
    text = JSON.stringify(value, null, 2);
  } catch {
    text = String(value);
  }
  return html`<pre class="panel json">${text}</pre>`;
}

export function isEmptyObject(v) {
  return v == null || (typeof v === "object" && !Array.isArray(v) && Object.keys(v).length === 0);
}

export function shortId(id) {
  if (!id) return "";
  const s = String(id);
  return s.length > 13 ? `${s.slice(0, 8)}…${s.slice(-4)}` : s;
}

const DLQ_REASONS = {
  max_attempts_exceeded: "Retries exhausted",
  non_retryable: "Permanent failure",
  handler_not_found: "No handler for task",
};

export function dlqReason(r) {
  return DLQ_REASONS[r] || String(r || "");
}

export function num(n) {
  if (n == null) return "";
  return Number(n).toLocaleString();
}

/** Convert a datetime-local input value to RFC 3339 (UTC). */
export function localToIso(v) {
  if (!v) return "";
  const d = new Date(v);
  return isNaN(d.getTime()) ? "" : d.toISOString();
}

/** Convert RFC 3339 to a datetime-local input value. */
export function isoToLocal(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "";
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
