// Same-origin API client. The tenant token lives in sessionStorage only:
// it is per tab and gone when the tab closes.

const TOKEN_KEY = "queueflow.token";
const BASE = "/api/v1";

export const auth = {
  get() {
    try {
      return sessionStorage.getItem(TOKEN_KEY) || "";
    } catch {
      return "";
    }
  },
  set(token) {
    try {
      sessionStorage.setItem(TOKEN_KEY, token);
    } catch {
      // Session storage unavailable (private mode quirks); the token is
      // then held only for this page load.
      memoryToken = token;
    }
  },
  clear() {
    memoryToken = "";
    try {
      sessionStorage.removeItem(TOKEN_KEY);
    } catch {
      // nothing to clear
    }
  },
};

let memoryToken = "";

function token() {
  return auth.get() || memoryToken;
}

export class ApiError extends Error {
  constructor(status, message, body) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

/** Fired on window when a request comes back 401. */
export const UNAUTHORIZED_EVENT = "queueflow:unauthorized";

function headers() {
  return { Authorization: `Bearer ${token()}`, Accept: "application/json" };
}

export function buildQuery(params) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null || v === "") continue;
    q.set(k, String(v));
  }
  const s = q.toString();
  return s ? `?${s}` : "";
}

/** GET a JSON resource under /api/v1. Throws ApiError on non-2xx. */
export async function get(path, params, { signal } = {}) {
  let resp;
  try {
    resp = await fetch(BASE + path + buildQuery(params), { headers: headers(), signal });
  } catch (e) {
    if (e && e.name === "AbortError") throw e;
    throw new ApiError(0, "The server could not be reached.", null);
  }
  if (resp.status === 401) {
    window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT, { detail: { path } }));
    throw new ApiError(401, "The token was rejected.", null);
  }
  let body = null;
  const text = await resp.text();
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = null;
    }
  }
  if (!resp.ok) {
    const msg = (body && body.error) || `${resp.status} ${resp.statusText}`;
    throw new ApiError(resp.status, msg, body);
  }
  return body;
}

/**
 * Subscribe to a job's SSE stream with fetch (EventSource cannot send an
 * Authorization header). Calls onJob(job) for every `status` event. Resolves
 * when the stream ends; rejects if streaming is unavailable so the caller
 * can fall back to polling.
 */
export async function streamJob(id, onJob, signal) {
  const resp = await fetch(`${BASE}/jobs/${encodeURIComponent(id)}/events`, {
    headers: { Authorization: `Bearer ${token()}`, Accept: "text/event-stream" },
    signal,
  });
  if (resp.status === 401) {
    window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT, { detail: { path: "events" } }));
    throw new ApiError(401, "The token was rejected.", null);
  }
  if (!resp.ok || !resp.body) {
    throw new ApiError(resp.status, "Streaming unavailable", null);
  }
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let eventName = "";
  let data = [];
  const flush = () => {
    if (data.length) {
      const payload = data.join("\n");
      if (eventName === "status" || eventName === "") {
        try {
          onJob(JSON.parse(payload));
        } catch {
          // keep-alive or malformed frame: ignore
        }
      }
    }
    eventName = "";
    data = [];
  };
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let nl;
    while ((nl = buffer.indexOf("\n")) >= 0) {
      let line = buffer.slice(0, nl);
      buffer = buffer.slice(nl + 1);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      if (line === "") {
        flush();
      } else if (line.startsWith(":")) {
        // comment / keep-alive
      } else if (line.startsWith("event:")) {
        eventName = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        data.push(line.slice(5).replace(/^ /, ""));
      }
    }
  }
  flush();
}
