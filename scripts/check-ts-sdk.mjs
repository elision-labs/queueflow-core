#!/usr/bin/env node
//
// Drift guard for the HAND-WRITTEN TypeScript SDK (../queueflow-sdk-nodejs).
//
// The other SDKs (python/rust/go) are generated from spec/openapi.json, so they
// can never drift. The TS SDK is hand-written for ergonomics, which means nothing
// otherwise stops it from falling behind the spec. This check asserts that every
// operation (METHOD + path) in the current OpenAPI spec is reachable from the
// hand-written client source — catching the common drift where the engine gains,
// drops, or renames an endpoint and the SDK is not updated to match.
//
// It is intentionally dependency-free (plain Node, no npm install) so it runs
// anywhere, including CI, without touching the SDK's own toolchain.
//
// Exit codes: 0 = in sync, 1 = drift found, 2 = bad invocation / missing inputs.

import { readFileSync, readdirSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SPEC = join(ROOT, "spec", "openapi.json");
const SDK_SRC = join(ROOT, "..", "queueflow-sdk-nodejs", "src");

function fail(msg, code = 2) {
  console.error(`check-ts-sdk: ${msg}`);
  process.exit(code);
}

if (!existsSync(SPEC)) fail(`spec not found: ${SPEC} (run 'make spec')`);
if (!existsSync(SDK_SRC)) fail(`SDK source not found: ${SDK_SRC}`);

const spec = JSON.parse(readFileSync(SPEC, "utf8"));

// All .ts source of the hand-written client, concatenated.
const src = readdirSync(SDK_SRC)
  .filter((f) => f.endsWith(".ts"))
  .map((f) => readFileSync(join(SDK_SRC, f), "utf8"))
  .join("\n");

// Build a matcher for one spec path. A `{param}` placeholder is written in the
// hand-written client as a `${...}` template expression (e.g.
// `/api/v1/jobs/${encodeURIComponent(id)}`), so a placeholder matches either a
// `${...}` interpolation or a literal `{param}`.
function pathToRegex(p) {
  const esc = p.replace(/[.*+?^$()|[\]\\]/g, "\\$&"); // escape regex metachars (not {})
  const withParams = esc.replace(/\{[^}]+\}/g, "(?:\\$\\{[^}]*\\}|\\{[^}]+\\})");
  return new RegExp(withParams);
}

const operations = [];
for (const [path, item] of Object.entries(spec.paths ?? {})) {
  for (const method of Object.keys(item)) {
    if (["get", "post", "put", "patch", "delete"].includes(method)) {
      operations.push({ method: method.toUpperCase(), path });
    }
  }
}

const missing = operations.filter(({ path }) => !pathToRegex(path).test(src));

console.log(
  `check-ts-sdk: ${operations.length} spec operations, ` +
    `${operations.length - missing.length} covered by ../queueflow-sdk-nodejs.`,
);

if (missing.length) {
  console.error("\nSpec operations NOT found in the hand-written SDK (drift):");
  for (const { method, path } of missing) console.error(`  ✗ ${method} ${path}`);
  console.error(
    "\nUpdate ../queueflow-sdk-nodejs/src to cover these, or remove them from the engine.",
  );
  process.exit(1);
}

console.log("check-ts-sdk: ✓ hand-written SDK is in sync with the spec.");
