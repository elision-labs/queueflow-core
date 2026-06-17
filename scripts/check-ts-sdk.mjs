#!/usr/bin/env node
//
// Drift guard for the TypeScript SDK (../queueflow-sdk-nodejs).
//
// The SDK is now a GENERATED CORE (core/, models + per-tag API clients + fetch
// runtime, produced from spec/openapi.json) plus a thin HAND-WRITTEN ergonomic
// facade (src/: waitFor, watch, the worker loop, the workflow builder, typed
// errors). That split changes what can drift:
//
//   * The wire types and every operation are covered by the generated core BY
//     CONSTRUCTION, so the old "is every path reachable from hand-written code"
//     heuristic is no longer needed for them.
//   * Two new things CAN drift and are checked here:
//       1. Spec -> core:   the core was not regenerated after a spec change, so
//          a spec operation has no generated method.  (`npm run generate-core`)
//       2. Core -> facade: a whole tag/resource group exists in the spec but is
//          not surfaced by the hand-written facade (e.g. a new top-level area).
//
// Dependency-free (plain Node) so it runs in CI without the SDK's toolchain.
//
// Exit codes: 0 = in sync, 1 = drift found, 2 = bad invocation / missing inputs.

import { readFileSync, readdirSync, existsSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SPEC = join(ROOT, "spec", "openapi.json");
// CI checks the SDK out at an arbitrary path; QUEUEFLOW_SDK_DIR overrides the
// default sibling layout used for local development.
const SDK = process.env.QUEUEFLOW_SDK_DIR
  ? resolve(process.env.QUEUEFLOW_SDK_DIR)
  : join(ROOT, "..", "queueflow-sdk-nodejs");
const CORE_APIS = join(SDK, "core", "src", "apis");
const SDK_SRC = join(SDK, "src");

function fail(msg, code = 2) {
  console.error(`check-ts-sdk: ${msg}`);
  process.exit(code);
}

if (!existsSync(SPEC)) fail(`spec not found: ${SPEC} (run 'make spec')`);
if (!existsSync(CORE_APIS)) {
  fail(
    `generated core not found: ${CORE_APIS}\n` +
      `Generate it: (cd ${SDK} && npm run generate-core)`,
  );
}
if (!existsSync(SDK_SRC)) fail(`facade source not found: ${SDK_SRC}`);

const spec = JSON.parse(readFileSync(SPEC, "utf8"));

function concatTs(dir) {
  return readdirSync(dir)
    .filter((f) => f.endsWith(".ts"))
    .map((f) => readFileSync(join(dir, f), "utf8"))
    .join("\n");
}

const coreSrc = concatTs(CORE_APIS);
const facadeSrc = (function walk(dir) {
  let out = "";
  for (const e of readdirSync(dir)) {
    const p = join(dir, e);
    if (statSync(p).isDirectory()) out += walk(p);
    else if (e.endsWith(".ts")) out += "\n" + readFileSync(p, "utf8");
  }
  return out;
})(SDK_SRC);

// --- Check 1: every spec operation has a generated core method ----------------
const operations = [];
const tags = new Set();
for (const item of Object.values(spec.paths ?? {})) {
  for (const [method, op] of Object.entries(item)) {
    if (!["get", "post", "put", "patch", "delete"].includes(method)) continue;
    if (op.operationId) operations.push(op.operationId);
    for (const t of op.tags ?? []) tags.add(t);
  }
}

const missingOps = operations.filter(
  (op) => !new RegExp(`\\b${op}\\s*\\(`).test(coreSrc),
);

// --- Check 2: every tag/resource group is surfaced by the facade --------------
// A tag `jobs` is served by the generated `JobsApi`; assert the facade imports
// or instantiates each one (so a new top-level area can't be silently dropped).
const apiClassFor = (tag) => tag.charAt(0).toUpperCase() + tag.slice(1) + "Api";
const missingGroups = [...tags]
  .map(apiClassFor)
  .filter((cls) => !facadeSrc.includes(cls));

console.log(
  `check-ts-sdk: ${operations.length} spec operations, ` +
    `${operations.length - missingOps.length} in the generated core; ` +
    `${tags.size} tag groups, ${tags.size - missingGroups.length} surfaced by the facade.`,
);

let drift = false;
if (missingOps.length) {
  drift = true;
  console.error("\nSpec operations MISSING from the generated core (regenerate it):");
  for (const op of missingOps) console.error(`  ✗ ${op}`);
  console.error(`\nRun: (cd ${SDK} && npm run generate-core)`);
}
if (missingGroups.length) {
  drift = true;
  console.error("\nTag groups NOT surfaced by the hand-written facade:");
  for (const cls of missingGroups) console.error(`  ✗ ${cls}`);
  console.error("\nAdd a resource for these in ../queueflow-sdk-nodejs/src/client.ts.");
}

if (drift) process.exit(1);
console.log("check-ts-sdk: ✓ generated core matches the spec and the facade surfaces every group.");
