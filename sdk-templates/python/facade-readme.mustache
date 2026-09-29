# QueueFlow Python facade

This package ships an ergonomic facade (`queueflow.facade`) layered over the generated client. It is
the recommended entry point. The generated `queueflow.api.*` clients and `queueflow.models.*` remain
available for advanced use.

## Quick start

```python
from queueflow.facade import QueueFlow, wf

qf = QueueFlow("http://localhost:8000", "dev")

# Enqueue a job and wait for the result.
job = qf.create_job("echo", payload={"hello": "world"}, max_retries=3)
done = qf.wait_for_job(job.id)
print(done.status, done.result)

# Declare and run a DAG workflow.
dag = (
    wf("etl")
    .step("extract", "echo")
    .step("transform", "echo", after=["extract"])
    .step("load", "echo", after=["transform"])
)
workflow = qf.create_workflow(dag)
finished = qf.wait_for_workflow(workflow.id)
print(finished.status, finished.context)
```

## API

`QueueFlow(base_url, token)` exposes:

- `.jobs` / `.workflows` / `.worker` / `.cron` / `.dlq` / `.system` — the generated api clients, for anything the
  helpers below do not cover.
- `.create_job(task, payload=None, priority=None, max_retries=None, timeout=None, queue=None, idempotency_key=None) -> Job`
- `.wait_for_job(job_id, timeout=60.0, interval=0.5) -> Job`
- `.create_workflow(builder_or_request) -> Workflow`
- `.wait_for_workflow(workflow_id, timeout=60.0, interval=0.5) -> Workflow`

`wf(name)` / `WorkflowBuilder` build a DAG; `.build()` validates locally (duplicate names, dangling
dependencies, cycles) and raises `WorkflowValidationError` before any network round-trip.
`wait_for_*` raise `WaitTimeout` past their deadline.

## How this package is built

The facade is injected at generation time from the queueflow-core-rs template
(`sdk-templates/python/facade.mustache`), so it is regenerated alongside the generated core and can
never drift from the server. Do not edit `queueflow/facade.py` directly; edit the template.

## Worker protocol notes

`qf.worker` exposes the raw worker endpoints (lease, heartbeat, complete, fail); no worker
runtime ships with this SDK. Three rules keep the at-least-once contract honest:

1. Worker routes authenticate with the **worker token**, not a tenant token. Pass it as
   `QueueFlow(base_url, token, worker_token=...)` and `qf.worker` will use it; without it,
   `qf.worker` reuses the tenant token, which only works in the server's development mode.
2. Heartbeat every in-flight job at roughly half its lease interval. A heartbeat whose `status`
   is anything other than `running` (or an HTTP 409) means the server owns the outcome: abandon
   the handler and report nothing. Never process a leased batch sequentially without
   heartbeating the jobs still waiting - their leases expire and the server redelivers them.
3. Delivery is at-least-once, so handlers must be idempotent. Report permanent failures with
   `retryable=False` so they dead-letter immediately instead of burning retries.

## Known limitation: stream_job_events

`JobsApi.stream_job_events` cannot stream: it buffers the whole SSE response until the server
closes it (terminal status or the 15-minute cap). Use `wait_for_job` (polling) instead, or the
`stream_job_events_without_preload_content` variant with a hand-rolled SSE parser.
