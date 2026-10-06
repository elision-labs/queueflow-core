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

## Worker runtime

Run task handlers in this process with `run_worker` — it leases jobs, heartbeats each one at
half the lease interval while the handler runs, and reports the outcome. The engine owns
retries, backoff, the dead-letter queue, and workflow advancement.

```python
import threading
from queueflow.facade import QueueFlow, NonRetryableError

qf = QueueFlow("http://localhost:8000", "tenant-key", worker_token="worker-secret")

def send_email(job):
    if not job.payload.get("to"):
        raise NonRetryableError("no recipient")   # straight to the dead-letter queue
    ...                                           # raising anything else retries per policy
    return {"sent": True}

def transcode(job, ctx):
    for chunk in chunks(job.payload):
        if ctx.cancelled:                         # lease lost / cancelled mid-run: stop,
            return {}                             # the server owns the outcome now
        process(chunk)
    return {"ok": True}

stop = threading.Event()                          # set it (e.g. from a signal handler) to drain
qf.run_worker("orders", {"send_email": send_email, "transcode": transcode}, stop=stop)
```

Rules the runtime enforces for you — and that any hand-rolled loop over the raw `qf.worker`
endpoints must honour too:

1. Worker routes authenticate with the **worker token**, not a tenant token. Pass it as
   `QueueFlow(base_url, token, worker_token=...)`; without it, `qf.worker` reuses the tenant
   token, which only works in the server's development mode. `run_worker` raises on 401/403
   rather than spinning against a bad credential.
2. Every in-flight job is heartbeated at half its lease interval; a heartbeat showing the job
   no longer running (or an HTTP 409) abandons reporting and flips `ctx.cancelled`.
3. Delivery is at-least-once, so handlers must be idempotent. Raise `NonRetryableError` (or any
   exception with `retryable = False`) for permanent failures so they dead-letter immediately.
   A failed outcome *report* is never converted into a job failure — the lease expires and the
   engine redelivers.

## Known limitation: stream_job_events

`JobsApi.stream_job_events` cannot stream: it buffers the whole SSE response until the server
closes it (terminal status or the 15-minute cap). Use `wait_for_job` (polling) instead, or the
`stream_job_events_without_preload_content` variant with a hand-rolled SSE parser.
