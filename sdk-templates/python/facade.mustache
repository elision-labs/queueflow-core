"""Ergonomic facade for the QueueFlow Python SDK.

Hand-written ergonomics layered on the GENERATED core (api clients + models).
Injected at generation time as a supporting file, so it ships with the
generated package but is never produced by the raw codegen. The wire types and
the per-tag api clients come from the generated modules and are never edited.
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .api_client import ApiClient
from .configuration import Configuration
from .api.jobs_api import JobsApi
from .api.workflows_api import WorkflowsApi
from .api.worker_api import WorkerApi
from .api.system_api import SystemApi
from .api.health_api import HealthApi
from .api.cron_api import CronApi
from .api.dlq_api import DlqApi
from .exceptions import ApiException
from .models.complete_job_request import CompleteJobRequest
from .models.create_job_request import CreateJobRequest
from .models.fail_job_request import FailJobRequest
from .models.heartbeat_request import HeartbeatRequest
from .models.lease_jobs_request import LeaseJobsRequest
from .models.job_config_request import JobConfigRequest
from .models.create_workflow_request import CreateWorkflowRequest
from .models.create_cron_request import CreateCronRequest
from .models.workflow_step import WorkflowStep
from .models.job import Job
from .models.queue_stats import QueueStats
from .models.workflow import Workflow

_TERMINAL_JOB = {"completed", "failed", "cancelled"}
_TERMINAL_WORKFLOW = {"completed", "failed", "partially_failed", "cancelled"}


class WaitTimeout(TimeoutError):
    """Raised when wait_for_* exceeds its deadline."""


class WorkflowValidationError(ValueError):
    """Raised by WorkflowBuilder.build() for a structurally invalid DAG."""


class NonRetryableError(Exception):
    """Raise from a worker handler to mark the failure PERMANENT: the job
    skips its remaining retries and dead-letters immediately (e.g. invalid
    input). Any raised exception carrying ``retryable = False`` behaves the
    same; this class is the convenient spelling."""

    retryable = False


class WorkerContext:
    """Per-job context handed to two-argument worker handlers.

    ``cancelled`` flips to True when the job's lease is lost (the job was
    cancelled mid-run, or the lease expired and was reclaimed). From then on
    the server owns the outcome: long handlers should check it periodically
    and stop, because any further work is discarded.
    """

    def __init__(self) -> None:
        self._lost = threading.Event()

    @property
    def cancelled(self) -> bool:
        return self._lost.is_set()


_logger = logging.getLogger("queueflow.worker")


class WorkerRunResult:
    """What :meth:`QueueFlow.run_worker` returns once the worker has stopped.

    ``unfinished`` lists the jobs whose handlers were still running when
    ``drain_timeout`` elapsed. Nothing was reported for them: their leases
    expire and the server redelivers. Empty when every in-flight handler
    finished in time.
    """

    def __init__(self, unfinished: List[Job]) -> None:
        self.unfinished = unfinished

    def __repr__(self) -> str:
        return "WorkerRunResult(unfinished=" + repr([j.id for j in self.unfinished]) + ")"


class _InFlight:
    """Bookkeeping for one leased job while its handler runs."""

    def __init__(self, lease: Any) -> None:
        self.lease = lease
        self.ctx = WorkerContext()
        self.hb_done = threading.Event()

    def abandon(self) -> None:
        """Stop heartbeating and flip ``ctx.cancelled``; nothing is reported afterwards."""
        self.hb_done.set()
        self.ctx._lost.set()


class _TimeoutApiClient(ApiClient):
    """The generated ApiClient with a default per-request timeout.

    The generated Configuration has no timeout setting; every generated api
    method takes ``_request_timeout`` and passes None through to urllib3,
    which then waits forever. This applies ``default_timeout`` (seconds, or a
    ``(connect, read)`` pair) whenever a call does not set its own.
    """

    def __init__(self, configuration: Configuration, default_timeout: Union[None, float, Tuple[float, float]]) -> None:
        super().__init__(configuration)
        self.default_timeout = default_timeout

    def call_api(
        self,
        method,
        url,
        header_params=None,
        body=None,
        post_params=None,
        _request_timeout=None,
    ):
        if _request_timeout is None:
            _request_timeout = self.default_timeout
        return super().call_api(
            method,
            url,
            header_params=header_params,
            body=body,
            post_params=post_params,
            _request_timeout=_request_timeout,
        )


class QueueFlow:
    """Ergonomic client wrapping the generated api clients.

    qf = QueueFlow("http://localhost:8000", "dev")
    job = qf.create_job("echo", payload={"hi": 1})
    done = qf.wait_for_job(job.id)

    The worker-protocol routes (``qf.worker``: lease, heartbeat, complete,
    fail) authenticate with the server's *worker token*, not a tenant token.
    Pass it as ``worker_token=``; without it, ``qf.worker`` reuses the tenant
    token, which only works in the server's development mode.

    ``timeout`` is the default per-request timeout in seconds (30 by default;
    a ``(connect, read)`` pair is also accepted, ``None`` disables it). Any
    generated api method can override it per call with ``_request_timeout=``;
    the worker runtime's long-poll lease calls set their own.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        worker_token: Optional[str] = None,
        timeout: Union[None, float, Tuple[float, float]] = 30.0,
    ) -> None:
        self.timeout = timeout
        config = Configuration(host=base_url.rstrip("/"))
        config.access_token = token
        self._client = _TimeoutApiClient(config, timeout)
        self.jobs = JobsApi(self._client)
        self.workflows = WorkflowsApi(self._client)
        if worker_token is not None:
            worker_config = Configuration(host=base_url.rstrip("/"))
            worker_config.access_token = worker_token
            self._worker_client: Optional[ApiClient] = _TimeoutApiClient(worker_config, timeout)
            self.worker = WorkerApi(self._worker_client)
        else:
            self._worker_client = None
            self.worker = WorkerApi(self._client)
        self.cron = CronApi(self._client)
        self.dlq = DlqApi(self._client)
        self.system = SystemApi(self._client)
        self.health_api = HealthApi(self._client)

    # --- jobs ---------------------------------------------------------------

    def create_job(
        self,
        task: str,
        payload: Optional[Dict[str, Any]] = None,
        priority: Optional[int] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        queue: Optional[str] = None,
        retry_backoff: Optional[str] = None,
        retry_delay_secs: Optional[int] = None,
        retry_max_delay_secs: Optional[int] = None,
        jitter_factor: Optional[float] = None,
        idempotency_key: Optional[str] = None,
        run_at: Optional[Any] = None,
    ) -> Job:
        """Enqueue a job and return its freshly-created record.

        run_at (a datetime) delays the first run: the job is created
        immediately but stays invisible to workers until then.
        """
        config = None
        overrides = (
            priority, max_retries, timeout, queue,
            retry_backoff, retry_delay_secs, retry_max_delay_secs, jitter_factor,
        )
        if any(v is not None for v in overrides):
            config = JobConfigRequest(
                priority=priority,
                max_retries=max_retries,
                timeout=timeout,
                queue=queue,
                retry_backoff=retry_backoff,
                retry_delay_secs=retry_delay_secs,
                retry_max_delay_secs=retry_max_delay_secs,
                jitter_factor=jitter_factor,
            )
        req = CreateJobRequest(task_name=task, payload=payload or {}, config=config, run_at=run_at)
        created = self.jobs.create_job(req, idempotency_key=idempotency_key)
        return self.jobs.get_job(created.job_id)

    def wait_for_job(self, job_id: str, timeout: float = 60.0, interval: float = 0.5) -> Job:
        """Poll until the job reaches a terminal state (completed/failed/cancelled)."""
        return self._poll(lambda: self.jobs.get_job(job_id), _TERMINAL_JOB, job_id, "job", timeout, interval)

    # --- workflows ----------------------------------------------------------

    def create_workflow(self, workflow: "WorkflowBuilder | CreateWorkflowRequest") -> Workflow:
        body = workflow.build() if isinstance(workflow, WorkflowBuilder) else workflow
        created = self.workflows.create_workflow(body)
        return self.workflows.get_workflow(created.workflow_id)

    def wait_for_workflow(self, workflow_id: str, timeout: float = 60.0, interval: float = 0.5) -> Workflow:
        return self._poll(
            lambda: self.workflows.get_workflow(workflow_id),
            _TERMINAL_WORKFLOW,
            workflow_id,
            "workflow",
            timeout,
            interval,
        )

    # --- cron & dead letters ------------------------------------------------

    def create_cron(
        self,
        name: str,
        schedule: str,
        task: str,
        payload: Optional[Dict[str, Any]] = None,
        queue: Optional[str] = None,
    ) -> str:
        """Register a recurring enqueue (5-field crontab, UTC); returns the schedule id."""
        req = CreateCronRequest(
            name=name, cron_expr=schedule, task_name=task, payload=payload or {}, queue=queue
        )
        return self.cron.create_cron(req).cron_id

    def replay_dead_letter(self, dead_letter_id: int) -> Job:
        """Re-run a dead-lettered job as a fresh job; returns the new job record.

        Each entry replays at most once (a second replay is a 409).
        """
        replayed = self.dlq.replay_dead_letter(dead_letter_id)
        return self.jobs.get_job(replayed.job_id)

    # --- system -------------------------------------------------------------

    def queues(self) -> List[QueueStats]:
        """Live backlog per queue (``GET /api/v1/queues``): claimable ``pending``,
        future-dated ``scheduled`` and ``running`` counts, plus how long the
        oldest claimable job has waited. Queues with no non-terminal jobs are
        omitted.
        """
        return self.system.list_queues().queues

    # --- worker runtime -------------------------------------------------------

    def run_worker(
        self,
        queue: str,
        handlers: Dict[str, Callable[..., Optional[Dict[str, Any]]]],
        lease_secs: int = 30,
        wait_secs: int = 20,
        stop: Optional["threading.Event"] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        concurrency: int = 1,
        poll_wait_secs: int = 5,
        drain_timeout: float = 30.0,
    ) -> WorkerRunResult:
        """Run a worker loop: lease jobs from ``queue``, dispatch to
        ``handlers`` by task name, heartbeat while a handler runs, and report
        the outcome. The engine owns retries, backoff, the dead-letter queue,
        and workflow advancement.

        Semantics (matching the Node SDK's ``qf.worker.run``):

        * Up to ``concurrency`` handlers run at once (default 1) on a
          ``ThreadPoolExecutor``. Each lease asks for as many jobs as there
          are free slots, and no lease is issued while every slot is busy.
        * Delivery is **at-least-once**: make handlers idempotent.
        * A handler returns the job's result dict (or None for ``{}``).
          Raising fails the job through the engine's retry policy; raise
          :class:`NonRetryableError` (or any exception with
          ``retryable = False``) to dead-letter immediately.
        * Each in-flight job is heartbeated at half the lease interval by its
          own thread. A heartbeat showing the job is no longer running
          (cancelled mid-run, or the lease was reclaimed) abandons reporting,
          since the server owns the outcome, and flips ``ctx.cancelled`` so a
          two-argument handler ``(job, ctx)`` can stop early.
        * A failed *report* is never re-reported as a job failure: the lease
          expires and the engine redelivers.
        * Lease errors with HTTP 401/403 raise (a wrong or missing
          ``worker_token`` cannot heal by retrying); other lease errors go to
          ``on_error`` (or a throttled log) with a 1s backoff.

        Stopping: set ``stop`` (a ``threading.Event``). The generated urllib3
        client cannot cancel an in-flight long poll, so while ``stop`` is
        provided each lease waits at most ``poll_wait_secs`` (default 5,
        never more than ``wait_secs``), which bounds stop latency to about
        that long. Without ``stop`` the full ``wait_secs`` long poll is used.
        After the loop exits, in-flight handlers get up to ``drain_timeout``
        seconds (default 30) to finish and report. Handlers still running
        afterwards are abandoned: their ``ctx.cancelled`` flips, nothing is
        reported for them (the lease expires and the server redelivers), and
        they are listed in the returned :class:`WorkerRunResult`. Python
        cannot kill a thread, so an abandoned handler keeps its pool thread
        until it returns; long handlers should check ``ctx.cancelled``.

        Each lease call carries its own request timeout of the effective
        wait plus 35 seconds, independent of the client-wide ``timeout``.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        effective_wait = min(wait_secs, poll_wait_secs) if stop is not None else wait_secs
        _logger.info(
            "worker leasing %r with concurrency %d (tasks: %s)",
            queue,
            concurrency,
            ", ".join(sorted(handlers)),
        )
        pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="queueflow-worker")
        in_flight: Dict[Future, _InFlight] = {}
        lock = threading.Lock()
        slot_freed = threading.Event()
        failures = 0
        fatal: Optional[BaseException] = None

        def release(fut: Future) -> None:
            with lock:
                in_flight.pop(fut, None)
            slot_freed.set()

        try:
            while stop is None or not stop.is_set():
                with lock:
                    free = concurrency - len(in_flight)
                if free <= 0:
                    # Every slot is busy: no lease until a handler settles. The
                    # bounded wait keeps ``stop`` responsive.
                    slot_freed.wait(0.5)
                    slot_freed.clear()
                    continue
                try:
                    leased = self.worker.lease_jobs(
                        queue,
                        # The server caps max_jobs at 100 per call.
                        LeaseJobsRequest(
                            max_jobs=min(free, 100), lease_secs=lease_secs, wait_secs=effective_wait
                        ),
                        _request_timeout=float(effective_wait + 35),
                    ).jobs
                    failures = 0
                except ApiException as err:
                    if err.status in (401, 403):
                        fatal = err
                        break
                    failures += 1
                    self._lease_failed(err, failures, on_error)
                    if self._sleep_interruptibly(1.0, stop):
                        break
                    continue
                except Exception as err:  # noqa: BLE001 - transport-level failure
                    failures += 1
                    self._lease_failed(err, failures, on_error)
                    if self._sleep_interruptibly(1.0, stop):
                        break
                    continue
                for lease in leased:
                    entry = _InFlight(lease)
                    with lock:
                        fut = pool.submit(self._run_one, entry, handlers, lease_secs)
                        in_flight[fut] = entry
                    fut.add_done_callback(release)
        finally:
            unfinished = self._drain(in_flight, lock, drain_timeout)
            pool.shutdown(wait=False)
        if fatal is not None:
            raise fatal
        _logger.info("worker stopped (%d unfinished)", len(unfinished))
        return WorkerRunResult(unfinished)

    @staticmethod
    def _drain(in_flight: Dict[Future, _InFlight], lock: "threading.Lock", drain_timeout: float) -> List[Job]:
        """Wait up to ``drain_timeout`` for in-flight handlers; abandon the rest."""
        with lock:
            futures = list(in_flight)
        if not futures:
            return []
        _logger.info("draining %d in-flight job(s) for up to %.1fs", len(futures), drain_timeout)
        _, not_done = wait(futures, timeout=max(0.0, drain_timeout))
        with lock:
            left = [in_flight[f] for f in not_done if f in in_flight]
        for entry in left:
            entry.abandon()
        if left:
            _logger.warning(
                "gave up on %d job(s) still running after %.1fs; their leases will expire and the engine redelivers",
                len(left),
                drain_timeout,
            )
        return [entry.lease.job for entry in left]

    def _lease_failed(self, err, failures, on_error) -> None:
        if on_error is not None:
            on_error(err)
        elif failures == 1 or failures % 30 == 0:
            _logger.warning("lease failed (%d consecutive): %s", failures, err)

    @staticmethod
    def _sleep_interruptibly(seconds: float, stop: Optional["threading.Event"]) -> bool:
        """Sleep; returns True if ``stop`` fired."""
        if stop is None:
            time.sleep(seconds)
            return False
        return stop.wait(seconds)

    def _run_one(self, entry: _InFlight, handlers, lease_secs: int) -> None:
        lease = entry.lease
        job = lease.job
        handler = handlers.get(job.task_name)
        if handler is None:
            self._report(
                job.id,
                FailJobRequest(
                    lease_token=lease.lease_token,
                    error="no worker handler for task '" + job.task_name + "'",
                    retryable=False,
                ),
            )
            return

        ctx = entry.ctx
        hb_done = entry.hb_done
        hb = threading.Thread(
            target=self._heartbeat_loop,
            args=(job.id, lease.lease_token, lease_secs, ctx, hb_done),
            daemon=True,
        )
        hb.start()

        # Handler outcome and outcome *reporting* are separate concerns: a
        # reporting error must never be re-reported as a job failure.
        outcome_error: Optional[BaseException] = None
        result: Dict[str, Any] = {}
        try:
            result = self._call_handler(handler, job, ctx) or {}
        except Exception as err:  # noqa: BLE001 - the engine applies policy
            outcome_error = err
        finally:
            hb_done.set()
            hb.join(timeout=5)

        if ctx.cancelled:
            return  # the server owns the outcome (lease lost, or abandoned by the drain)

        if outcome_error is None:
            self._report(job.id, CompleteJobRequest(lease_token=lease.lease_token, result=result))
        else:
            retryable = getattr(outcome_error, "retryable", True) is not False
            self._report(
                job.id,
                FailJobRequest(
                    lease_token=lease.lease_token,
                    error=str(outcome_error) or type(outcome_error).__name__,
                    retryable=retryable,
                ),
            )

    @staticmethod
    def _call_handler(handler, job, ctx):
        """Call ``handler(job)`` or ``handler(job, ctx)`` by arity."""
        try:
            params = [
                p
                for p in inspect.signature(handler).parameters.values()
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            ]
            wants_ctx = len(params) >= 2
        except (TypeError, ValueError):
            wants_ctx = False
        return handler(job, ctx) if wants_ctx else handler(job)

    def _report(self, job_id: str, request) -> None:
        try:
            if isinstance(request, CompleteJobRequest):
                self.worker.complete_job(job_id, request)
            else:
                self.worker.fail_job(job_id, request)
        except Exception as err:  # noqa: BLE001 - lease expiry redelivers
            _logger.warning(
                "failed to report job %s outcome; the lease will expire and the engine redelivers: %s",
                job_id,
                err,
            )

    def _heartbeat_loop(self, job_id, lease_token, lease_secs, ctx, done) -> None:
        interval = max(1.0, lease_secs / 2.0)
        while not done.wait(interval):
            try:
                status = self.worker.heartbeat_job(
                    job_id,
                    HeartbeatRequest(lease_token=lease_token, extend_secs=lease_secs),
                ).status
                if status != "running":
                    ctx._lost.set()
                    return
            except ApiException as err:
                if err.status == 409:
                    ctx._lost.set()
                    return
            except Exception:  # noqa: BLE001 - transient; next tick retries
                pass

    # --- internal -----------------------------------------------------------

    @staticmethod
    def _poll(fetch, terminal, rid, kind, timeout, interval):
        deadline = time.monotonic() + timeout
        while True:
            value = fetch()
            if value.status in terminal:
                return value
            if time.monotonic() + interval > deadline:
                raise WaitTimeout(kind + " " + rid + " did not finish within " + str(timeout) + "s")
            time.sleep(interval)


class WorkflowBuilder:
    """Typed builder for a workflow DAG, with local validation before the round-trip."""

    def __init__(self, name: str) -> None:
        if not name:
            raise WorkflowValidationError("workflow name is required")
        self.name = name
        self._steps: List[WorkflowStep] = []
        self._context: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] = {}

    def step(
        self,
        name: str,
        task: str,
        after: Optional[List[str]] = None,
        payload: Optional[Dict[str, Any]] = None,
        on_failure: Optional[str] = None,
        on_success: Optional[str] = None,
        config: Optional[Any] = None,
    ) -> "WorkflowBuilder":
        """Add a step. config is a models.JobConfig override for this step;
        the server fills defaults for any field left unset."""
        self._steps.append(
            WorkflowStep(
                name=name,
                task_name=task,
                depends_on=after or [],
                payload=payload or {},
                on_failure=on_failure,
                on_success=on_success,
                config=config,
            )
        )
        return self

    def context(self, **kwargs: Any) -> "WorkflowBuilder":
        self._context.update(kwargs)
        return self

    def build(self) -> CreateWorkflowRequest:
        if not self._steps:
            raise WorkflowValidationError('workflow "' + self.name + '" has no steps')
        names = set()
        for s in self._steps:
            if s.name in names:
                raise WorkflowValidationError('duplicate step name "' + s.name + '"')
            names.add(s.name)
        for s in self._steps:
            for dep in (s.depends_on or []):
                if dep not in names:
                    raise WorkflowValidationError('step "' + s.name + '" depends on unknown step "' + dep + '"')
        self._assert_acyclic()
        return CreateWorkflowRequest(
            name=self.name, steps=self._steps, context=self._context, metadata=self._metadata
        )

    def _assert_acyclic(self) -> None:
        by_name = {s.name: s for s in self._steps}
        visiting, done = set(), set()

        def visit(node: str, stack: List[str]) -> None:
            if node in done:
                return
            if node in visiting:
                cycle = " -> ".join(stack[stack.index(node):] + [node])
                raise WorkflowValidationError("dependency cycle: " + cycle)
            visiting.add(node)
            for dep in (by_name[node].depends_on or []):
                visit(dep, stack + [node])
            visiting.discard(node)
            done.add(node)

        for s in self._steps:
            visit(s.name, [])


def wf(name: str) -> WorkflowBuilder:
    """Shorthand for WorkflowBuilder(name)."""
    return WorkflowBuilder(name)
