# Grafana dashboard

`queueflow-overview.json` is an importable Grafana dashboard for the metrics
the server exposes on its Prometheus port (`--metrics-port`, default 9090,
path `/metrics`). Import it with Dashboards -> New -> Import, pick your
Prometheus data source when prompted, and point Prometheus at every QueueFlow
replica's metrics port.

What it shows:

- **Store reachable, claimable, running, oldest claimable job, dead letters**
  as headline stats. The oldest-claimable-job age is the signal to alert on:
  it grows when workers cannot keep up and is independent of queue size.
- **Backlog and oldest age per queue** from the `queueflow_queue_*` gauges.
  These are read from the database on every scrape, so every replica reports
  the same values; sum or max across replicas accordingly (the dashboard uses
  `min`/`max`/raw series, not sums, for them).
- **Job and workflow outcome rates** from the engine counters. Counters are
  process-local and reset on restart; the dashboard sums `rate()` across
  replicas.
- **Handler duration and queue-wait percentiles** from the in-process worker
  histograms (`queueflow_handler_duration_seconds`,
  `queueflow_job_queue_wait_seconds`). Remote workers over the HTTP protocol
  are not observed by these two; they only cover handlers running inside the
  server binary.

Suggested alerts (PromQL):

```
# Work is piling up on some queue for more than five minutes.
max(queueflow_queue_oldest_pending_age_seconds) > 300

# The metrics endpoint cannot reach the database.
min(queueflow_store_up) == 0

# Jobs are being dead-lettered.
sum(rate(queueflow_jobs_dead_lettered_total[5m])) > 0
```
