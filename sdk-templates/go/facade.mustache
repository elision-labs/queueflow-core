/*
Ergonomic facade for the QueueFlow Go SDK.

Hand-written ergonomics layered on the GENERATED core (APIClient + models).
Injected at generation time as a supporting file, so it ships with the
generated package but is never produced by the raw codegen. The wire types and
the per-tag API services come from the generated files and are never edited.

The facade is the recommended entry point: a QueueFlow client with one method
per endpoint, WaitForJob/WaitForWorkflow pollers, a workflow builder with local
DAG validation, typed errors, and a worker runtime (Run) that leases jobs,
heartbeats them, and reports outcomes.
*/

package queueflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultTimeout is the per-request HTTP timeout applied to the generated
// client when Options.Timeout is zero.
const DefaultTimeout = 30 * time.Second

// leaseTimeoutSlack is added to a lease call's wait_secs to form its deadline:
// a long poll must outlive the regular per-request timeout.
const leaseTimeoutSlack = 35 * time.Second

var terminalJob = map[string]bool{"completed": true, "failed": true, "cancelled": true}
var terminalWorkflow = map[string]bool{"completed": true, "failed": true, "partially_failed": true, "cancelled": true}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// ErrTimeout is wrapped by WaitForJob and WaitForWorkflow when the deadline
// passes before a terminal status. Test with errors.Is(err, ErrTimeout).
var ErrTimeout = errors.New("timed out")

// APIError is a non-2xx response from the server. Every facade method wraps
// the generated client's error in one, so the status is always available:
//
//	var apiErr *queueflow.APIError
//	if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound { ... }
//
// Transport failures (connection refused, context cancelled) are returned
// unwrapped, so errors.Is(err, context.Canceled) keeps working.
type APIError struct {
	// Op is the facade method that failed, e.g. "GetJob".
	Op string
	// Status is the HTTP status code.
	Status int
	// Body is the raw error body (usually {"error": "..."}), possibly empty.
	Body []byte
	// Err is the generated client's error (*GenericOpenAPIError).
	Err error
}

func (e *APIError) Error() string {
	body := strings.TrimSpace(string(e.Body))
	if body == "" && e.Err != nil {
		body = e.Err.Error()
	}
	return fmt.Sprintf("queueflow: %s: HTTP %d: %s", e.Op, e.Status, body)
}

func (e *APIError) Unwrap() error { return e.Err }

// StatusCode returns the HTTP status carried by err (an *APIError anywhere in
// its chain), or 0 for transport and local errors.
func StatusCode(err error) int {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

// wrap turns a generated-client (response, error) pair into a facade error.
func wrap(op string, resp *http.Response, err error) error {
	if err == nil {
		return nil
	}
	if resp != nil && resp.StatusCode >= 400 {
		var body []byte
		var g *GenericOpenAPIError
		if errors.As(err, &g) {
			body = g.Body()
		}
		return &APIError{Op: op, Status: resp.StatusCode, Body: body, Err: err}
	}
	return fmt.Errorf("queueflow: %s: %w", op, err)
}

// NonRetryableError marks a worker handler failure as PERMANENT: the job skips
// its remaining retries and dead-letters immediately (e.g. invalid input).
// Return NonRetryable(err) from a Handler, or any error that wraps one.
type NonRetryableError struct {
	Err error
}

// NonRetryable wraps err so the worker reports it with retryable=false.
func NonRetryable(err error) error { return &NonRetryableError{Err: err} }

func (e *NonRetryableError) Error() string {
	if e.Err == nil {
		return "non-retryable failure"
	}
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error { return e.Err }

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Options configures NewQueueFlowWithOptions.
type Options struct {
	// BaseURL of the server, e.g. "http://localhost:8000". Required.
	BaseURL string
	// Token is the tenant bearer token (jobs, workflows, cron, DLQ, system
	// routes). Required.
	Token string
	// WorkerToken authenticates the worker-protocol routes (LeaseJobs,
	// HeartbeatJob, CompleteJob, FailJob, Run). Servers started with
	// --worker-token refuse tenant tokens there. Defaults to Token, which
	// only works on a server running without a worker token.
	WorkerToken string
	// Timeout is the per-request HTTP timeout (default DefaultTimeout). Lease
	// calls are exempt: they get wait_secs plus 35 seconds instead.
	Timeout time.Duration
	// HTTPClient, when set, is copied and used for tenant calls (its Timeout
	// is replaced by Timeout when zero) and, with no Timeout, for worker
	// calls, which carry per-call deadlines. nil uses http.DefaultTransport.
	HTTPClient *http.Client
}

// QueueFlow is an ergonomic wrapper over the generated APIClient.
//
//	qf := queueflow.NewQueueFlow("http://localhost:8000", "dev")
//	id, _ := qf.CreateJob(ctx, "echo", map[string]interface{}{"hi": 1})
//	done, _ := qf.WaitForJob(ctx, id, queueflow.WaitOptions{})
type QueueFlow struct {
	// Client is the generated client authenticated with the tenant token.
	Client *APIClient
	// WorkerClient is the generated client authenticated with the worker
	// token; its HTTP client has no global timeout (worker calls set
	// per-call deadlines).
	WorkerClient *APIClient
	timeout      time.Duration
}

// NewQueueFlow builds a client pointed at baseURL, authenticating with token.
// The worker token defaults to token; use NewQueueFlowWithOptions to set a
// separate one (required by servers started with --worker-token).
func NewQueueFlow(baseURL, token string) *QueueFlow {
	return build(Options{BaseURL: baseURL, Token: token})
}

// NewQueueFlowWithOptions builds a client from opts, validating that BaseURL
// and Token are set.
func NewQueueFlowWithOptions(opts Options) (*QueueFlow, error) {
	if opts.BaseURL == "" {
		return nil, errors.New("queueflow: Options.BaseURL is required")
	}
	if opts.Token == "" {
		return nil, errors.New("queueflow: Options.Token is required")
	}
	return build(opts), nil
}

func build(opts Options) *QueueFlow {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	workerToken := opts.WorkerToken
	if workerToken == "" {
		workerToken = opts.Token
	}

	var tenantHTTP, workerHTTP http.Client
	if opts.HTTPClient != nil {
		tenantHTTP = *opts.HTTPClient
		workerHTTP = *opts.HTTPClient
	}
	if tenantHTTP.Timeout == 0 {
		tenantHTTP.Timeout = timeout
	}
	workerHTTP.Timeout = 0

	baseURL := strings.TrimRight(opts.BaseURL, "/")
	mk := func(token string, hc *http.Client) *APIClient {
		cfg := NewConfiguration()
		// Two statements, not a nested literal: a doubled brace would be read
		// as a tag by the mustache renderer that injects this file.
		server := ServerConfiguration{URL: baseURL}
		cfg.Servers = ServerConfigurations{server}
		cfg.AddDefaultHeader("Authorization", "Bearer "+token)
		cfg.HTTPClient = hc
		return NewAPIClient(cfg)
	}
	return &QueueFlow{
		Client:       mk(opts.Token, &tenantHTTP),
		WorkerClient: mk(workerToken, &workerHTTP),
		timeout:      timeout,
	}
}

// Timeout is the per-request HTTP timeout in effect.
func (q *QueueFlow) Timeout() time.Duration { return q.timeout }

// ---------------------------------------------------------------------------
// Shared option types
// ---------------------------------------------------------------------------

// ListOptions filters the list endpoints. Zero values are not sent, so the
// server defaults apply.
type ListOptions struct {
	Status string
	Queue  string
	Limit  int64
	Offset int64
	// OrderBy is "created_at ASC" or "created_at DESC".
	OrderBy      string
	IncludeTotal bool
	// Cursor is the opaque keyset cursor from a previous page's NextCursor.
	// When set, Offset is ignored (cheaper than deep OFFSET paging).
	Cursor        string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

// WaitOptions tunes WaitForJob and WaitForWorkflow. Zero values default to a
// 60 second timeout and a 500 millisecond poll interval.
type WaitOptions struct {
	Timeout  time.Duration
	Interval time.Duration
}

func (o WaitOptions) resolve() (time.Duration, time.Duration) {
	timeout, interval := o.Timeout, o.Interval
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return timeout, interval
}

// poll fetches until isTerminal or the deadline passes.
func poll[T any](ctx context.Context, kind, id string, opts WaitOptions, fetch func() (*T, error), isTerminal func(*T) bool) (*T, error) {
	timeout, interval := opts.resolve()
	deadline := time.Now().Add(timeout)
	for {
		v, err := fetch()
		if err != nil {
			return nil, err
		}
		if isTerminal(v) {
			return v, nil
		}
		if time.Now().Add(interval).After(deadline) {
			return nil, fmt.Errorf("queueflow: %s %s did not finish within %s: %w", kind, id, timeout, ErrTimeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

// CreateJobOptions carries everything CreateJobRequest accepts. nil pointers
// and empty strings are not sent, so the server defaults apply. Use PtrInt32,
// PtrInt64, PtrFloat64 and PtrTime (from the generated core) for literals.
type CreateJobOptions struct {
	Payload map[string]interface{}
	// Queue overrides the destination queue.
	Queue string
	// Priority: higher is claimed first within a queue.
	Priority *int32
	// MaxRetries before dead-lettering.
	MaxRetries *int32
	// Timeout is the per-attempt timeout, in seconds.
	Timeout *int64
	// RetryBackoff is how retry delays grow (default exponential).
	RetryBackoff BackoffStrategy
	// RetryDelaySecs is the base retry delay, in seconds.
	RetryDelaySecs *int64
	// RetryMaxDelaySecs bounds any computed retry delay, in seconds.
	RetryMaxDelaySecs *int64
	// JitterFactor in 0..=1 (0.1 = +/-10%).
	JitterFactor *float64
	// IdempotencyKey makes the create idempotent per tenant (sent as the
	// Idempotency-Key header): the same key returns the original job id.
	IdempotencyKey string
	// RunAt delays the first run; the job is created immediately but stays
	// invisible to workers until then.
	RunAt *time.Time
}

func (o CreateJobOptions) request(task string) CreateJobRequest {
	req := CreateJobRequest{TaskName: task}
	if o.Payload != nil {
		req.Payload = o.Payload
	}
	cfg := JobConfigRequest{}
	set := false
	if o.Queue != "" {
		cfg.Queue = *NewNullableString(PtrString(o.Queue))
		set = true
	}
	if o.Priority != nil {
		cfg.Priority = *NewNullableInt32(o.Priority)
		set = true
	}
	if o.MaxRetries != nil {
		cfg.MaxRetries = *NewNullableInt32(o.MaxRetries)
		set = true
	}
	if o.Timeout != nil {
		cfg.Timeout = *NewNullableInt64(o.Timeout)
		set = true
	}
	if o.RetryBackoff != "" {
		b := o.RetryBackoff
		cfg.RetryBackoff = *NewNullableBackoffStrategy(&b)
		set = true
	}
	if o.RetryDelaySecs != nil {
		cfg.RetryDelaySecs = *NewNullableInt64(o.RetryDelaySecs)
		set = true
	}
	if o.RetryMaxDelaySecs != nil {
		cfg.RetryMaxDelaySecs = *NewNullableInt64(o.RetryMaxDelaySecs)
		set = true
	}
	if o.JitterFactor != nil {
		cfg.JitterFactor = *NewNullableFloat64(o.JitterFactor)
		set = true
	}
	if set {
		req.Config = *NewNullableJobConfigRequest(&cfg)
	}
	if o.RunAt != nil {
		t := o.RunAt.UTC()
		req.RunAt = *NewNullableTime(&t)
	}
	return req
}

// CreateJob enqueues a job with the server's default config and returns the
// new job id. nil payload sends no payload. See CreateJobWithOptions for
// queue, priority, retries, idempotency and run_at.
func (q *QueueFlow) CreateJob(ctx context.Context, task string, payload map[string]interface{}) (string, error) {
	return q.CreateJobWithOptions(ctx, task, CreateJobOptions{Payload: payload})
}

// CreateJobWithOptions enqueues a job and returns the new job id (the
// original job's id when IdempotencyKey matches an earlier create).
func (q *QueueFlow) CreateJobWithOptions(ctx context.Context, task string, opts CreateJobOptions) (string, error) {
	call := q.Client.JobsAPI.CreateJob(ctx).CreateJobRequest(opts.request(task))
	if opts.IdempotencyKey != "" {
		call = call.IdempotencyKey(opts.IdempotencyKey)
	}
	created, resp, err := call.Execute()
	if err != nil {
		return "", wrap("CreateJob", resp, err)
	}
	return created.JobId, nil
}

// GetJob fetches a job record.
func (q *QueueFlow) GetJob(ctx context.Context, id string) (*Job, error) {
	job, resp, err := q.Client.JobsAPI.GetJob(ctx, id).Execute()
	if err != nil {
		return nil, wrap("GetJob", resp, err)
	}
	return job, nil
}

// ListJobs lists jobs. The response carries NextCursor when more pages exist.
func (q *QueueFlow) ListJobs(ctx context.Context, opts ListOptions) (*ListJobsResponse, error) {
	r := q.Client.JobsAPI.ListJobs(ctx)
	if opts.Status != "" {
		r = r.Status(opts.Status)
	}
	if opts.Queue != "" {
		r = r.Queue(opts.Queue)
	}
	if opts.Limit > 0 {
		r = r.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		r = r.Offset(opts.Offset)
	}
	if opts.OrderBy != "" {
		r = r.OrderBy(opts.OrderBy)
	}
	if opts.IncludeTotal {
		r = r.IncludeTotal(true)
	}
	if opts.Cursor != "" {
		r = r.Cursor(opts.Cursor)
	}
	if opts.CreatedAfter != nil {
		r = r.CreatedAfter(*opts.CreatedAfter)
	}
	if opts.CreatedBefore != nil {
		r = r.CreatedBefore(*opts.CreatedBefore)
	}
	out, resp, err := r.Execute()
	if err != nil {
		return nil, wrap("ListJobs", resp, err)
	}
	return out, nil
}

// CancelJob cancels a pending, scheduled or running job.
func (q *QueueFlow) CancelJob(ctx context.Context, id string) error {
	resp, err := q.Client.JobsAPI.CancelJob(ctx, id).Execute()
	return wrap("CancelJob", resp, err)
}

// WaitForJob polls until the job reaches a terminal state (completed, failed
// or cancelled). It returns an error wrapping ErrTimeout past opts.Timeout
// (default 60s) and ctx.Err() when ctx is done.
func (q *QueueFlow) WaitForJob(ctx context.Context, id string, opts WaitOptions) (*Job, error) {
	return poll(ctx, "job", id, opts,
		func() (*Job, error) { return q.GetJob(ctx, id) },
		func(j *Job) bool { return terminalJob[string(j.Status)] })
}

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

// CreateWorkflow validates the builder's DAG locally, creates the workflow
// and returns the new workflow id.
func (q *QueueFlow) CreateWorkflow(ctx context.Context, b *WorkflowBuilder) (string, error) {
	body, err := b.Build()
	if err != nil {
		return "", err
	}
	created, resp, err := q.Client.WorkflowsAPI.CreateWorkflow(ctx).CreateWorkflowRequest(*body).Execute()
	if err != nil {
		return "", wrap("CreateWorkflow", resp, err)
	}
	return created.WorkflowId, nil
}

// GetWorkflow fetches a workflow record (its step definitions, not their
// runtime status; see WorkflowSteps).
func (q *QueueFlow) GetWorkflow(ctx context.Context, id string) (*Workflow, error) {
	wf, resp, err := q.Client.WorkflowsAPI.GetWorkflow(ctx, id).Execute()
	if err != nil {
		return nil, wrap("GetWorkflow", resp, err)
	}
	return wf, nil
}

// ListWorkflows lists workflows.
func (q *QueueFlow) ListWorkflows(ctx context.Context, opts ListOptions) (*ListWorkflowsResponse, error) {
	r := q.Client.WorkflowsAPI.ListWorkflows(ctx)
	if opts.Status != "" {
		r = r.Status(opts.Status)
	}
	if opts.Queue != "" {
		r = r.Queue(opts.Queue)
	}
	if opts.Limit > 0 {
		r = r.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		r = r.Offset(opts.Offset)
	}
	if opts.OrderBy != "" {
		r = r.OrderBy(opts.OrderBy)
	}
	if opts.IncludeTotal {
		r = r.IncludeTotal(true)
	}
	if opts.Cursor != "" {
		r = r.Cursor(opts.Cursor)
	}
	if opts.CreatedAfter != nil {
		r = r.CreatedAfter(*opts.CreatedAfter)
	}
	if opts.CreatedBefore != nil {
		r = r.CreatedBefore(*opts.CreatedBefore)
	}
	out, resp, err := r.Execute()
	if err != nil {
		return nil, wrap("ListWorkflows", resp, err)
	}
	return out, nil
}

// CancelWorkflow cancels a workflow and its not-yet-finished steps.
func (q *QueueFlow) CancelWorkflow(ctx context.Context, id string) error {
	resp, err := q.Client.WorkflowsAPI.CancelWorkflow(ctx, id).Execute()
	return wrap("CancelWorkflow", resp, err)
}

// WorkflowDiagram returns the Mermaid ("graph TD") diagram of the DAG.
func (q *QueueFlow) WorkflowDiagram(ctx context.Context, id string) (*WorkflowDiagramResponse, error) {
	out, resp, err := q.Client.WorkflowsAPI.GetWorkflowDiagram(ctx, id).Execute()
	if err != nil {
		return nil, wrap("WorkflowDiagram", resp, err)
	}
	return out, nil
}

// WorkflowSteps returns the runtime status of every step in declaration
// order, with the id of the job executing each step once scheduled.
func (q *QueueFlow) WorkflowSteps(ctx context.Context, id string) ([]WorkflowStepState, error) {
	out, resp, err := q.Client.WorkflowsAPI.GetWorkflowStepStates(ctx, id).Execute()
	if err != nil {
		return nil, wrap("WorkflowSteps", resp, err)
	}
	return out.Steps, nil
}

// WaitForWorkflow polls until the workflow reaches a terminal state
// (completed, failed, partially_failed or cancelled).
func (q *QueueFlow) WaitForWorkflow(ctx context.Context, id string, opts WaitOptions) (*Workflow, error) {
	return poll(ctx, "workflow", id, opts,
		func() (*Workflow, error) { return q.GetWorkflow(ctx, id) },
		func(w *Workflow) bool { return terminalWorkflow[string(w.Status)] })
}

// ---------------------------------------------------------------------------
// Cron
// ---------------------------------------------------------------------------

// CreateCronOptions carries the optional CreateCronRequest fields.
type CreateCronOptions struct {
	// Payload is sent with every firing.
	Payload map[string]interface{}
	// Queue for the enqueued jobs (server default when empty).
	Queue string
	// Config overrides the enqueued jobs' execution config.
	Config *JobConfig
}

// CreateCron registers a recurring enqueue (5-field crontab, evaluated in
// UTC) and returns the schedule id. payload is sent with every firing; nil
// means an empty payload.
func (q *QueueFlow) CreateCron(ctx context.Context, name, cronExpr, task string, payload map[string]interface{}) (string, error) {
	return q.CreateCronWithOptions(ctx, name, cronExpr, task, CreateCronOptions{Payload: payload})
}

// CreateCronWithOptions registers a recurring enqueue with a queue and config
// override and returns the schedule id.
func (q *QueueFlow) CreateCronWithOptions(ctx context.Context, name, cronExpr, task string, opts CreateCronOptions) (string, error) {
	payload := opts.Payload
	if payload == nil {
		payload = map[string]interface{}{}
	}
	req := CreateCronRequest{Name: name, CronExpr: cronExpr, TaskName: task, Payload: payload}
	if opts.Queue != "" {
		req.Queue = *NewNullableString(PtrString(opts.Queue))
	}
	if opts.Config != nil {
		req.Config = *NewNullableJobConfig(opts.Config)
	}
	created, resp, err := q.Client.CronAPI.CreateCron(ctx).CreateCronRequest(req).Execute()
	if err != nil {
		return "", wrap("CreateCron", resp, err)
	}
	return created.CronId, nil
}

// ListCrons lists cron schedules.
func (q *QueueFlow) ListCrons(ctx context.Context, opts ListOptions) (*ListCronsResponse, error) {
	r := q.Client.CronAPI.ListCrons(ctx)
	if opts.Status != "" {
		r = r.Status(opts.Status)
	}
	if opts.Queue != "" {
		r = r.Queue(opts.Queue)
	}
	if opts.Limit > 0 {
		r = r.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		r = r.Offset(opts.Offset)
	}
	if opts.OrderBy != "" {
		r = r.OrderBy(opts.OrderBy)
	}
	if opts.IncludeTotal {
		r = r.IncludeTotal(true)
	}
	if opts.Cursor != "" {
		r = r.Cursor(opts.Cursor)
	}
	if opts.CreatedAfter != nil {
		r = r.CreatedAfter(*opts.CreatedAfter)
	}
	if opts.CreatedBefore != nil {
		r = r.CreatedBefore(*opts.CreatedBefore)
	}
	out, resp, err := r.Execute()
	if err != nil {
		return nil, wrap("ListCrons", resp, err)
	}
	return out, nil
}

// GetCron fetches a cron schedule.
func (q *QueueFlow) GetCron(ctx context.Context, id string) (*CronSchedule, error) {
	out, resp, err := q.Client.CronAPI.GetCron(ctx, id).Execute()
	if err != nil {
		return nil, wrap("GetCron", resp, err)
	}
	return out, nil
}

// DeleteCron deletes a schedule; already-enqueued jobs are unaffected.
func (q *QueueFlow) DeleteCron(ctx context.Context, id string) error {
	resp, err := q.Client.CronAPI.DeleteCron(ctx, id).Execute()
	return wrap("DeleteCron", resp, err)
}

// PauseCron stops firings until ResumeCron.
func (q *QueueFlow) PauseCron(ctx context.Context, id string) error {
	resp, err := q.Client.CronAPI.PauseCron(ctx, id).Execute()
	return wrap("PauseCron", resp, err)
}

// ResumeCron resumes firings at the next future occurrence (missed runs are
// skipped).
func (q *QueueFlow) ResumeCron(ctx context.Context, id string) error {
	resp, err := q.Client.CronAPI.ResumeCron(ctx, id).Execute()
	return wrap("ResumeCron", resp, err)
}

// ---------------------------------------------------------------------------
// Dead letters
// ---------------------------------------------------------------------------

// ListDeadLetters lists terminally-failed jobs.
func (q *QueueFlow) ListDeadLetters(ctx context.Context, opts ListOptions) (*ListDeadLettersResponse, error) {
	r := q.Client.DlqAPI.ListDeadLetters(ctx)
	if opts.Status != "" {
		r = r.Status(opts.Status)
	}
	if opts.Queue != "" {
		r = r.Queue(opts.Queue)
	}
	if opts.Limit > 0 {
		r = r.Limit(opts.Limit)
	}
	if opts.Offset > 0 {
		r = r.Offset(opts.Offset)
	}
	if opts.OrderBy != "" {
		r = r.OrderBy(opts.OrderBy)
	}
	if opts.IncludeTotal {
		r = r.IncludeTotal(true)
	}
	if opts.Cursor != "" {
		r = r.Cursor(opts.Cursor)
	}
	if opts.CreatedAfter != nil {
		r = r.CreatedAfter(*opts.CreatedAfter)
	}
	if opts.CreatedBefore != nil {
		r = r.CreatedBefore(*opts.CreatedBefore)
	}
	out, resp, err := r.Execute()
	if err != nil {
		return nil, wrap("ListDeadLetters", resp, err)
	}
	return out, nil
}

// GetDeadLetter fetches one dead-letter entry.
func (q *QueueFlow) GetDeadLetter(ctx context.Context, id int64) (*DeadLetter, error) {
	out, resp, err := q.Client.DlqAPI.GetDeadLetter(ctx, id).Execute()
	if err != nil {
		return nil, wrap("GetDeadLetter", resp, err)
	}
	return out, nil
}

// ReplayDeadLetter re-runs a dead-lettered job as a fresh job and returns the
// new job id. Each entry replays at most once (a second replay is a 409).
func (q *QueueFlow) ReplayDeadLetter(ctx context.Context, id int64) (string, error) {
	out, resp, err := q.Client.DlqAPI.ReplayDeadLetter(ctx, id).Execute()
	if err != nil {
		return "", wrap("ReplayDeadLetter", resp, err)
	}
	return out.JobId, nil
}

// ---------------------------------------------------------------------------
// System and health
// ---------------------------------------------------------------------------

// Stats returns the engine's counters.
func (q *QueueFlow) Stats(ctx context.Context) (*StatsSnapshot, error) {
	out, resp, err := q.Client.SystemAPI.GetStats(ctx).Execute()
	if err != nil {
		return nil, wrap("Stats", resp, err)
	}
	return out, nil
}

// Queues returns the live backlog per queue (pending, scheduled, running and
// the oldest pending age), one entry per queue with non-terminal jobs.
func (q *QueueFlow) Queues(ctx context.Context) ([]QueueStats, error) {
	out, resp, err := q.Client.SystemAPI.ListQueues(ctx).Execute()
	if err != nil {
		return nil, wrap("Queues", resp, err)
	}
	return out.Queues, nil
}

// Tasks returns the names of the task handlers registered on the server.
func (q *QueueFlow) Tasks(ctx context.Context) ([]string, error) {
	out, resp, err := q.Client.SystemAPI.ListTasks(ctx).Execute()
	if err != nil {
		return nil, wrap("Tasks", resp, err)
	}
	return out.Tasks, nil
}

// Health is the liveness probe (GET /health).
func (q *QueueFlow) Health(ctx context.Context) (*HealthStatus, error) {
	out, resp, err := q.Client.HealthAPI.GetHealth(ctx).Execute()
	if err != nil {
		return nil, wrap("Health", resp, err)
	}
	return out, nil
}

// Ready is the readiness probe (GET /ready).
func (q *QueueFlow) Ready(ctx context.Context) (*ReadyStatus, error) {
	out, resp, err := q.Client.HealthAPI.GetReady(ctx).Execute()
	if err != nil {
		return nil, wrap("Ready", resp, err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Worker protocol (worker token)
// ---------------------------------------------------------------------------

// LeaseOptions tunes LeaseJobs. Zero values take the server defaults
// (max_jobs 1, lease_secs 30, wait_secs 0).
type LeaseOptions struct {
	// MaxJobs to lease in one call (1..=100).
	MaxJobs int32
	// LeaseSecs is the lease duration (1..=3600); heartbeat to extend.
	LeaseSecs int32
	// WaitSecs is the long-poll wait when the queue is empty (0..=30).
	WaitSecs int32
}

// LeaseJobs leases up to opts.MaxJobs jobs from queue, long-polling up to
// opts.WaitSecs when it is empty. The call's deadline is wait_secs plus 35
// seconds (or ctx's, whichever is sooner).
func (q *QueueFlow) LeaseJobs(ctx context.Context, queue string, opts LeaseOptions) ([]LeasedJob, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(opts.WaitSecs)*time.Second+leaseTimeoutSlack)
	defer cancel()
	req := LeaseJobsRequest{}
	if opts.MaxJobs > 0 {
		req.MaxJobs = *NewNullableInt32(PtrInt32(opts.MaxJobs))
	}
	if opts.LeaseSecs > 0 {
		req.LeaseSecs = *NewNullableInt32(PtrInt32(opts.LeaseSecs))
	}
	if opts.WaitSecs > 0 {
		req.WaitSecs = *NewNullableInt32(PtrInt32(opts.WaitSecs))
	}
	out, resp, err := q.WorkerClient.WorkerAPI.LeaseJobs(ctx, queue).LeaseJobsRequest(req).Execute()
	if err != nil {
		return nil, wrap("LeaseJobs", resp, err)
	}
	return out.Jobs, nil
}

// HeartbeatJob extends a lease by extendSecs and returns the job's current
// status: JOBSTATUS_RUNNING means extended; anything else means the server
// owns the outcome and the handler should stop. A 409 means the lease was
// already lost.
func (q *QueueFlow) HeartbeatJob(ctx context.Context, lease *LeasedJob, extendSecs int32) (JobStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	req := HeartbeatRequest{LeaseToken: lease.LeaseToken, ExtendSecs: extendSecs}
	out, resp, err := q.WorkerClient.WorkerAPI.HeartbeatJob(ctx, lease.Job.Id).HeartbeatRequest(req).Execute()
	if err != nil {
		return "", wrap("HeartbeatJob", resp, err)
	}
	return out.Status, nil
}

// CompleteJob reports success with result. Replaying against a finished job
// is a server-side no-op.
func (q *QueueFlow) CompleteJob(ctx context.Context, lease *LeasedJob, result map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	if result == nil {
		result = map[string]interface{}{}
	}
	req := CompleteJobRequest{LeaseToken: lease.LeaseToken, Result: result}
	resp, err := q.WorkerClient.WorkerAPI.CompleteJob(ctx, lease.Job.Id).CompleteJobRequest(req).Execute()
	return wrap("CompleteJob", resp, err)
}

// FailJob reports failure; the server applies the job's retry policy, or
// dead-letters it immediately when retryable is false.
func (q *QueueFlow) FailJob(ctx context.Context, lease *LeasedJob, message string, retryable bool) error {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()
	req := FailJobRequest{LeaseToken: lease.LeaseToken, Error: message, Retryable: PtrBool(retryable)}
	resp, err := q.WorkerClient.WorkerAPI.FailJob(ctx, lease.Job.Id).FailJobRequest(req).Execute()
	return wrap("FailJob", resp, err)
}

// ---------------------------------------------------------------------------
// Worker runtime
// ---------------------------------------------------------------------------

// Handler executes one leased job and returns its result. Delivery is
// at-least-once, so handlers must be idempotent. ctx is cancelled when the
// job's lease is lost (cancelled mid-run or reclaimed after expiry; the
// server then owns the outcome and nothing is reported) and when Run's drain
// timeout passes after a stop. A returned error is reported as a retryable
// failure unless it is or wraps a *NonRetryableError.
type Handler func(ctx context.Context, job *Job) (map[string]interface{}, error)

// WorkerOptions tunes Run. Zero values take the documented defaults.
type WorkerOptions struct {
	// Concurrency is the number of jobs in flight at once (default 1). Each
	// lease call asks for as many jobs as there are free slots.
	Concurrency int
	// LeaseSecs is the lease duration (default 30); each job is heartbeated
	// at half this interval while its handler runs.
	LeaseSecs int32
	// WaitSecs is the server-side long-poll when the queue is empty (default
	// 20, max 30).
	WaitSecs int32
	// DrainTimeout bounds how long Run waits for in-flight handlers after ctx
	// is cancelled (default 30s). Past it their contexts are cancelled and
	// Run returns; unreported leases expire and the server redelivers.
	DrainTimeout time.Duration
	// OnError, when set, is called with every lease failure instead of the
	// default throttled log line (first failure, then every 30th).
	OnError func(err error)
	// Logf receives the worker's diagnostics (default log.Printf).
	Logf func(format string, args ...interface{})
}

func (o WorkerOptions) withDefaults() WorkerOptions {
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.LeaseSecs <= 0 {
		o.LeaseSecs = 30
	}
	if o.WaitSecs <= 0 {
		o.WaitSecs = 20
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 30 * time.Second
	}
	if o.Logf == nil {
		o.Logf = log.Printf
	}
	return o
}

// Run is the worker loop: lease up to opts.Concurrency jobs from queue,
// dispatch each to handlers by task name in its own goroutine, heartbeat
// while the handler runs, and report the outcome. Jobs whose task has no
// handler are failed non-retryably.
//
// Run returns nil once ctx is cancelled: the in-flight lease long-poll is
// abandoned, then in-flight handlers are given opts.DrainTimeout to finish
// and report. It returns the *APIError when the lease call is rejected with
// 401 or 403, since a wrong or missing worker token cannot heal by retrying.
// Other lease errors back off one second and retry.
func (q *QueueFlow) Run(ctx context.Context, queue string, handlers map[string]Handler, opts WorkerOptions) error {
	opts = opts.withDefaults()

	// Handlers run under jobsCtx, not ctx, so a stop does not cut them off
	// before the drain window has passed.
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	var wg sync.WaitGroup
	slots := make(chan struct{}, opts.Concurrency) // one token per in-flight job
	failures := 0
	var fatal error

loop:
	for {
		// Hold a slot before leasing so we never lease more than we can run.
		select {
		case <-ctx.Done():
			break loop
		case slots <- struct{}{}:
		}
		free := int32(cap(slots) - len(slots) + 1)
		leases, err := q.LeaseJobs(ctx, queue, LeaseOptions{MaxJobs: free, LeaseSecs: opts.LeaseSecs, WaitSecs: opts.WaitSecs})
		if err != nil {
			<-slots
			if ctx.Err() != nil {
				break loop
			}
			if s := StatusCode(err); s == http.StatusUnauthorized || s == http.StatusForbidden {
				fatal = err
				break loop
			}
			failures++
			if opts.OnError != nil {
				opts.OnError(err)
			} else if failures == 1 || failures%30 == 0 {
				opts.Logf("[queueflow] worker lease failed (%d consecutive): %v", failures, err)
			}
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(time.Second):
			}
			continue
		}
		failures = 0
		if len(leases) == 0 {
			<-slots
			continue
		}
		for i := range leases {
			if i > 0 {
				slots <- struct{}{}
			}
			lease := leases[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				q.runOne(jobsCtx, &lease, handlers, opts)
			}()
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(opts.DrainTimeout):
		opts.Logf("[queueflow] worker drain timeout (%s) passed with %d job(s) in flight; their leases will expire and the server redelivers", opts.DrainTimeout, len(slots))
		cancelJobs()
	}
	return fatal
}

// runOne dispatches one leased job under a heartbeat and reports its outcome.
func (q *QueueFlow) runOne(parent context.Context, lease *LeasedJob, handlers map[string]Handler, opts WorkerOptions) {
	job := &lease.Job
	handler, ok := handlers[job.TaskName]
	if !ok {
		if err := q.FailJob(context.Background(), lease, fmt.Sprintf("no handler for task %q", job.TaskName), false); err != nil {
			opts.Logf("[queueflow] job %s: failed to report missing handler: %v", job.Id, err)
		}
		return
	}

	// The heartbeat doubles as the lease-loss channel: a non-running status
	// or a 409 cancels the handler's context, and nothing is reported since
	// the server already owns the outcome.
	jobCtx, cancel := context.WithCancel(parent)
	defer cancel()
	var lost int32
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		every := time.Duration(opts.LeaseSecs/2) * time.Second
		if every < time.Second {
			every = time.Second
		}
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-ticker.C:
			}
			status, err := q.HeartbeatJob(context.Background(), lease, opts.LeaseSecs)
			if err != nil {
				if StatusCode(err) == http.StatusConflict {
					atomic.StoreInt32(&lost, 1)
					cancel()
					return
				}
				opts.Logf("[queueflow] job %s: heartbeat failed: %v", job.Id, err)
				continue
			}
			if status != JOBSTATUS_RUNNING {
				atomic.StoreInt32(&lost, 1)
				cancel()
				return
			}
		}
	}()

	result, herr := callHandler(jobCtx, handler, job)
	close(hbStop)
	<-hbDone

	if atomic.LoadInt32(&lost) == 1 {
		return
	}
	if herr != nil && parent.Err() != nil {
		// Cut off by the drain timeout: let the lease expire and the server
		// redeliver, rather than burn a retry on our own shutdown.
		return
	}

	// Handler outcome and outcome reporting are separate concerns: a
	// reporting error is logged, never re-reported as a job failure.
	var rerr error
	if herr == nil {
		rerr = q.CompleteJob(context.Background(), lease, result)
	} else {
		var nr *NonRetryableError
		retryable := !errors.As(herr, &nr)
		rerr = q.FailJob(context.Background(), lease, herr.Error(), retryable)
	}
	if rerr != nil {
		opts.Logf("[queueflow] job %s: failed to report outcome; the lease will expire and the server redelivers: %v", job.Id, rerr)
	}
}

// callHandler runs h, converting a panic into a retryable failure so one bad
// job cannot take the worker process down.
func callHandler(ctx context.Context, h Handler, job *Job) (result map[string]interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, job)
}

// ---------------------------------------------------------------------------
// Workflow builder
// ---------------------------------------------------------------------------

// StepOptions configures one workflow step for StepWithOptions.
type StepOptions struct {
	// After names the steps that must complete before this one runs.
	After []string
	// Payload is passed to the task handler.
	Payload map[string]interface{}
	// Config overrides the step's execution config (retries, timeout, ...).
	Config *JobConfig
	// OnFailure is ONFAILURE_HALT (default), ONFAILURE_SKIP or ONFAILURE_CONTINUE.
	OnFailure OnFailure
	OnSuccess OnSuccess
	Metadata  map[string]interface{}
}

// WorkflowBuilder is a typed builder for a workflow DAG with local validation.
type WorkflowBuilder struct {
	name     string
	steps    []WorkflowStep
	context  map[string]interface{}
	metadata map[string]interface{}
}

// NewWorkflowBuilder starts a workflow definition named name.
func NewWorkflowBuilder(name string) *WorkflowBuilder {
	return &WorkflowBuilder{name: name}
}

// Step adds a step running task, optionally gated on the named "after" steps.
func (b *WorkflowBuilder) Step(name, task string, after ...string) *WorkflowBuilder {
	return b.StepWithOptions(name, task, StepOptions{After: after})
}

// StepWithOptions adds a step with a payload, failure policy, config override
// and dependencies.
func (b *WorkflowBuilder) StepWithOptions(name, task string, opts StepOptions) *WorkflowBuilder {
	s := NewWorkflowStep(name, task)
	if len(opts.After) > 0 {
		s.DependsOn = opts.After
	}
	if opts.Payload != nil {
		s.Payload = opts.Payload
	}
	if opts.Config != nil {
		s.Config = *NewNullableJobConfig(opts.Config)
	}
	if opts.OnFailure != "" {
		f := opts.OnFailure
		s.OnFailure = &f
	}
	if opts.OnSuccess != "" {
		f := opts.OnSuccess
		s.OnSuccess = &f
	}
	if opts.Metadata != nil {
		s.Metadata = opts.Metadata
	}
	b.steps = append(b.steps, *s)
	return b
}

// Context seeds the shared workflow context (merged across calls).
func (b *WorkflowBuilder) Context(context map[string]interface{}) *WorkflowBuilder {
	if b.context == nil {
		b.context = map[string]interface{}{}
	}
	for k, v := range context {
		b.context[k] = v
	}
	return b
}

// Metadata attaches metadata to the workflow (merged across calls).
func (b *WorkflowBuilder) Metadata(metadata map[string]interface{}) *WorkflowBuilder {
	if b.metadata == nil {
		b.metadata = map[string]interface{}{}
	}
	for k, v := range metadata {
		b.metadata[k] = v
	}
	return b
}

// Build validates the DAG (duplicate names, dangling deps, cycles) and returns
// the request body. A structurally invalid DAG returns an error before any
// network round-trip.
func (b *WorkflowBuilder) Build() (*CreateWorkflowRequest, error) {
	if b.name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}
	if len(b.steps) == 0 {
		return nil, fmt.Errorf("workflow %q has no steps", b.name)
	}
	names := map[string]bool{}
	for _, s := range b.steps {
		if names[s.Name] {
			return nil, fmt.Errorf("duplicate step name %q", s.Name)
		}
		names[s.Name] = true
	}
	for _, s := range b.steps {
		for _, dep := range s.DependsOn {
			if !names[dep] {
				return nil, fmt.Errorf("step %q depends on unknown step %q", s.Name, dep)
			}
		}
	}
	if err := b.assertAcyclic(); err != nil {
		return nil, err
	}
	req := NewCreateWorkflowRequest(b.name, b.steps)
	if b.context != nil {
		req.Context = b.context
	}
	if b.metadata != nil {
		req.Metadata = b.metadata
	}
	return req, nil
}

func (b *WorkflowBuilder) assertAcyclic() error {
	byName := map[string][]string{}
	for _, s := range b.steps {
		byName[s.Name] = s.DependsOn
	}
	visiting := map[string]bool{}
	done := map[string]bool{}
	var visit func(node string) error
	visit = func(node string) error {
		if done[node] {
			return nil
		}
		if visiting[node] {
			return fmt.Errorf("dependency cycle at %q", node)
		}
		visiting[node] = true
		for _, dep := range byName[node] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[node] = false
		done[node] = true
		return nil
	}
	for _, s := range b.steps {
		if err := visit(s.Name); err != nil {
			return err
		}
	}
	return nil
}
