/*
Ergonomic facade for the QueueFlow Go SDK.

Hand-written ergonomics layered on the GENERATED core (APIClient + models).
Injected at generation time as a supporting file, so it ships with the
generated package but is never produced by the raw codegen. The wire types and
the per-tag API services come from the generated files and are never edited.
*/

package queueflow

import (
	"context"
	"fmt"
	"time"
)

var terminalJob = map[string]bool{"completed": true, "failed": true, "cancelled": true}
var terminalWorkflow = map[string]bool{"completed": true, "failed": true, "partially_failed": true, "cancelled": true}

// QueueFlow is an ergonomic wrapper over the generated APIClient.
//
//	qf := queueflow.NewQueueFlow("http://localhost:8000", "dev")
//	job, _ := qf.CreateJob("echo", map[string]any{"hi": 1})
//	done, _ := qf.WaitForJob(job.Id, 60*time.Second, 500*time.Millisecond)
type QueueFlow struct {
	Client *APIClient
	ctx    context.Context
}

// NewQueueFlow builds a client pointed at baseURL, authenticating with token.
func NewQueueFlow(baseURL, token string) *QueueFlow {
	cfg := NewConfiguration()
	server := ServerConfiguration{URL: baseURL}
	cfg.Servers = ServerConfigurations{server}
	cfg.AddDefaultHeader("Authorization", "Bearer "+token)
	return &QueueFlow{Client: NewAPIClient(cfg), ctx: context.Background()}
}

// CreateJob enqueues a job and returns its freshly-created record.
func (q *QueueFlow) CreateJob(task string, payload map[string]interface{}) (*Job, error) {
	req := CreateJobRequest{TaskName: task}
	if payload != nil {
		req.Payload = payload
	}
	created, _, err := q.Client.JobsAPI.CreateJob(q.ctx).CreateJobRequest(req).Execute()
	if err != nil {
		return nil, err
	}
	job, _, err := q.Client.JobsAPI.GetJob(q.ctx, created.JobId).Execute()
	return job, err
}

// WaitForJob polls until the job reaches a terminal state or timeout elapses.
func (q *QueueFlow) WaitForJob(jobID string, timeout, interval time.Duration) (*Job, error) {
	deadline := time.Now().Add(timeout)
	for {
		job, _, err := q.Client.JobsAPI.GetJob(q.ctx, jobID).Execute()
		if err != nil {
			return nil, err
		}
		if terminalJob[string(job.Status)] {
			return job, nil
		}
		if time.Now().Add(interval).After(deadline) {
			return nil, fmt.Errorf("job %s did not finish within %s", jobID, timeout)
		}
		time.Sleep(interval)
	}
}

// CreateCron registers a recurring enqueue (5-field crontab, evaluated in
// UTC) and returns the schedule id. payload is sent with every firing; nil
// means an empty payload. Use q.Client.CronAPI for list, pause, resume, and
// delete.
func (q *QueueFlow) CreateCron(name, cronExpr, task string, payload map[string]interface{}) (string, error) {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	req := CreateCronRequest{Name: name, CronExpr: cronExpr, TaskName: task, Payload: payload}
	resp, _, err := q.Client.CronAPI.CreateCron(q.ctx).CreateCronRequest(req).Execute()
	if err != nil {
		return "", err
	}
	return resp.CronId, nil
}

// ReplayDeadLetter re-runs a dead-lettered job as a fresh job and returns the
// new job record. Each entry replays at most once (a second replay is a 409).
// Use q.Client.DlqAPI to list and inspect entries.
func (q *QueueFlow) ReplayDeadLetter(id int64) (*Job, error) {
	resp, _, err := q.Client.DlqAPI.ReplayDeadLetter(q.ctx, id).Execute()
	if err != nil {
		return nil, err
	}
	job, _, err := q.Client.JobsAPI.GetJob(q.ctx, resp.JobId).Execute()
	return job, err
}

// CreateWorkflow creates a workflow from a builder (or a raw request) and
// returns the freshly-created record.
func (q *QueueFlow) CreateWorkflow(b *WorkflowBuilder) (*Workflow, error) {
	body, err := b.Build()
	if err != nil {
		return nil, err
	}
	created, _, err := q.Client.WorkflowsAPI.CreateWorkflow(q.ctx).CreateWorkflowRequest(*body).Execute()
	if err != nil {
		return nil, err
	}
	wf, _, err := q.Client.WorkflowsAPI.GetWorkflow(q.ctx, created.WorkflowId).Execute()
	return wf, err
}

// WaitForWorkflow polls until the workflow reaches a terminal state.
func (q *QueueFlow) WaitForWorkflow(workflowID string, timeout, interval time.Duration) (*Workflow, error) {
	deadline := time.Now().Add(timeout)
	for {
		wf, _, err := q.Client.WorkflowsAPI.GetWorkflow(q.ctx, workflowID).Execute()
		if err != nil {
			return nil, err
		}
		if terminalWorkflow[string(wf.Status)] {
			return wf, nil
		}
		if time.Now().Add(interval).After(deadline) {
			return nil, fmt.Errorf("workflow %s did not finish within %s", workflowID, timeout)
		}
		time.Sleep(interval)
	}
}

// WorkflowBuilder is a typed builder for a workflow DAG with local validation.
type WorkflowBuilder struct {
	name  string
	steps []WorkflowStep
}

// NewWorkflowBuilder starts a workflow definition named name.
func NewWorkflowBuilder(name string) *WorkflowBuilder {
	return &WorkflowBuilder{name: name}
}

// Step adds a step running task, optionally gated on the named "after" steps.
func (b *WorkflowBuilder) Step(name, task string, after ...string) *WorkflowBuilder {
	s := NewWorkflowStep(name, task)
	if len(after) > 0 {
		s.DependsOn = after
	}
	b.steps = append(b.steps, *s)
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
	return NewCreateWorkflowRequest(b.name, b.steps), nil
}

func (b *WorkflowBuilder) assertAcyclic() error {
	byName := map[string][]string{}
	for _, s := range b.steps {
		byName[s.Name] = s.DependsOn
	}
	visiting := map[string]bool{}
	done := map[string]bool{}
	var visit func(node string, stack []string) error
	visit = func(node string, stack []string) error {
		if done[node] {
			return nil
		}
		if visiting[node] {
			return fmt.Errorf("dependency cycle at %q", node)
		}
		visiting[node] = true
		for _, dep := range byName[node] {
			if err := visit(dep, append(stack, node)); err != nil {
				return err
			}
		}
		visiting[node] = false
		done[node] = true
		return nil
	}
	for _, s := range b.steps {
		if err := visit(s.Name, nil); err != nil {
			return err
		}
	}
	return nil
}
