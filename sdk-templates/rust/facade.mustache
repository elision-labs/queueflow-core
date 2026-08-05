/*
Ergonomic facade for the QueueFlow Rust SDK.

Hand-written ergonomics layered on the GENERATED core (apis + models).
Injected at generation time as a supporting file, so it ships with the
generated crate but is never produced by the raw codegen. The wire types and
the per-tag API modules come from the generated files and are never edited.
*/

use std::collections::{HashMap, HashSet};
use std::error;
use std::fmt;
use std::time::{Duration, Instant};

use crate::apis::configuration::Configuration;
use crate::apis::{cron_api, dlq_api, jobs_api, workflows_api};
use crate::models;

/// Errors surfaced by the facade helpers.
#[derive(Debug)]
pub enum FacadeError {
    /// The generated client failed (transport, decode, or a non-2xx response).
    Api(Box<dyn error::Error + Send + Sync>),
    /// A workflow definition failed local validation before any round-trip.
    InvalidWorkflow(String),
    /// A `wait_for_*` poller ran out of time.
    Timeout(String),
}

impl fmt::Display for FacadeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FacadeError::Api(e) => write!(f, "{}", e),
            FacadeError::InvalidWorkflow(msg) | FacadeError::Timeout(msg) => write!(f, "{}", msg),
        }
    }
}

impl error::Error for FacadeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            FacadeError::Api(e) => {
                let e: &(dyn error::Error + 'static) = e.as_ref();
                Some(e)
            }
            _ => None,
        }
    }
}

fn api_err<E: error::Error + Send + Sync + 'static>(e: E) -> FacadeError {
    FacadeError::Api(Box::new(e))
}

fn job_terminal(status: models::JobStatus) -> bool {
    matches!(
        status,
        models::JobStatus::Completed | models::JobStatus::Failed | models::JobStatus::Cancelled
    )
}

fn workflow_terminal(status: models::WorkflowStatus) -> bool {
    matches!(
        status,
        models::WorkflowStatus::Completed
            | models::WorkflowStatus::Failed
            | models::WorkflowStatus::PartiallyFailed
            | models::WorkflowStatus::Cancelled
    )
}

/// QueueFlow is an ergonomic wrapper over the generated API modules.
///
/// ```no_run
/// # async fn demo() -> Result<(), queueflow::FacadeError> {
/// use std::time::Duration;
///
/// let qf = queueflow::QueueFlow::new("http://localhost:8000", "dev");
/// let job = qf.create_job("echo", None).await?;
/// let done = qf
///     .wait_for_job(&job.id, Duration::from_secs(60), Duration::from_millis(500))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct QueueFlow {
    /// The generated client configuration. Drop down to the raw `apis::*`
    /// functions with `&qf.config` for anything the helpers do not cover.
    pub config: Configuration,
}

impl QueueFlow {
    /// Builds a client pointed at `base_url`, authenticating with `token`.
    pub fn new(base_url: &str, token: &str) -> Self {
        let mut config = Configuration::new();
        config.base_path = base_url.trim_end_matches('/').to_owned();
        config.bearer_access_token = Some(token.to_owned());
        QueueFlow { config }
    }

    /// Enqueues a job and returns its freshly-created record.
    pub async fn create_job(
        &self,
        task: &str,
        payload: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<models::Job, FacadeError> {
        let mut req = models::CreateJobRequest::new(task.to_owned());
        req.payload = payload;
        let created = jobs_api::create_job(&self.config, req, None)
            .await
            .map_err(api_err)?;
        jobs_api::get_job(&self.config, &created.job_id)
            .await
            .map_err(api_err)
    }

    /// Polls until the job reaches a terminal state or `timeout` elapses.
    pub async fn wait_for_job(
        &self,
        job_id: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<models::Job, FacadeError> {
        let deadline = Instant::now() + timeout;
        loop {
            let job = jobs_api::get_job(&self.config, job_id)
                .await
                .map_err(api_err)?;
            if job_terminal(job.status) {
                return Ok(job);
            }
            if Instant::now() + interval > deadline {
                return Err(FacadeError::Timeout(format!(
                    "job {} did not finish within {:?}",
                    job_id, timeout
                )));
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Creates a workflow from a builder and returns the freshly-created
    /// record.
    pub async fn create_workflow(
        &self,
        builder: &WorkflowBuilder,
    ) -> Result<models::Workflow, FacadeError> {
        let body = builder.build()?;
        let created = workflows_api::create_workflow(&self.config, body)
            .await
            .map_err(api_err)?;
        workflows_api::get_workflow(&self.config, &created.workflow_id)
            .await
            .map_err(api_err)
    }

    /// Registers a recurring enqueue (5-field crontab, evaluated in UTC) and
    /// returns the schedule id. Drop down to [`crate::apis::cron_api`] for
    /// list, pause, resume, and delete.
    pub async fn create_cron(
        &self,
        name: &str,
        schedule: &str,
        task: &str,
    ) -> Result<String, FacadeError> {
        let req = models::CreateCronRequest::new(
            name.to_owned(),
            schedule.to_owned(),
            task.to_owned(),
        );
        let resp = cron_api::create_cron(&self.config, req)
            .await
            .map_err(api_err)?;
        Ok(resp.cron_id)
    }

    /// Re-runs a dead-lettered job as a fresh job and returns the new job
    /// record. Each entry replays at most once (a second replay is a 409).
    /// Drop down to [`crate::apis::dlq_api`] to list and inspect entries.
    pub async fn replay_dead_letter(&self, id: i64) -> Result<models::Job, FacadeError> {
        let resp = dlq_api::replay_dead_letter(&self.config, id)
            .await
            .map_err(api_err)?;
        jobs_api::get_job(&self.config, &resp.job_id)
            .await
            .map_err(api_err)
    }

    /// Polls until the workflow reaches a terminal state or `timeout` elapses.
    pub async fn wait_for_workflow(
        &self,
        workflow_id: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<models::Workflow, FacadeError> {
        let deadline = Instant::now() + timeout;
        loop {
            let wf = workflows_api::get_workflow(&self.config, workflow_id)
                .await
                .map_err(api_err)?;
            if workflow_terminal(wf.status) {
                return Ok(wf);
            }
            if Instant::now() + interval > deadline {
                return Err(FacadeError::Timeout(format!(
                    "workflow {} did not finish within {:?}",
                    workflow_id, timeout
                )));
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// A typed builder for a workflow DAG with local validation.
pub struct WorkflowBuilder {
    name: String,
    steps: Vec<models::WorkflowStep>,
}

impl WorkflowBuilder {
    /// Starts a workflow definition named `name`.
    pub fn new(name: &str) -> Self {
        WorkflowBuilder {
            name: name.to_owned(),
            steps: Vec::new(),
        }
    }

    /// Adds a step running `task`, gated on the named `after` steps.
    pub fn step(mut self, name: &str, task: &str, after: &[&str]) -> Self {
        let mut step = models::WorkflowStep::new(name.to_owned(), task.to_owned());
        if !after.is_empty() {
            step.depends_on = Some(after.iter().map(|d| d.to_string()).collect());
        }
        self.steps.push(step);
        self
    }

    /// Validates the DAG (duplicate names, dangling deps, cycles) and returns
    /// the request body. A structurally invalid DAG returns an error before
    /// any network round-trip.
    pub fn build(&self) -> Result<models::CreateWorkflowRequest, FacadeError> {
        if self.name.is_empty() {
            return Err(FacadeError::InvalidWorkflow(
                "workflow name is required".to_owned(),
            ));
        }
        if self.steps.is_empty() {
            return Err(FacadeError::InvalidWorkflow(format!(
                "workflow {:?} has no steps",
                self.name
            )));
        }
        let mut names = HashSet::new();
        for step in &self.steps {
            if !names.insert(step.name.as_str()) {
                return Err(FacadeError::InvalidWorkflow(format!(
                    "duplicate step name {:?}",
                    step.name
                )));
            }
        }
        for step in &self.steps {
            for dep in step.depends_on.iter().flatten() {
                if !names.contains(dep.as_str()) {
                    return Err(FacadeError::InvalidWorkflow(format!(
                        "step {:?} depends on unknown step {:?}",
                        step.name, dep
                    )));
                }
            }
        }
        assert_acyclic(&self.steps)?;
        Ok(models::CreateWorkflowRequest::new(
            self.name.clone(),
            self.steps.clone(),
        ))
    }
}

fn assert_acyclic(steps: &[models::WorkflowStep]) -> Result<(), FacadeError> {
    fn visit<'a>(
        node: &'a str,
        deps_of: &HashMap<&'a str, &'a [String]>,
        visiting: &mut HashSet<&'a str>,
        done: &mut HashSet<&'a str>,
    ) -> Result<(), FacadeError> {
        if done.contains(node) {
            return Ok(());
        }
        if !visiting.insert(node) {
            return Err(FacadeError::InvalidWorkflow(format!(
                "dependency cycle at {:?}",
                node
            )));
        }
        if let Some(deps) = deps_of.get(node) {
            for dep in deps.iter() {
                visit(dep, deps_of, visiting, done)?;
            }
        }
        visiting.remove(node);
        done.insert(node);
        Ok(())
    }

    let deps_of: HashMap<&str, &[String]> = steps
        .iter()
        .map(|s| {
            (
                s.name.as_str(),
                s.depends_on.as_deref().unwrap_or(&[]),
            )
        })
        .collect();
    let mut visiting = HashSet::new();
    let mut done = HashSet::new();
    for step in steps {
        visit(step.name.as_str(), &deps_of, &mut visiting, &mut done)?;
    }
    Ok(())
}
