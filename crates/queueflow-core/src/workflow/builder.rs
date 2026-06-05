//! Ergonomic builder DSL for assembling workflows in Rust code.
//!
//! ```
//! use queueflow_core::workflow::{WorkflowBuilder, StepBuilder};
//! use queueflow_core::OnFailure;
//! use serde_json::json;
//!
//! let req = WorkflowBuilder::new("order_123")
//!     .step(StepBuilder::new("validate").task("validate_order").payload_value("order_id", json!("123")))
//!     .step(StepBuilder::new("pay").task("process_payment").after("validate"))
//!     .step(StepBuilder::new("ship").task("create_shipment").after("pay").on_failure(OnFailure::Continue))
//!     .build()
//!     .expect("valid DAG");
//! assert_eq!(req.steps.len(), 3);
//! ```

use crate::domain::*;
use crate::workflow::dag::{CycleError, DependencyGraph};

/// Builds a single [`WorkflowStep`].
#[derive(Clone, Debug)]
pub struct StepBuilder {
    step: WorkflowStep,
}

impl StepBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            step: WorkflowStep {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    /// The registered task handler this step invokes (defaults to the step name).
    pub fn task(mut self, task_name: impl Into<String>) -> Self {
        self.step.task_name = task_name.into();
        self
    }

    pub fn payload(mut self, payload: Map) -> Self {
        self.step.payload = payload;
        self
    }

    pub fn payload_value(mut self, key: impl Into<String>, value: Json) -> Self {
        self.step.payload.insert(key.into(), value);
        self
    }

    /// Declare that this step runs after `dep` completes.
    pub fn after(mut self, dep: impl Into<String>) -> Self {
        self.step.depends_on.push(dep.into());
        self
    }

    pub fn depends_on<I, S>(mut self, deps: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.step.depends_on = deps.into_iter().map(Into::into).collect();
        self
    }

    pub fn config(mut self, config: JobConfig) -> Self {
        self.step.config = Some(config);
        self
    }

    pub fn on_failure(mut self, on_failure: OnFailure) -> Self {
        self.step.on_failure = on_failure;
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: Json) -> Self {
        self.step.metadata.insert(key.into(), value);
        self
    }

    fn finish(mut self) -> WorkflowStep {
        if self.step.task_name.is_empty() {
            self.step.task_name = self.step.name.clone();
        }
        self.step
    }
}

/// Assembles a [`CreateWorkflowRequest`], validating the DAG on `build()`.
#[derive(Clone, Debug)]
pub struct WorkflowBuilder {
    name: String,
    steps: Vec<WorkflowStep>,
    context: Map,
    metadata: Map,
}

impl WorkflowBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            context: Map::new(),
            metadata: Map::new(),
        }
    }

    pub fn step(mut self, step: StepBuilder) -> Self {
        self.steps.push(step.finish());
        self
    }

    pub fn add_step(mut self, step: WorkflowStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Seed the workflow's shared context (visible to every step under
    /// `_context`).
    pub fn context(mut self, key: impl Into<String>, value: Json) -> Self {
        self.context.insert(key.into(), value);
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: Json) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Validate the DAG (unique names, resolvable + acyclic dependencies) and
    /// produce the request.
    pub fn build(self) -> Result<CreateWorkflowRequest, CycleError> {
        DependencyGraph::new(&self.steps)?;
        Ok(CreateWorkflowRequest {
            name: self.name,
            steps: self.steps,
            context: self.context,
            metadata: self.metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_valid_linear_workflow() {
        let req = WorkflowBuilder::new("wf")
            .step(StepBuilder::new("a").task("ta"))
            .step(StepBuilder::new("b").after("a"))
            .build()
            .unwrap();
        assert_eq!(req.steps.len(), 2);
        // task defaults to the step name when omitted
        assert_eq!(req.steps[1].task_name, "b");
        assert_eq!(req.steps[1].depends_on, vec!["a".to_string()]);
    }

    #[test]
    fn rejects_cycle_at_build() {
        let err = WorkflowBuilder::new("wf")
            .step(StepBuilder::new("a").after("b"))
            .step(StepBuilder::new("b").after("a"))
            .build()
            .unwrap_err();
        assert!(matches!(err, CycleError::Cycle(_)));
    }
}
