//! Dependency-graph validation and analysis for workflows.
//!
//! Steps form a directed acyclic graph addressed by name. Validation happens
//! once at creation time (cheap, O(V+E)) so cycles and dangling dependencies
//! are reported as `400`s before anything is enqueued.

use std::collections::{HashMap, VecDeque};

use crate::domain::WorkflowStep;

/// Why a set of steps is not a valid DAG.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CycleError {
    #[error("workflow must contain at least one step")]
    Empty,
    #[error("duplicate step name: {0}")]
    DuplicateStepName(String),
    #[error("step '{step}' depends on unknown step '{dep}'")]
    MissingDependency { step: String, dep: String },
    #[error("workflow has a dependency cycle involving: {0}")]
    Cycle(String),
}

/// A validated workflow DAG. Construction proves there are no duplicate names,
/// no dangling dependencies, and no cycles.
#[derive(Debug)]
pub struct DependencyGraph<'a> {
    steps: &'a [WorkflowStep],
    order: Vec<usize>,
}

impl<'a> DependencyGraph<'a> {
    /// Validate `steps` and compute a topological order, or fail.
    pub fn new(steps: &'a [WorkflowStep]) -> Result<Self, CycleError> {
        if steps.is_empty() {
            return Err(CycleError::Empty);
        }

        let mut by_name: HashMap<&str, usize> = HashMap::with_capacity(steps.len());
        for (i, s) in steps.iter().enumerate() {
            if by_name.insert(s.name.as_str(), i).is_some() {
                return Err(CycleError::DuplicateStepName(s.name.clone()));
            }
        }

        for s in steps {
            for d in &s.depends_on {
                if !by_name.contains_key(d.as_str()) {
                    return Err(CycleError::MissingDependency {
                        step: s.name.clone(),
                        dep: d.clone(),
                    });
                }
            }
        }

        // Kahn's algorithm for topological sort + cycle detection.
        let n = steps.len();
        let mut indegree = vec![0usize; n];
        let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, s) in steps.iter().enumerate() {
            for d in &s.depends_on {
                let di = by_name[d.as_str()];
                adjacency[di].push(i);
                indegree[i] += 1;
            }
        }

        let mut queue: VecDeque<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
        let mut order = Vec::with_capacity(n);
        while let Some(u) = queue.pop_front() {
            order.push(u);
            for &v in &adjacency[u] {
                indegree[v] -= 1;
                if indegree[v] == 0 {
                    queue.push_back(v);
                }
            }
        }

        if order.len() != n {
            let mut involved: Vec<String> = (0..n)
                .filter(|&i| indegree[i] > 0)
                .map(|i| steps[i].name.clone())
                .collect();
            involved.sort();
            return Err(CycleError::Cycle(involved.join(", ")));
        }

        Ok(Self { steps, order })
    }

    /// Step names in a valid execution order.
    pub fn topological_order(&self) -> Vec<&'a str> {
        self.order
            .iter()
            .map(|&i| self.steps[i].name.as_str())
            .collect()
    }

    /// Step names with no dependencies — the initial work to enqueue.
    pub fn roots(&self) -> Vec<&'a str> {
        self.steps
            .iter()
            .filter(|s| s.depends_on.is_empty())
            .map(|s| s.name.as_str())
            .collect()
    }

    /// Render the DAG as a Mermaid `graph TD` document (used by the
    /// `/workflows/{id}/diagram` endpoint).
    pub fn mermaid(&self) -> String {
        let mut out = String::from("graph TD\n");
        for s in self.steps {
            out.push_str(&format!(
                "    {}[\"{}\"]\n",
                sanitize(&s.name),
                escape(&s.name)
            ));
        }
        for s in self.steps {
            for d in &s.depends_on {
                out.push_str(&format!("    {} --> {}\n", sanitize(d), sanitize(&s.name)));
            }
        }
        out
    }
}

/// Mermaid node ids must be identifier-ish; map arbitrary names to a safe token.
fn sanitize(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    if s.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) {
        s.insert(0, 'n');
    }
    s
}

fn escape(name: &str) -> String {
    name.replace('"', "'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::WorkflowStep;

    fn step(name: &str, deps: &[&str]) -> WorkflowStep {
        WorkflowStep {
            name: name.to_string(),
            task_name: "t".into(),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn empty_is_rejected() {
        assert_eq!(DependencyGraph::new(&[]).unwrap_err(), CycleError::Empty);
    }

    #[test]
    fn duplicate_names_rejected() {
        let steps = vec![step("a", &[]), step("a", &[])];
        assert_eq!(
            DependencyGraph::new(&steps).unwrap_err(),
            CycleError::DuplicateStepName("a".into())
        );
    }

    #[test]
    fn dangling_dependency_rejected() {
        let steps = vec![step("a", &["ghost"])];
        assert_eq!(
            DependencyGraph::new(&steps).unwrap_err(),
            CycleError::MissingDependency {
                step: "a".into(),
                dep: "ghost".into()
            }
        );
    }

    #[test]
    fn cycle_detected() {
        let steps = vec![step("a", &["c"]), step("b", &["a"]), step("c", &["b"])];
        match DependencyGraph::new(&steps).unwrap_err() {
            CycleError::Cycle(s) => {
                assert!(s.contains('a') && s.contains('b') && s.contains('c'));
            }
            e => panic!("expected cycle, got {e:?}"),
        }
    }

    #[test]
    fn topological_order_respects_dependencies() {
        let steps = vec![
            step("ship", &["pay"]),
            step("pay", &["validate"]),
            step("validate", &[]),
        ];
        let g = DependencyGraph::new(&steps).unwrap();
        let order = g.topological_order();
        let pos = |n: &str| order.iter().position(|x| *x == n).unwrap();
        assert!(pos("validate") < pos("pay"));
        assert!(pos("pay") < pos("ship"));
        assert_eq!(g.roots(), vec!["validate"]);
    }

    #[test]
    fn mermaid_lists_nodes_and_edges() {
        let steps = vec![step("a", &[]), step("b", &["a"])];
        let g = DependencyGraph::new(&steps).unwrap();
        let m = g.mermaid();
        assert!(m.starts_with("graph TD"));
        assert!(m.contains("a --> b"));
    }
}
