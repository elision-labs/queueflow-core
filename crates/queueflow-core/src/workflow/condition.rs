//! Step conditions: a small JSON predicate language over the workflow
//! context, evaluated when a step becomes ready.
//!
//! A condition is a tree of `all` / `any` / `not` nodes over leaves. A leaf
//! names a dotted `path` into the context (`validate.ok`, `order.items.0`)
//! and compares the value there with `value` using `op`. Missing paths
//! resolve to `null`: `exists` is false, every comparison is false, and
//! `ne` is true.

use std::cmp::Ordering;

use crate::domain::{ConditionOp, Json, Map, StepCondition};

/// Check a condition's shape: exactly one of `path`, `all`, `any`, `not`;
/// `op` and `value` only with `path`; `op` consistent with `value`.
pub fn validate(cond: &StepCondition) -> Result<(), String> {
    let branches = [
        cond.path.is_some(),
        cond.all.is_some(),
        cond.any.is_some(),
        cond.not.is_some(),
    ]
    .iter()
    .filter(|b| **b)
    .count();
    if branches != 1 {
        return Err("a condition needs exactly one of path, all, any or not".into());
    }
    match &cond.path {
        Some(p) => {
            if p.is_empty() {
                return Err("condition path must not be empty".into());
            }
            match (cond.op.unwrap_or(default_op(cond)), cond.value.is_some()) {
                (ConditionOp::Exists, true) => Err("op exists does not take a value".into()),
                (ConditionOp::Exists, false) => Ok(()),
                (_, false) => Err(format!(
                    "op {} needs a value",
                    serde_json::to_string(&cond.op.unwrap_or_default()).unwrap_or_default()
                )),
                (ConditionOp::In, true) => match &cond.value {
                    Some(Json::Array(_)) => Ok(()),
                    _ => Err("op in needs an array value".into()),
                },
                _ => Ok(()),
            }
        }
        None => {
            if cond.op.is_some() || cond.value.is_some() {
                return Err("op and value are only valid with path".into());
            }
            for member in cond.all.iter().chain(cond.any.iter()).flatten() {
                validate(member)?;
            }
            if let Some(n) = &cond.not {
                validate(n)?;
            }
            if cond.all.as_ref().is_some_and(|v| v.is_empty())
                || cond.any.as_ref().is_some_and(|v| v.is_empty())
            {
                return Err("all and any need at least one member".into());
            }
            Ok(())
        }
    }
}

/// The operator a leaf means when `op` is omitted: `eq` with a value,
/// `exists` without one.
fn default_op(cond: &StepCondition) -> ConditionOp {
    if cond.value.is_some() {
        ConditionOp::Eq
    } else {
        ConditionOp::Exists
    }
}

/// Evaluate a (validated) condition against the context.
pub fn evaluate(cond: &StepCondition, context: &Map) -> bool {
    if let Some(members) = &cond.all {
        return members.iter().all(|m| evaluate(m, context));
    }
    if let Some(members) = &cond.any {
        return members.iter().any(|m| evaluate(m, context));
    }
    if let Some(inner) = &cond.not {
        return !evaluate(inner, context);
    }
    let Some(path) = &cond.path else {
        return false;
    };
    let actual = lookup(context, path);
    let op = cond.op.unwrap_or(default_op(cond));
    let expected = cond.value.as_ref();
    match op {
        ConditionOp::Exists => actual.is_some_and(|v| !v.is_null()),
        ConditionOp::Eq => actual == expected,
        ConditionOp::Ne => actual != expected,
        ConditionOp::Gt => compare(actual, expected) == Some(Ordering::Greater),
        ConditionOp::Gte => matches!(
            compare(actual, expected),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        ConditionOp::Lt => compare(actual, expected) == Some(Ordering::Less),
        ConditionOp::Lte => matches!(
            compare(actual, expected),
            Some(Ordering::Less | Ordering::Equal)
        ),
        ConditionOp::Contains => match (actual, expected) {
            (Some(Json::Array(items)), Some(v)) => items.contains(v),
            (Some(Json::String(s)), Some(Json::String(needle))) => s.contains(needle.as_str()),
            (Some(Json::Object(o)), Some(Json::String(key))) => o.contains_key(key),
            _ => false,
        },
        ConditionOp::In => match (actual, expected) {
            (Some(v), Some(Json::Array(items))) => items.contains(v),
            _ => false,
        },
    }
}

/// Resolve a dotted path against the context. Numeric segments index
/// arrays. Returns `None` when any segment is missing.
pub fn lookup<'a>(context: &'a Map, path: &str) -> Option<&'a Json> {
    let mut segments = path.split('.');
    let first = segments.next()?;
    let mut current = context.get(first)?;
    for seg in segments {
        current = match current {
            Json::Object(o) => o.get(seg)?,
            Json::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Order two JSON scalars: numbers numerically, strings lexicographically,
/// booleans false < true. Mixed or non-scalar operands do not compare.
fn compare(a: Option<&Json>, b: Option<&Json>) -> Option<Ordering> {
    match (a?, b?) {
        (Json::Number(x), Json::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
        (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(v: serde_json::Value) -> Map {
        v.as_object().cloned().unwrap().into_iter().collect()
    }

    fn leaf(path: &str, op: Option<ConditionOp>, value: Option<Json>) -> StepCondition {
        StepCondition {
            path: Some(path.into()),
            op,
            value,
            ..Default::default()
        }
    }

    #[test]
    fn leaves_compare_scalars_and_default_to_eq_or_exists() {
        let c = ctx(
            json!({"validate": {"ok": true, "total": 120.5, "tier": "gold"},
                           "tags": ["a", "b"], "nothing": null}),
        );
        assert!(evaluate(&leaf("validate.ok", None, Some(json!(true))), &c));
        assert!(!evaluate(
            &leaf("validate.ok", None, Some(json!(false))),
            &c
        ));
        assert!(evaluate(
            &leaf("validate.total", Some(ConditionOp::Gt), Some(json!(100))),
            &c
        ));
        assert!(!evaluate(
            &leaf("validate.total", Some(ConditionOp::Lt), Some(json!(100))),
            &c
        ));
        assert!(evaluate(
            &leaf(
                "validate.tier",
                Some(ConditionOp::In),
                Some(json!(["gold", "silver"]))
            ),
            &c
        ));
        assert!(evaluate(
            &leaf("tags", Some(ConditionOp::Contains), Some(json!("b"))),
            &c
        ));
        assert!(evaluate(&leaf("tags.1", None, Some(json!("b"))), &c));
        assert!(evaluate(&leaf("validate", None, None), &c));
        assert!(!evaluate(&leaf("nothing", None, None), &c));
        assert!(!evaluate(&leaf("missing.deep", None, None), &c));
        // A missing path is "not equal" to anything and never greater.
        assert!(evaluate(
            &leaf("missing", Some(ConditionOp::Ne), Some(json!(1))),
            &c
        ));
        assert!(!evaluate(
            &leaf("missing", Some(ConditionOp::Gt), Some(json!(1))),
            &c
        ));
    }

    #[test]
    fn combinators_nest() {
        let c = ctx(json!({"a": 1, "b": 2}));
        let cond = StepCondition {
            all: Some(vec![
                leaf("a", None, Some(json!(1))),
                StepCondition {
                    any: Some(vec![
                        leaf("b", None, Some(json!(3))),
                        StepCondition {
                            not: Some(Box::new(leaf("b", None, Some(json!(3))))),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        assert!(validate(&cond).is_ok());
        assert!(evaluate(&cond, &c));
    }

    #[test]
    fn validation_rejects_malformed_shapes() {
        assert!(validate(&StepCondition::default()).is_err());
        assert!(validate(&StepCondition {
            path: Some("a".into()),
            all: Some(vec![]),
            ..Default::default()
        })
        .is_err());
        assert!(validate(&leaf("a", Some(ConditionOp::Gt), None)).is_err());
        assert!(validate(&leaf("a", Some(ConditionOp::Exists), Some(json!(1)))).is_err());
        assert!(validate(&leaf("a", Some(ConditionOp::In), Some(json!(1)))).is_err());
        assert!(validate(&StepCondition {
            all: Some(vec![]),
            ..Default::default()
        })
        .is_err());
        assert!(validate(&leaf("a", None, Some(json!(1)))).is_ok());
        assert!(validate(&leaf("a", None, None)).is_ok());
    }
}
