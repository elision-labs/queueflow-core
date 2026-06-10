//! CLI subcommands that talk to a running QueueFlow server via
//! `queueflow-client`. All output is JSON on stdout (logs go to stderr), so
//! the commands compose with `jq` and scripts.

use std::time::Duration;

use anyhow::Context;
use queueflow_client::{Client, CreateJobOptions, ListQuery};
use queueflow_core::{CreateWorkflowRequest, Map};

use crate::cli::{ClientArgs, JobCommand, WorkflowCommand};

fn client(args: &ClientArgs) -> Client {
    Client::new(&args.server_url, &args.token)
}

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn parse_payload(raw: &str) -> anyhow::Result<Map> {
    serde_json::from_str(raw).context("payload must be a JSON object")
}

pub async fn job(cmd: JobCommand) -> anyhow::Result<()> {
    match cmd {
        JobCommand::Create {
            client: args,
            task,
            payload,
            queue,
            max_retries,
            timeout_secs,
            idempotency_key,
            run_at,
            wait,
        } => {
            let c = client(&args);
            let id = c
                .create_job(
                    &task,
                    parse_payload(&payload)?,
                    CreateJobOptions {
                        queue,
                        priority: None,
                        max_retries,
                        timeout_secs,
                        idempotency_key,
                        run_at,
                    },
                )
                .await?;
            if wait {
                let job = c
                    .wait_for_job(&id, Duration::from_millis(500), Duration::from_secs(600))
                    .await?;
                print_json(&job)
            } else {
                print_json(&serde_json::json!({ "job_id": id }))
            }
        }
        JobCommand::Get { client: args, id } => print_json(&client(&args).get_job(&id).await?),
        JobCommand::List {
            client: args,
            status,
            queue,
            limit,
            offset,
            include_total,
        } => {
            let page = client(&args)
                .list_jobs(&ListQuery {
                    status,
                    queue,
                    limit,
                    offset,
                    include_total,
                })
                .await?;
            print_json(&serde_json::json!({
                "jobs": page.jobs,
                "has_more": page.has_more,
                "total": page.total,
            }))
        }
        JobCommand::Cancel { client: args, id } => {
            client(&args).cancel_job(&id).await?;
            print_json(&serde_json::json!({ "cancelled": id }))
        }
        JobCommand::Watch {
            client: args,
            id,
            timeout_secs,
        } => {
            let job = client(&args)
                .wait_for_job(
                    &id,
                    Duration::from_millis(500),
                    Duration::from_secs(timeout_secs),
                )
                .await?;
            print_json(&job)
        }
    }
}

pub async fn workflow(cmd: WorkflowCommand) -> anyhow::Result<()> {
    match cmd {
        WorkflowCommand::Create { client: args, file } => {
            let raw = if file == "-" {
                std::io::read_to_string(std::io::stdin())?
            } else {
                std::fs::read_to_string(&file).with_context(|| format!("read {file}"))?
            };
            let req: CreateWorkflowRequest =
                serde_json::from_str(&raw).context("workflow definition JSON")?;
            let id = client(&args).create_workflow(&req).await?;
            print_json(&serde_json::json!({ "workflow_id": id }))
        }
        WorkflowCommand::Get { client: args, id } => {
            print_json(&client(&args).get_workflow(&id).await?)
        }
        WorkflowCommand::List {
            client: args,
            status,
            limit,
            offset,
            include_total,
        } => {
            let page = client(&args)
                .list_workflows(&ListQuery {
                    status,
                    queue: None,
                    limit,
                    offset,
                    include_total,
                })
                .await?;
            print_json(&serde_json::json!({
                "workflows": page.workflows,
                "has_more": page.has_more,
                "total": page.total,
            }))
        }
        WorkflowCommand::Cancel { client: args, id } => {
            client(&args).cancel_workflow(&id).await?;
            print_json(&serde_json::json!({ "cancelled": id }))
        }
        WorkflowCommand::Diagram { client: args, id } => {
            println!("{}", client(&args).workflow_diagram(&id).await?);
            Ok(())
        }
    }
}

pub async fn tasks(args: ClientArgs) -> anyhow::Result<()> {
    print_json(&client(&args).tasks().await?)
}

pub async fn stats(args: ClientArgs) -> anyhow::Result<()> {
    print_json(&client(&args).stats().await?)
}
