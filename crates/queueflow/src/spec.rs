//! `queueflow spec` — emit the OpenAPI document for SDK generation.

use std::fs;
use std::path::Path;

use anyhow::Context;
use queueflow_api::ApiDoc;
use utoipa::OpenApi;

/// Write `openapi.json` and `openapi.yaml` into `output_dir`. Runs offline — no
/// server, no database — so CI can regenerate SDKs deterministically.
pub fn write_spec(output_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    let doc = ApiDoc::openapi();
    let json = doc.to_pretty_json().context("serialize OpenAPI JSON")?;
    let yaml = doc.to_yaml().context("serialize OpenAPI YAML")?;

    let json_path = output_dir.join("openapi.json");
    let yaml_path = output_dir.join("openapi.yaml");
    fs::write(&json_path, json).with_context(|| format!("write {}", json_path.display()))?;
    fs::write(&yaml_path, yaml).with_context(|| format!("write {}", yaml_path.display()))?;

    println!("wrote {}", json_path.display());
    println!("wrote {}", yaml_path.display());
    Ok(())
}
