//! Read-only project files, served as MCP resources.
//!
//! These are the files an agent otherwise re-reads on every turn: the project
//! config, the schema, rollout manifests, test suites, the saved snapshots.
//!
//! # What is never served
//!
//! `.env` and `.env.local` hold `SURREALDB_PASSWORD`, so there is no family that
//! can reach them. `surrealkit.toml` is safe to serve raw because
//! `ProjectConfig::parse` rejects an inline `pass`/`password` under `[target.*]`
//! outright -- if the file contained one, the project would not have loaded --
//! but the value of any other secret-shaped key is still redacted, since
//! `[variables]` is free-form.
//!
//! Test suites are **not** served raw. `ActorSpec` carries inline `password`,
//! `token` and `username` fields alongside their `*_env` counterparts, so a
//! naive read would leak test credentials.
//!
//! Everything served is contained under the project root by [`safe_join`], which
//! canonicalises and therefore catches a symlink pointing out of the tree.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use rmcp::model::{
	AnnotateAble, RawResource, RawResourceTemplate, Resource, ResourceContents, ResourceTemplate,
};

use super::context::CallContext;
use crate::paths::safe_join;

/// The URI scheme every resource shares.
const SCHEME: &str = "surrealkit";

/// A secret-shaped key whose value is replaced before the file is served.
fn is_secret_key(key: &str) -> bool {
	let key = key.trim().to_ascii_lowercase();
	["pass", "password", "secret", "token", "api_key", "api-key", "apikey"]
		.iter()
		.any(|needle| key == *needle || key.ends_with(&format!("_{needle}")))
}

/// Blank the value of any secret-shaped assignment in a TOML document.
///
/// Deliberately a line scan rather than a parse-and-reserialize: comments in a
/// schema or config file are genuinely useful to an agent, and round-tripping
/// TOML would drop them.
pub fn redact_toml(raw: &str) -> String {
	raw.lines()
		.map(|line| {
			let trimmed = line.trim_start();
			if trimmed.starts_with('#') {
				return line.to_string();
			}
			match line.split_once('=') {
				Some((key, _)) if is_secret_key(key) => {
					let indent = &line[..line.len() - trimmed.len()];
					format!("{indent}{} = \"<redacted>\"", key.trim())
				}
				_ => line.to_string(),
			}
		})
		.collect::<Vec<_>>()
		.join("\n")
}

/// The resource families, as RFC 6570 templates.
///
/// `{+path}` is the *reserved* expansion and is load-bearing: a plain `{path}`
/// percent-encodes `/`, which would break the nested schema paths the bundled
/// template itself ships (`schema/organization/organization_role.surql`).
pub fn templates() -> Vec<ResourceTemplate> {
	vec![
		template(
			"surrealkit://schema/{+path}",
			"Schema file",
			"A .surql schema file under <folder>/schema.",
			"text/plain",
		),
		template(
			"surrealkit://seed/{+path}",
			"Seed file",
			"A .surql seed file under <folder>/seed.",
			"text/plain",
		),
		template(
			"surrealkit://rollout/{+path}",
			"Rollout manifest",
			"A rollout manifest under <folder>/rollouts.",
			"application/toml",
		),
		template(
			"surrealkit://test-suite/{+path}",
			"Test suite",
			"A test suite under <folder>/tests/suites. Credentials are redacted.",
			"application/toml",
		),
	]
}

fn template(uri: &str, name: &str, description: &str, mime: &str) -> ResourceTemplate {
	let mut raw = RawResourceTemplate::new(uri, name.to_string());
	raw.title = Some(name.to_string());
	raw.description = Some(description.to_string());
	raw.mime_type = Some(mime.to_string());
	raw.no_annotation()
}

/// Every resource that currently exists on disk.
///
/// Concrete instances are listed as well as templated, so hosts that do not
/// implement resource templates still see the whole project.
pub fn list(ctx: &CallContext) -> Vec<Resource> {
	let mut out = Vec::new();

	if let Some(config) = &ctx.config_path {
		out.push(resource(
			"surrealkit://config",
			"surrealkit.toml",
			&format!("Project configuration ({})", config.display()),
			"application/toml",
		));
	}
	out.push(resource(
		"surrealkit://types/schema",
		"Generated schema document",
		"The typed schema document written by `typegen`, if it has been generated.",
		"application/json",
	));

	let folder = Path::new(&ctx.folder);
	collect(&mut out, &folder.join("schema"), "schema", "text/plain", "Schema file");
	collect(&mut out, &folder.join("seed"), "seed", "text/plain", "Seed file");
	collect(&mut out, &folder.join("rollouts"), "rollout", "application/toml", "Rollout manifest");
	collect(
		&mut out,
		&folder.join("tests").join("suites"),
		"test-suite",
		"application/toml",
		"Test suite",
	);
	out
}

fn collect(out: &mut Vec<Resource>, dir: &Path, family: &str, mime: &str, kind: &str) {
	let Ok(walker) = std::fs::read_dir(dir) else {
		return;
	};
	let mut entries: Vec<PathBuf> = walkdir::WalkDir::new(dir)
		.follow_links(false)
		.into_iter()
		.filter_map(std::result::Result::ok)
		.filter(|e| e.file_type().is_file())
		.map(|e| e.path().to_path_buf())
		.collect();
	drop(walker);
	entries.sort();

	for path in entries {
		let Ok(rel) = path.strip_prefix(dir) else {
			continue;
		};
		let rel = rel.to_string_lossy().replace('\\', "/");
		out.push(resource(
			&format!("{SCHEME}://{family}/{rel}"),
			&rel,
			&format!("{kind}: {rel}"),
			mime,
		));
	}
}

fn resource(uri: &str, name: &str, description: &str, mime: &str) -> Resource {
	let mut raw = RawResource::new(uri, name.to_string());
	raw.title = Some(name.to_string());
	raw.description = Some(description.to_string());
	raw.mime_type = Some(mime.to_string());
	raw.no_annotation()
}

/// Read one resource by URI.
pub fn read(ctx: &CallContext, uri: &str) -> Result<Vec<ResourceContents>> {
	let rest = uri
		.strip_prefix(&format!("{SCHEME}://"))
		.with_context(|| format!("unsupported resource URI {uri:?}: expected a {SCHEME}:// URI"))?;
	let (family, path) = match rest.split_once('/') {
		Some((family, path)) => (family, path),
		None => (rest, ""),
	};

	let folder = Path::new(&ctx.folder);
	let (base, redact): (PathBuf, bool) = match family {
		"config" => {
			let config = ctx.config_path.clone().context("this project has no surrealkit.toml")?;
			return Ok(vec![text(uri, &redact_toml(&std::fs::read_to_string(&config)?))]);
		}
		"types" => {
			let generated = folder.join("types").join("schema.json");
			let raw = std::fs::read_to_string(&generated).with_context(|| {
				format!(
					"{} has not been generated yet; call the `typegen` tool with write: true",
					generated.display()
				)
			})?;
			return Ok(vec![text(uri, &raw)]);
		}
		"schema" => (folder.join("schema"), false),
		"seed" => (folder.join("seed"), false),
		"rollout" => (folder.join("rollouts"), false),
		// Test suites carry inline `password`/`token` actor fields.
		"test-suite" => (folder.join("tests").join("suites"), true),
		other => bail!(
			"unknown resource family {other:?}; expected one of: config, types, schema, seed, \
			 rollout, test-suite"
		),
	};

	let resolved = safe_join(&base, path)?;
	let raw = std::fs::read_to_string(&resolved)
		.with_context(|| format!("reading {}", resolved.display()))?;
	let body = if redact {
		redact_toml(&raw)
	} else {
		raw
	};
	Ok(vec![text(uri, &body)])
}

fn text(uri: &str, contents: &str) -> ResourceContents {
	ResourceContents::text(contents, uri)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn secret_shaped_keys_are_redacted() {
		let raw = indoc::indoc! {r#"
			[variables]
			region = "eu-west-1"
			api_key = "sk-live-abcdef"
			password = "hunter2"
		"#};
		let out = redact_toml(raw);
		assert!(out.contains("region = \"eu-west-1\""), "ordinary values survive: {out}");
		assert!(!out.contains("sk-live-abcdef"), "api_key leaked: {out}");
		assert!(!out.contains("hunter2"), "password leaked: {out}");
		assert!(out.contains("api_key = \"<redacted>\""), "got: {out}");
	}

	#[test]
	fn pass_env_names_survive_redaction() {
		// The variable *name* is committed to the config and useful; only values
		// of secret-shaped keys are blanked.
		let out = redact_toml("[target.prod]\npass_env = \"PROD_DB_PASSWORD\"\n");
		assert!(out.contains("PROD_DB_PASSWORD"), "got: {out}");
	}

	#[test]
	fn comments_are_preserved() {
		let out = redact_toml("# password = \"in a comment\"\nregion = \"x\"\n");
		assert!(out.contains("# password = \"in a comment\""), "got: {out}");
	}
}
