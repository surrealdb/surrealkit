//! One query, checked against a project's schema without executing it.
//!
//! [`check_query`] is `surrealkit check` for a single piece of SurrealQL that is not a file:
//! the text an editor holds, a query an agent is about to send. It applies the schema of the
//! modules asked for to the same hermetic in-memory engine `check` uses, then checks the text
//! against it, under the project's `[analyze]` policy and the suppressions written in the
//! text's own comments. Nothing is executed and nothing connects.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::Serialize;

use super::discover::{Role, Source, Sources, discover_schema};
use super::engine::SchemaApply;
use super::policy::Policy;
use super::render::{self, JsonFinding};
use super::run;
use crate::project::ProjectConfig;
use crate::selection::select_modules;
use crate::variables::TemplateVars;

/// The name findings are reported under when the request gives none.
const DEFAULT_NAME: &str = "<query>";

/// One query, checked against the project's schema without executing it.
#[derive(Debug, Clone, Default)]
pub struct QueryCheckRequest {
	/// The SurrealQL text.
	pub text: String,
	/// The name findings are reported under, in place of a file path (default `"<query>"`).
	pub name: Option<String>,
	/// A `[target.<name>]`: the schema is the modules this target accepts. Nothing connects.
	pub target: Option<String>,
	/// `--schema` modules, intersected with the target's when both are given.
	pub schema: Vec<String>,
}

/// What checking one query found.
#[derive(Debug, Serialize)]
pub struct QueryCheck {
	/// The findings in the query, in the same shape as an entry of `check --json`'s
	/// `diagnostics`. Spans, lines and columns are within the query text, and `path` is the
	/// request's name.
	pub diagnostics: Vec<JsonFinding>,
	/// Whether no finding is an error.
	pub passed: bool,
	pub errors: usize,
	pub warnings: usize,
	pub hints: usize,
	/// Findings an in-source `allow` in the query silenced.
	pub suppressed: usize,
	/// Findings policy silenced.
	pub allowed: usize,
	/// Whether the query itself was analyzed. The query's findings come from the engine's own
	/// checker, which arrives with SurrealDB 4.1; until then this is `false` and a query with
	/// any number of mistakes passes.
	pub analyzed: bool,
	/// How many errors the schema had when it was applied, after policy and suppressions. The
	/// findings themselves are `check`'s to report; a count above zero says the query was
	/// checked against a schema that did not apply cleanly.
	pub schema_errors: usize,
}

/// The schema modules a request names: `request.schema` as for `compile`, and then the ones
/// `request.target` accepts. An undeclared target is an error naming the declared ones.
fn modules_of(
	project: &ProjectConfig,
	request: &QueryCheckRequest,
) -> Result<Vec<crate::module::Module>> {
	let modules = select_modules(project, &request.schema, false, false)?;
	let Some(name) = &request.target else {
		return Ok(modules);
	};
	let Some(target) = project.target.get(name) else {
		bail!(
			"unknown target {name:?}; declared targets are: {}",
			if project.target.is_empty() {
				"(none)".to_string()
			} else {
				project.target.keys().cloned().collect::<Vec<_>>().join(", ")
			}
		);
	};
	let accepted: Vec<_> = modules
		.into_iter()
		.filter(|module| {
			target.schemas.as_ref().is_none_or(|s| s.iter().any(|n| n == module.name()))
		})
		.collect();
	if accepted.is_empty() {
		bail!(
			"target {name:?} accepts none of the selected schema modules, so there is no schema \
			 to check the query against; pick a target whose `schemas` list includes them"
		);
	}
	Ok(accepted)
}

/// Check `request.text` against the schema of `project`'s modules, without executing it.
///
/// The modules are `request.schema` (as for `compile`: every declared module when it is empty)
/// and, when `request.target` names a `[target.<name>]`, only those it accepts. Their schema
/// files are read from `folder` (a relative one is under `root`; the process working directory
/// is never read) and applied to a fresh engine with `[typegen] experimental`, `vars` being
/// the template variables. A schema that does not apply cleanly does not stop the check; it
/// shows as [`QueryCheck::schema_errors`].
///
/// The query's own findings are then graded by `[analyze]` policy, with the suppressions its
/// comments carry. Findings of the schema files are not part of the result, and neither is
/// `K9201`, which is for host code. The query's findings come from the engine's checker once
/// SurrealDB 4.1 is in: until then [`QueryCheck::analyzed`] is `false` and only a malformed
/// suppression comment is found.
pub async fn check_query(
	project: &ProjectConfig,
	root: &Path,
	folder: &str,
	vars: &TemplateVars,
	request: &QueryCheckRequest,
) -> Result<QueryCheck> {
	let policy = Policy::from_config(&project.analyze)?;
	let modules = modules_of(project, request)?;
	let schema = discover_schema(project, folder, root, &modules)?;

	let mut engine = SchemaApply::new(&project.typegen.experimental).await?;
	let applied = run(
		&mut engine,
		&Sources {
			root: root.to_path_buf(),
			schema,
			queries: Vec::new(),
			schema_dirs: Vec::new(),
		},
		&policy,
		vars,
	)
	.await?;

	let name = request.name.as_deref().filter(|name| !name.is_empty()).unwrap_or(DEFAULT_NAME);
	let query = Source {
		path: PathBuf::from(name),
		text: request.text.clone(),
		role: Role::Query,
		module: None,
	};
	let checked = run(
		&mut engine,
		&Sources {
			root: root.to_path_buf(),
			schema: Vec::new(),
			queries: vec![query],
			schema_dirs: Vec::new(),
		},
		&policy,
		vars,
	)
	.await?;

	let diagnostics = checked
		.findings
		.iter()
		.map(|finding| {
			let mut diagnostic = render::json_finding(&checked, finding, Path::new(""));
			// The name is not a path under any directory: show it as it was given.
			diagnostic.path = name.to_string();
			diagnostic
		})
		.collect();
	let summary = &checked.summary;
	Ok(QueryCheck {
		diagnostics,
		passed: checked.passed(),
		errors: summary.errors,
		warnings: summary.warnings,
		hints: summary.hints,
		suppressed: summary.suppressed,
		allowed: summary.allowed,
		analyzed: summary.queries_analyzed,
		schema_errors: applied.summary.errors,
	})
}

#[cfg(test)]
mod tests {
	use tempfile::TempDir;

	use super::*;
	use crate::check::finding::{L7013, Severity};

	/// A project: `surrealkit.toml` and the files, written under a temp directory.
	fn fixture(config: &str, files: &[(&str, &str)]) -> (TempDir, ProjectConfig) {
		let dir = TempDir::new().expect("tempdir");
		std::fs::write(dir.path().join("surrealkit.toml"), config).expect("config");
		for (path, text) in files {
			let path = dir.path().join(path);
			std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
			std::fs::write(path, text).expect("file");
		}
		let project = ProjectConfig::parse(config).expect("config parses");
		(dir, project)
	}

	async fn check(
		dir: &TempDir,
		project: &ProjectConfig,
		request: &QueryCheckRequest,
	) -> Result<QueryCheck> {
		let folder = dir.path().join("database").to_string_lossy().into_owned();
		check_query(project, dir.path(), &folder, &TemplateVars::default(), request).await
	}

	fn request(text: &str) -> QueryCheckRequest {
		QueryCheckRequest {
			text: text.to_string(),
			..QueryCheckRequest::default()
		}
	}

	const BAD_DIRECTIVE: &str = "SELECT 1;\n-- surreal: allow(*)\nSELECT 2;\n";

	#[tokio::test]
	async fn a_clean_query_has_no_findings_and_is_not_analyzed_yet() {
		let (dir, project) =
			fixture("", &[("database/schema/a.surql", "DEFINE TABLE t SCHEMAFULL;")]);
		let found = check(&dir, &project, &request("SELECT * FROM t;")).await.unwrap();
		assert!(found.diagnostics.is_empty());
		assert!(found.passed);
		assert!(!found.analyzed);
		assert_eq!(found.schema_errors, 0);
	}

	#[tokio::test]
	async fn findings_are_shown_under_the_name_and_positioned_in_the_text() {
		let (dir, project) = fixture("", &[]);
		let mut asked = request(BAD_DIRECTIVE);
		let found = check(&dir, &project, &asked).await.unwrap();
		assert_eq!(found.diagnostics.len(), 1);
		let diagnostic = &found.diagnostics[0];
		assert_eq!(diagnostic.code, L7013);
		assert_eq!(diagnostic.path, "<query>");
		assert_eq!((diagnostic.span.line, diagnostic.span.column), (2, 1));
		assert!(BAD_DIRECTIVE[diagnostic.span.start..diagnostic.span.end].contains("allow(*)"));
		assert_eq!((found.warnings, found.errors), (1, 0));
		assert!(found.passed);

		asked.name = Some("src/queries.ts#list".to_string());
		let found = check(&dir, &project, &asked).await.unwrap();
		assert_eq!(found.diagnostics[0].path, "src/queries.ts#list");
	}

	#[tokio::test]
	async fn policy_grades_what_the_query_finds() {
		let (dir, project) = fixture("[analyze.lints]\nL7013 = \"allow\"\n", &[]);
		let found = check(&dir, &project, &request(BAD_DIRECTIVE)).await.unwrap();
		assert!(found.diagnostics.is_empty());
		assert_eq!((found.allowed, found.warnings), (1, 0));

		let (dir, project) = fixture("[analyze]\nwarnings_as_errors = true\n", &[]);
		let found = check(&dir, &project, &request(BAD_DIRECTIVE)).await.unwrap();
		assert_eq!(found.diagnostics[0].severity, Severity::Error);
		assert!(!found.passed);
	}

	#[tokio::test]
	async fn a_broken_schema_is_counted_and_its_findings_are_not_the_queries() {
		let (dir, project) =
			fixture("", &[("database/schema/a.surql", "DEFINE FIELD x.y ON nope TYPE string;")]);
		let found = check(&dir, &project, &request("SELECT 1;")).await.unwrap();
		assert!(found.diagnostics.is_empty());
		assert!(found.passed, "the query is clean");
		assert_eq!(found.schema_errors, 1);
	}

	#[tokio::test]
	async fn an_undeclared_target_is_an_error_naming_the_declared_ones() {
		let (dir, project) = fixture("[target.dev]\n", &[]);
		let mut asked = request("SELECT 1;");
		asked.target = Some("prod".to_string());
		let message = format!("{:#}", check(&dir, &project, &asked).await.unwrap_err());
		assert!(message.contains("prod") && message.contains("dev"), "{message}");
	}
}
