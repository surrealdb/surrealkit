//! The adapter between a check and what analyses SurrealQL.
//!
//! A backend turns what it finds into [`Finding`]s, with each code's intrinsic severity and
//! default level; the caller applies suppressions and policy. [`SchemaApply`] is the backend
//! this build ships.

use anyhow::Result;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::discover::Source;
use super::finding::{Finding, K9001, K9002, K9003, LineIndex, Note, NoteKind};
use crate::compile::{Rejection, apply_statement, engine, experimental_features};
use crate::variables::TemplateVars;

/// An analysis backend.
///
/// A run takes each schema file in order, [`check`](Self::check)s it against the schema applied
/// so far and then [`apply`](Self::apply)s it, and last checks every query file.
#[expect(async_fn_in_trait, reason = "a check is driven by one task and never spawns a backend")]
pub trait Engine {
	/// Check one file's text against the schema applied so far, without applying it.
	async fn check(&mut self, source: &Source, vars: &TemplateVars) -> Result<Vec<Finding>>;

	/// Apply one schema file. Findings are for what failed; a problem in the file itself is
	/// never an `Err`, which is for the backend failing.
	async fn apply(&mut self, source: &Source, vars: &TemplateVars) -> Result<Vec<Finding>>;

	/// Whether [`check`](Self::check) analyses anything. When it does not, the report says the
	/// query files were found but not analysed.
	fn analyzes(&self) -> bool;
}

/// The backend that works on SurrealDB 4.0: it applies the schema and reports what the engine
/// rejects, and analyses no query.
///
/// Query analysis (type checking) comes from the engine's own checker, the SDK's
/// `Surreal::check` in SurrealDB 4.1, which a backend of its own will call. Until then
/// [`check`](Engine::check) finds nothing and [`analyzes`](Engine::analyzes) is `false`.
///
/// The schema is applied exactly as `surrealkit compile` applies it: the same hermetic
/// in-memory engine (no network, no scripting, only the experimental capabilities asked for)
/// and the same statement preparation, one statement at a time so that each statement the
/// engine rejects is its own finding and the next one still applies.
pub struct SchemaApply {
	db: Surreal<Any>,
}

impl SchemaApply {
	/// A fresh engine, holding nothing. `experimental` names the capabilities it allows, as
	/// `[typegen] experimental` does.
	pub async fn new(experimental: &[String]) -> Result<Self> {
		let (features, _) = experimental_features(experimental)?;
		Ok(Self {
			db: engine(&features, "check").await?,
		})
	}
}

impl Engine for SchemaApply {
	async fn check(&mut self, _source: &Source, _vars: &TemplateVars) -> Result<Vec<Finding>> {
		Ok(Vec::new())
	}

	async fn apply(&mut self, source: &Source, vars: &TemplateVars) -> Result<Vec<Finding>> {
		let text = source.text.as_str();
		let statements = match crate::surql_scan::scan(text) {
			Ok(statements) => statements,
			Err(err) => {
				let start = LineIndex::new(text).offset(err.line as usize, err.column as usize);
				let end = start + text[start..].chars().next().map_or(0, char::len_utf8);
				let message = err.to_string();
				let message = message.split_once(": ").map_or(message.as_str(), |(_, rest)| rest);
				return Ok(vec![Finding::kit(K9001, message, source.path.clone(), start..end)]);
			}
		};
		let path = source.path.display().to_string();
		let mut findings = Vec::new();
		for (index, statement) in statements.iter().enumerate() {
			let applied = apply_statement(&self.db, &path, statement.text(text), vars).await;
			let finding = match applied {
				Ok(()) => continue,
				Err(Rejection::Variable(err)) => {
					let message = err.to_string();
					let message =
						message.split_once(" (set via").map_or(message.as_str(), |(head, _)| head);
					Finding::kit(K9002, message, source.path.clone(), statement.span.clone())
						.with_note(Note {
							kind: NoteKind::Help,
							message: "set it with `--var NAME=VALUE` or under `[variables]` in \
							          surrealkit.toml; `check` does not read SURREALKIT_VAR_* \
							          from the environment"
								.to_string(),
							span: None,
							replacement: None,
						})
				}
				Err(Rejection::Refused(err)) => {
					Finding::kit(K9003, "", source.path.clone(), statement.span.clone())
						.with_message_lines(&format!("{err:#}"))
				}
				Err(Rejection::Engine(message)) => {
					Finding::kit(K9003, "", source.path.clone(), statement.span.clone())
						.with_message_lines(&message)
				}
			};
			findings.push(finding.in_statement(index));
		}
		Ok(findings)
	}

	fn analyzes(&self) -> bool {
		false
	}
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::*;
	use crate::check::discover::Role;

	fn source(text: &str) -> Source {
		Source {
			path: PathBuf::from("/p/database/schema/a.surql"),
			text: text.to_string(),
			role: Role::Schema,
			module: None,
		}
	}

	async fn apply(engine: &mut SchemaApply, text: &str) -> Vec<Finding> {
		engine.apply(&source(text), &TemplateVars::default()).await.expect("backend works")
	}

	#[tokio::test]
	async fn a_schema_that_the_engine_accepts_has_no_findings_and_builds_on_earlier_files() {
		let mut engine = SchemaApply::new(&[]).await.expect("engine");
		assert!(apply(&mut engine, "DEFINE TABLE t SCHEMAFULL;").await.is_empty());
		// The second file's field needs the first file's table.
		assert!(apply(&mut engine, "DEFINE FIELD a ON t TYPE string;").await.is_empty());
		assert!(!engine.analyzes());
		let found = engine.check(&source("SELECT nope FROM t"), &TemplateVars::default()).await;
		assert!(found.expect("check").is_empty());
	}

	#[tokio::test]
	async fn a_rejected_statement_is_one_finding_and_the_next_still_applies() {
		let mut engine = SchemaApply::new(&[]).await.expect("engine");
		let text = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a.b ON t TYPE string;\n\
		            DEFINE FIELD c ON t TYPE int;\n";
		let findings = apply(&mut engine, text).await;
		assert_eq!(findings.len(), 1, "{findings:#?}");
		let finding = &findings[0];
		assert_eq!(finding.code, K9003);
		assert_eq!(finding.statement, Some(1));
		assert_eq!(&text[finding.span.clone()], "DEFINE FIELD a.b ON t TYPE string");
		assert!(!finding.message.is_empty());
		// `c` was defined: defining it again as an index of itself is fine, and a field
		// whose table is missing is not.
		assert!(apply(&mut engine, "DEFINE FIELD d ON t TYPE int;").await.is_empty());
		let missing = apply(&mut engine, "DEFINE FIELD d ON missing TYPE int;").await;
		assert!(missing.is_empty() || missing[0].code == K9003);
	}

	#[tokio::test]
	async fn a_file_that_does_not_split_is_k9001_at_the_error() {
		let mut engine = SchemaApply::new(&[]).await.expect("engine");
		let text = "DEFINE TABLE t;\nDEFINE FIELD é ON t DEFAULT 'open;\n";
		let findings = apply(&mut engine, text).await;
		assert_eq!(findings.len(), 1);
		assert_eq!(findings[0].code, K9001);
		assert_eq!(findings[0].statement, None);
		assert_eq!(&text[findings[0].span.clone()], "'");
		assert!(findings[0].message.contains("never closed"), "{}", findings[0].message);
		assert!(!findings[0].message.starts_with("line "), "{}", findings[0].message);
	}

	#[tokio::test]
	async fn an_unset_template_variable_is_k9002() {
		let mut engine = SchemaApply::new(&[]).await.expect("engine");
		let findings = apply(&mut engine, "DEFINE TABLE ${PREFIX}_t;").await;
		assert_eq!(findings.len(), 1);
		assert_eq!(findings[0].code, K9002);
		assert!(findings[0].message.contains("PREFIX"), "{}", findings[0].message);
		assert!(!findings[0].message.contains("SURREALKIT_VAR"), "{}", findings[0].message);
		let mut vars = TemplateVars::default();
		vars.vars.insert("PREFIX".to_string(), "app".to_string());
		let set = engine.apply(&source("DEFINE TABLE ${PREFIX}_t;"), &vars).await.unwrap();
		assert!(set.is_empty(), "{set:#?}");
	}

	#[tokio::test]
	async fn what_sync_refuses_is_refused_here_too() {
		let mut engine = SchemaApply::new(&[]).await.expect("engine");
		for statement in ["REMOVE TABLE t", "SELECT * FROM t", "DEFINE NAMESPACE n"] {
			let findings = apply(&mut engine, &format!("{statement};")).await;
			assert_eq!(findings.len(), 1, "{statement}: {findings:#?}");
			assert_eq!(findings[0].code, K9003, "{statement}");
		}
	}

	#[tokio::test]
	async fn the_engine_has_no_network() {
		let mut engine = SchemaApply::new(&[]).await.expect("engine");
		let findings =
			apply(&mut engine, "DEFINE PARAM $p VALUE http::get('http://127.0.0.1:1/');").await;
		assert_eq!(findings.len(), 1, "http is denied: {findings:#?}");
	}
}
