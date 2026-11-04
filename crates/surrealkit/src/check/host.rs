//! Lints of the SurrealQL host code sends: the queries [`host_queries`]
//! finds in TypeScript, JavaScript, Svelte, Vue and Astro sources.
//!
//! One rule, `K9201`: a template literal with `${…}` passed to a query method splices the value
//! into the SurrealQL text. A `surql`-tagged template is a call expression, not a literal, so
//! the extractor never reads it and the rule never fires on it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::finding::{Finding, K9201, Note, NoteKind};
use super::{ExtraFile, HostCounts};
use crate::host_queries::{self, HostQuery};
use crate::variables::QueriesConfig;

/// What reading the host code found.
#[derive(Debug, Clone, Default)]
pub struct HostLint {
	/// The host files with a finding: their text and comments, for the report's carets and for
	/// suppressions.
	pub files: Vec<ExtraFile>,
	pub findings: Vec<Finding>,
	pub counts: HostCounts,
}

/// Read the host code under `config.sources` (relative to `root`) and report each `${…}`
/// interpolated into a query as `K9201`, at the substitution.
///
/// `counts.files` is the files that hold at least one query. A source that does not exist, or a
/// file that cannot be read, is an error.
pub fn lint(root: &Path, config: &QueriesConfig) -> Result<HostLint> {
	let roots: Vec<PathBuf> = config.sources.iter().map(|source| root.join(source)).collect();
	let methods: Vec<&str> = config.methods.iter().map(String::as_str).collect();
	let mut by_file: BTreeMap<PathBuf, Vec<HostQuery>> = BTreeMap::new();
	for found in host_queries::scan(&roots, &methods)? {
		by_file.entry(found.file).or_default().push(found.query);
	}

	let mut lint = HostLint::default();
	for (path, queries) in by_file {
		lint.counts.files += 1;
		lint.counts.queries += queries.len();
		let substitutions: Vec<_> =
			queries.iter().flat_map(|query| query.substitutions.iter()).collect();
		if substitutions.is_empty() {
			continue;
		}
		let text = std::fs::read_to_string(&path)
			.with_context(|| format!("reading {}", path.display()))?;
		for substitution in substitutions {
			lint.findings.push(interpolation(&path, substitution.host_range.clone()));
		}
		let comments = host_queries::comments(&path.to_string_lossy(), &text);
		lint.files.push(ExtraFile {
			path,
			text,
			comments: Some(comments),
		});
	}
	Ok(lint)
}

fn interpolation(path: &Path, span: std::ops::Range<usize>) -> Finding {
	Finding::kit(
		K9201,
		"`${…}` interpolated into a query string: the value is spliced into the SurrealQL text",
		path.to_path_buf(),
		span,
	)
	.with_note(Note {
		kind: NoteKind::Help,
		message: "pass it as a parameter (`$name` with a bindings object) or build the query with \
		          the SDK's `surql` tag"
			.to_string(),
		span: None,
		replacement: None,
	})
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::*;
	use crate::check::discover::Sources;
	use crate::check::engine::Engine;
	use crate::check::policy::Policy;
	use crate::check::{Extras, Report, run_with};
	use crate::variables::TemplateVars;

	struct NoSchema;

	impl Engine for NoSchema {
		async fn check(
			&mut self,
			_: &crate::check::discover::Source,
			_: &TemplateVars,
		) -> Result<Vec<Finding>> {
			Ok(Vec::new())
		}

		async fn apply(
			&mut self,
			_: &crate::check::discover::Source,
			_: &TemplateVars,
		) -> Result<Vec<Finding>> {
			Ok(Vec::new())
		}

		fn analyzes(&self) -> bool {
			false
		}
	}

	fn project(files: &[(&str, &str)]) -> tempfile::TempDir {
		let dir = tempfile::tempdir().expect("tempdir");
		for (name, text) in files {
			let path = dir.path().join(name);
			fs::create_dir_all(path.parent().expect("parent")).expect("dir");
			fs::write(path, text).expect("write");
		}
		dir
	}

	fn config(sources: &[&str]) -> QueriesConfig {
		QueriesConfig {
			sources: sources.iter().map(PathBuf::from).collect(),
			..QueriesConfig::default()
		}
	}

	async fn check(dir: &Path, sources: &[&str], policy: &str) -> Report {
		let linted = lint(dir, &config(sources)).expect("lint");
		let extras = Extras {
			files: linted.files,
			findings: linted.findings,
			host: Some(linted.counts),
			..Extras::default()
		};
		let raw = format!("[analyze]\n{}", policy.replace("[lints]", "[analyze.lints]"));
		let policy = Policy::from_config(
			&crate::project::ProjectConfig::parse(&raw).expect("parses").analyze,
		)
		.expect("policy");
		let sources = Sources {
			root: dir.to_path_buf(),
			schema: Vec::new(),
			queries: Vec::new(),
			schema_dirs: Vec::new(),
		};
		run_with(&mut NoSchema, &sources, &policy, &TemplateVars::default(), extras)
			.await
			.expect("run")
	}

	const APP: &str = "\
db.query(`SELECT * FROM person WHERE id = ${id}`);
db.query(`SELECT * FROM person`);
db.query('SELECT * FROM person WHERE id = ' + id);
db.query(\"SELECT 1\");
db.query(surql`SELECT * FROM person WHERE id = ${id}`);
";

	#[tokio::test]
	async fn an_untagged_template_with_a_substitution_is_k9201_at_the_substitution() {
		let dir = project(&[("src/app.ts", APP)]);
		let report = check(dir.path(), &["src"], "").await;
		assert_eq!(report.findings.len(), 1, "{:#?}", report.findings);
		let finding = &report.findings[0];
		assert_eq!(finding.code, K9201);
		assert_eq!(&APP[finding.span.clone()], "${id}");
		assert_eq!(finding.severity, crate::check::finding::Severity::Error);
		assert!(finding.message.starts_with("`${…}` interpolated into a query string"));
		assert_eq!(finding.notes[0].kind, NoteKind::Help);
		assert!(!report.passed());
		let s = &report.summary;
		assert_eq!((s.host_read, s.host_files, s.host_queries), (true, 1, 3));
		assert_eq!(s.queries_analyzed, false, "host queries are not `query_files`");
		assert!(report.texts.keys().any(|path| path.ends_with("src/app.ts")));
	}

	#[tokio::test]
	async fn a_surql_tagged_template_a_plain_string_and_a_clean_template_do_not_fire() {
		let clean = "\
db.query(surql`SELECT * FROM person WHERE id = ${id}`);
db.query(`SELECT * FROM person`);
db.query(\"SELECT * FROM person WHERE id = $id\", { id });
";
		let dir = project(&[("src/clean.ts", clean)]);
		let report = check(dir.path(), &["src"], "").await;
		assert!(report.findings.is_empty(), "{:#?}", report.findings);
		assert_eq!(report.summary.host_queries, 2, "the tagged template is not a literal");
		assert!(report.passed());
	}

	#[tokio::test]
	async fn a_suppression_comment_silences_the_substitution_on_the_next_line() {
		let text = "\
// surreal: allow(K9201) reason=\"the id is a validated integer\"
db.query(`SELECT * FROM person WHERE id = ${id}`);
db.query(`SELECT * FROM post WHERE id = ${id}`); // surreal: allow(K9201)
db.query(`SELECT * FROM user WHERE id = ${id}`);
";
		let dir = project(&[("src/app.ts", text)]);
		let report = check(dir.path(), &["src"], "require_suppression_reason = true\n").await;
		assert_eq!(report.summary.suppressed, 2);
		let spans: Vec<&str> = report.findings.iter().map(|f| &text[f.span.clone()]).collect();
		let (lines, user) = (&report.findings, text.rfind("${id}").unwrap());
		assert_eq!(
			lines.iter().filter(|f| f.code == K9201).map(|f| f.span.start).collect::<Vec<_>>(),
			[user],
			"{spans:?}"
		);
		// The trailing directive has no reason, and a reason is required: a warning that still
		// suppresses.
		let reasonless: Vec<_> = lines.iter().filter(|f| f.code.to_string() == "L7013").collect();
		assert_eq!(reasonless.len(), 1);
		assert!(text[reasonless[0].span.clone()].starts_with("// surreal: allow(K9201)"));
	}

	#[tokio::test]
	async fn a_string_that_looks_like_a_directive_does_not_suppress() {
		let text = "\
const s = \"// surreal: allow(K9201)\";
db.query(`SELECT ${id}`);
";
		let dir = project(&[("src/app.ts", text)]);
		let report = check(dir.path(), &["src"], "").await;
		assert_eq!(report.summary.suppressed, 0);
		assert_eq!(report.findings.len(), 1);
	}

	#[tokio::test]
	async fn svelte_and_vue_findings_point_into_the_whole_file() {
		let svelte = "<h1>hi</h1>\n<script>\n// surreal: allow(K9201)\nconst a = db.query(`SELECT ${one}`);\nconst b = db.query(`SELECT ${two}`);\n</script>\n";
		let vue = "<template><p/></template>\n<script setup lang=\"ts\">\nconst c = await db.query(`SELECT ${three}`);\n</script>\n";
		let dir = project(&[("web/Page.svelte", svelte), ("web/Comp.vue", vue)]);
		let report = check(dir.path(), &["web"], "").await;
		let found: Vec<(String, &str)> = report
			.findings
			.iter()
			.map(|f| {
				let text = &report.texts[&f.path];
				(f.path.file_name().unwrap().to_string_lossy().into_owned(), &text[f.span.clone()])
			})
			.collect();
		assert_eq!(
			found,
			[("Comp.vue".to_string(), "${three}"), ("Page.svelte".to_string(), "${two}")]
		);
		assert_eq!(report.summary.suppressed, 1, "`one` is covered from inside the script block");
	}

	#[tokio::test]
	async fn the_methods_are_configurable_and_the_level_is_policy() {
		let text = "db.run(`SELECT ${a}`);\ndb.query(`SELECT ${b}`);\n";
		let dir = project(&[("src/app.ts", text)]);
		let mut custom = config(&["src"]);
		custom.methods = vec!["run".to_string()];
		let linted = lint(dir.path(), &custom).expect("lint");
		assert_eq!(linted.findings.len(), 1);
		assert_eq!(&text[linted.findings[0].span.clone()], "${a}");

		let warned = check(dir.path(), &["src"], "[lints]\nK9201 = \"warn\"\n").await;
		assert!(warned.passed(), "K9201 is a lint level like any other");
		assert_eq!(warned.summary.warnings, 1);
		let allowed = check(dir.path(), &["src"], "[lints]\n\"9xxx\" = \"allow\"\n").await;
		assert_eq!((allowed.findings.len(), allowed.summary.allowed), (0, 1));
	}

	#[test]
	fn a_missing_source_is_an_error() {
		let dir = project(&[]);
		let error = lint(dir.path(), &config(&["nowhere"])).expect_err("missing");
		assert!(format!("{error:#}").contains("does not exist"), "{error:#}");
	}
}
