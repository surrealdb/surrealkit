//! `surrealkit check`: a lint for a project's SurrealQL, with no database.
//!
//! A check discovers the project's schema and query files ([`discover`]), runs them through an
//! analysis backend ([`Engine`](engine::Engine)), applies the `[analyze]` policy and the in-source suppressions
//! ([`policy`]) and reports what is left ([`Report`], rendered by [`render`]). The backend
//! uses an embedded in-memory engine, so nothing connects anywhere.
//!
//! The data model, the severity policy and the suppression grammar are those of the engine's
//! own checker (the SDK's `Surreal::check`, SurrealDB 4.1), so that the checker is a drop-in
//! backend behind the same configuration. The backend this build ships, [`SchemaApply`](engine::SchemaApply),
//! applies the schema and reports what the engine rejects.
//!
//! [`AnalyzeConfig`], the `[analyze]` section of `surrealkit.toml`, is plain serde and parses
//! whatever the cargo features; everything else needs the `analyze` feature.

mod config;
#[cfg(feature = "analyze")]
pub mod discover;
#[cfg(feature = "analyze")]
pub mod drift;
#[cfg(feature = "analyze")]
pub mod engine;
#[cfg(feature = "analyze")]
pub mod finding;
#[cfg(all(feature = "analyze", feature = "host-queries"))]
pub mod host;
#[cfg(feature = "analyze")]
pub mod policy;
#[cfg(feature = "analyze")]
pub mod render;

pub use config::AnalyzeConfig;
#[cfg(feature = "analyze")]
pub use run::{ExtraFile, Extras, HostCounts, Report, Summary, TargetFindings, run, run_with};

#[cfg(feature = "analyze")]
mod run {
	use std::collections::BTreeMap;
	use std::ops::Range;
	use std::path::{Path, PathBuf};

	use anyhow::Result;

	use super::discover::{Role, Source, Sources};
	use super::engine::Engine;
	use super::finding::{Finding, LineIndex, Severity};
	use super::policy::{Policy, Suppressions};
	use crate::variables::TemplateVars;

	/// What a check found.
	#[derive(Debug, Clone)]
	pub struct Report {
		/// The project root.
		pub root: PathBuf,
		/// Findings after suppressions and policy, sorted by path, span start and code.
		pub findings: Vec<Finding>,
		/// The text of each file that has a finding, as the spans of its findings index it.
		pub texts: BTreeMap<PathBuf, String>,
		pub summary: Summary,
	}

	impl Report {
		/// Whether no finding is an error.
		pub fn passed(&self) -> bool {
			self.summary.errors == 0
		}
	}

	/// The counts of a [`Report`].
	#[derive(Debug, Clone, Default, PartialEq, Eq)]
	pub struct Summary {
		pub schema_files: usize,
		pub query_files: usize,
		/// Whether the backend analysed the query files; when it did not, they were counted
		/// and nothing more.
		pub queries_analyzed: bool,
		pub errors: usize,
		pub warnings: usize,
		pub hints: usize,
		/// Findings an in-source `allow` silenced.
		pub suppressed: usize,
		/// Findings policy silenced (`allow` in `[analyze.lints]`, or a code's default).
		pub allowed: usize,
		/// Whether host code was read: `[typegen.queries] sources` is set and this build reads
		/// host code.
		pub host_read: bool,
		/// The host files that hold at least one query.
		pub host_files: usize,
		/// The queries found in host code.
		pub host_queries: usize,
		/// How many targets were compared with the files (`check --target`).
		pub targets_checked: usize,
		/// For each target compared, how many drift findings it has after suppressions and
		/// policy.
		pub drift: BTreeMap<String, usize>,
	}

	/// A file, besides the schema and query files, that findings point at.
	#[derive(Debug, Clone)]
	pub struct ExtraFile {
		/// Absolute.
		pub path: PathBuf,
		pub text: String,
		/// The byte ranges of the file's comments, where its suppression directives are; `None`
		/// for a file that has no suppressions.
		pub comments: Option<Vec<Range<usize>>>,
	}

	/// The findings of one target compared with the files.
	#[derive(Debug, Clone)]
	pub struct TargetFindings {
		pub target: String,
		pub findings: Vec<Finding>,
	}

	/// What a check found beyond the schema and query files: host code, and targets compared
	/// with the files. Every finding goes through suppressions and policy like the rest.
	#[derive(Debug, Clone, Default)]
	pub struct Extras {
		/// The files the findings below point at.
		pub files: Vec<ExtraFile>,
		/// Findings that belong to no target.
		pub findings: Vec<Finding>,
		/// Whether host code was read, and how much of it.
		pub host: Option<HostCounts>,
		/// Each target compared, with its findings.
		pub targets: Vec<TargetFindings>,
	}

	/// What reading host code found.
	#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
	pub struct HostCounts {
		pub files: usize,
		pub queries: usize,
	}

	/// Resolve `finding`'s severity by policy, and count it, or count it as allowed. Whether it
	/// is reported.
	fn settle(
		mut finding: Finding,
		policy: &Policy,
		summary: &mut Summary,
		findings: &mut Vec<Finding>,
	) -> bool {
		match policy.resolve(finding.code, finding.intrinsic(), finding.default_level()) {
			None => {
				summary.allowed += 1;
				false
			}
			Some(severity) => {
				finding.severity = severity;
				match severity {
					Severity::Error => summary.errors += 1,
					Severity::Warning => summary.warnings += 1,
					Severity::Hint => summary.hints += 1,
				}
				findings.push(finding);
				true
			}
		}
	}

	/// Run one check: each schema file is checked against the schema applied so far and then
	/// applied, and every query file is checked last. Suppressions are applied to the
	/// findings first, then policy; a malformed suppression is itself a finding, which goes
	/// through policy but cannot be suppressed.
	pub async fn run<E: Engine>(
		engine: &mut E,
		sources: &Sources,
		policy: &Policy,
		vars: &TemplateVars,
	) -> Result<Report> {
		run_with(engine, sources, policy, vars, Extras::default()).await
	}

	/// [`run`], with the findings of host code and of targets compared with the files, which go
	/// through the same suppressions, policy and report.
	pub async fn run_with<E: Engine>(
		engine: &mut E,
		sources: &Sources,
		policy: &Policy,
		vars: &TemplateVars,
		extras: Extras,
	) -> Result<Report> {
		// Each finding with the index of the target it belongs to, when it does.
		let mut found: Vec<(Finding, Option<usize>)> = Vec::new();
		for source in &sources.schema {
			found.extend(engine.check(source, vars).await?.into_iter().map(|f| (f, None)));
			found.extend(engine.apply(source, vars).await?.into_iter().map(|f| (f, None)));
		}
		for source in &sources.queries {
			found.extend(engine.check(source, vars).await?.into_iter().map(|f| (f, None)));
		}
		found.extend(extras.findings.into_iter().map(|f| (f, None)));
		let mut targets: Vec<&str> = Vec::new();
		for (index, target) in extras.targets.iter().enumerate() {
			targets.push(&target.target);
			found.extend(target.findings.iter().cloned().map(|f| (f, Some(index))));
		}

		let files: Vec<&Source> = sources.schema.iter().chain(&sources.queries).collect();
		let mut texts_of: Vec<(&Path, &str)> =
			files.iter().map(|s| (s.path.as_path(), s.text.as_str())).collect();
		texts_of.extend(extras.files.iter().map(|f| (f.path.as_path(), f.text.as_str())));
		let indexes: BTreeMap<&Path, LineIndex<'_>> =
			texts_of.iter().map(|(path, text)| (*path, LineIndex::new(text))).collect();
		let mut suppressions: BTreeMap<&Path, Suppressions> = BTreeMap::new();
		let mut directives: Vec<Finding> = Vec::new();
		for source in &files {
			let (read, malformed) = Suppressions::read(&source.path, &source.text, policy);
			suppressions.insert(source.path.as_path(), read);
			directives.extend(malformed);
		}
		for file in &extras.files {
			let Some(comments) = &file.comments else {
				continue;
			};
			let (read, malformed) =
				Suppressions::from_comments(&file.path, &file.text, comments, policy);
			suppressions.insert(file.path.as_path(), read);
			directives.extend(malformed);
		}

		let mut summary = Summary {
			schema_files: sources.schema.iter().filter(|s| s.role == Role::Schema).count(),
			query_files: sources.queries.len(),
			queries_analyzed: engine.analyzes(),
			host_read: extras.host.is_some(),
			host_files: extras.host.map_or(0, |host| host.files),
			host_queries: extras.host.map_or(0, |host| host.queries),
			targets_checked: targets.len(),
			drift: targets.iter().map(|name| ((*name).to_string(), 0)).collect(),
			..Summary::default()
		};
		let mut findings = Vec::new();
		for (finding, target) in found {
			let line = indexes
				.get(finding.path.as_path())
				.map_or(1, |index| index.position(finding.span.start).0);
			let silenced = suppressions
				.get(finding.path.as_path())
				.is_some_and(|read| read.suppresses(&finding, line));
			if silenced {
				summary.suppressed += 1;
			} else if settle(finding, policy, &mut summary, &mut findings)
				&& let Some(name) = target.map(|index| targets[index])
			{
				*summary.drift.entry(name.to_string()).or_insert(0) += 1;
			}
		}
		for finding in directives {
			settle(finding, policy, &mut summary, &mut findings);
		}
		findings
			.sort_by(|a, b| (&a.path, a.span.start, a.code).cmp(&(&b.path, b.span.start, b.code)));

		let texts = texts_of
			.iter()
			.filter(|(path, _)| findings.iter().any(|f| f.path == *path))
			.map(|(path, text)| ((*path).to_path_buf(), (*text).to_string()))
			.collect();
		Ok(Report {
			root: sources.root.clone(),
			findings,
			texts,
			summary,
		})
	}
}

#[cfg(all(test, feature = "analyze"))]
mod tests {
	use std::ops::Range;

	use super::*;
	use crate::check::discover::{Role, Source, Sources};
	use crate::check::engine::Engine;
	use crate::check::finding::{Code, Finding, K9003, L7013, Severity};
	use crate::check::policy::{Level, Policy};
	use crate::variables::TemplateVars;

	/// A backend that records the order it is called in and finds what it is told to.
	#[derive(Default)]
	struct Mock {
		calls: Vec<String>,
		on_apply: Vec<(String, Finding)>,
		on_check: Vec<(String, Finding)>,
		analyzes: bool,
	}

	impl Engine for Mock {
		async fn check(
			&mut self,
			source: &Source,
			_: &TemplateVars,
		) -> anyhow::Result<Vec<Finding>> {
			let name = file_name(source);
			self.calls.push(format!("check {name}"));
			Ok(self.on_check.iter().filter(|(n, _)| *n == name).map(|(_, f)| f.clone()).collect())
		}

		async fn apply(
			&mut self,
			source: &Source,
			_: &TemplateVars,
		) -> anyhow::Result<Vec<Finding>> {
			let name = file_name(source);
			self.calls.push(format!("apply {name}"));
			Ok(self.on_apply.iter().filter(|(n, _)| *n == name).map(|(_, f)| f.clone()).collect())
		}

		fn analyzes(&self) -> bool {
			self.analyzes
		}
	}

	fn file_name(source: &Source) -> String {
		source.path.file_name().expect("name").to_string_lossy().into_owned()
	}

	fn source(name: &str, text: &str, role: Role) -> Source {
		Source {
			path: std::path::PathBuf::from(format!("/p/{name}")),
			text: text.to_string(),
			role,
			module: None,
		}
	}

	fn sources(schema: Vec<Source>, queries: Vec<Source>) -> Sources {
		Sources {
			root: "/p".into(),
			schema,
			queries,
			schema_dirs: Vec::new(),
		}
	}

	fn at(name: &str, code: Code, span: Range<usize>) -> (String, Finding) {
		(
			name.to_string(),
			Finding::new(
				code,
				Severity::Error,
				Level::Deny,
				"m",
				std::path::PathBuf::from(format!("/p/{name}")),
				span,
			),
		)
	}

	fn policy(toml: &str) -> Policy {
		let raw = format!("[analyze]\n{}", toml.replace("[lints]", "[analyze.lints]"));
		Policy::from_config(&crate::project::ProjectConfig::parse(&raw).expect("parses").analyze)
			.expect("policy")
	}

	#[tokio::test]
	async fn each_schema_file_is_checked_then_applied_and_the_queries_come_last() {
		let mut mock = Mock::default();
		let sources = sources(
			vec![source("a", "", Role::Schema), source("b", "", Role::Schema)],
			vec![source("q", "", Role::Query)],
		);
		let report = run(&mut mock, &sources, &policy(""), &TemplateVars::default()).await.unwrap();
		assert_eq!(mock.calls, ["check a", "apply a", "check b", "apply b", "check q"]);
		assert_eq!(report.summary.schema_files, 2);
		assert_eq!(report.summary.query_files, 1);
		assert!(!report.summary.queries_analyzed);
		assert!(report.passed());
		assert!(report.texts.is_empty());
	}

	#[tokio::test]
	async fn findings_are_sorted_counted_and_carry_the_resolved_severity() {
		let mut mock = Mock::default();
		mock.on_apply.push(at("b", K9003, 5..6));
		mock.on_apply.push(at("a", K9003, 9..10));
		mock.on_apply.push(at("a", Code::kit(9001), 2..3));
		mock.on_apply.push(at("a", Code::kit(9002), 2..3));
		let schema =
			vec![source("a", "0123456789", Role::Schema), source("b", "012345", Role::Schema)];
		let sources = sources(schema, vec![]);
		let policy = policy("[lints]\nK9002 = \"warn\"\nK9001 = \"allow\"\n");
		let report = run(&mut mock, &sources, &policy, &TemplateVars::default()).await.unwrap();
		let order: Vec<_> = report
			.findings
			.iter()
			.map(|f| (file_name_of(&f.path), f.span.start, f.code.to_string(), f.severity))
			.collect();
		assert_eq!(
			order,
			vec![
				("a".into(), 2, "K9002".into(), Severity::Warning),
				("a".into(), 9, "K9003".into(), Severity::Error),
				("b".into(), 5, "K9003".into(), Severity::Error),
			]
		);
		assert_eq!((report.summary.errors, report.summary.warnings), (2, 1));
		assert_eq!(report.summary.allowed, 1);
		assert!(!report.passed());
		assert_eq!(report.texts.len(), 2, "the files with findings");
	}

	fn file_name_of(path: &std::path::Path) -> String {
		path.file_name().expect("name").to_string_lossy().into_owned()
	}

	#[tokio::test]
	async fn a_suppression_drops_the_finding_before_policy_and_counts_it() {
		let mut mock = Mock::default();
		let text = "-- surreal: allow(K9003)\nBAD;\nBAD;\n";
		mock.on_apply.push(at("a", K9003, 25..28));
		mock.on_apply.push(at("a", K9003, 30..33));
		let sources = sources(vec![source("a", text, Role::Schema)], vec![]);
		// Even a code the policy would deny is dropped by its own suppression.
		let policy = policy("[lints]\nK9003 = \"deny\"\n");
		let report = run(&mut mock, &sources, &policy, &TemplateVars::default()).await.unwrap();
		assert_eq!(report.summary.suppressed, 1);
		assert_eq!(report.findings.len(), 1);
		assert_eq!(report.findings[0].span, 30..33);
	}

	#[tokio::test]
	async fn a_malformed_directive_is_l7013_through_policy_and_suppresses_nothing() {
		let text = "-- surreal: allow(*)\nBAD;\n";
		for (toml, severity) in [
			("", Some(Severity::Warning)),
			("warnings_as_errors = true\n", Some(Severity::Error)),
			("[lints]\n\"7xxx\" = \"allow\"\n", None),
		] {
			let mut mock = Mock::default();
			mock.on_apply.push(at("a", K9003, 21..24));
			let sources = sources(vec![source("a", text, Role::Schema)], vec![]);
			let report =
				run(&mut mock, &sources, &policy(toml), &TemplateVars::default()).await.unwrap();
			assert_eq!(report.summary.suppressed, 0);
			let l7013: Vec<_> = report.findings.iter().filter(|f| f.code == L7013).collect();
			assert_eq!(l7013.first().map(|f| f.severity), severity, "{toml}");
			// The K9003 is reported either way: the bad directive silenced nothing.
			assert!(report.findings.iter().any(|f| f.code == K9003));
			assert_eq!(report.summary.allowed, usize::from(severity.is_none()));
		}
	}

	#[tokio::test]
	async fn query_files_carry_suppressions_too() {
		let mut mock = Mock {
			analyzes: true,
			..Mock::default()
		};
		mock.on_check.push(at("q", Code::from_number(1002), 40..41));
		let text =
			"SELECT 1;\nSELECT 2;\nSELECT 3;\nSELECT 4;\n-- surreal: allow(E1002) reason=\"x\"\n";
		let sources = sources(vec![], vec![source("q", &format!("{text}x;\n"), Role::Query)]);
		let report = run(&mut mock, &sources, &policy(""), &TemplateVars::default()).await.unwrap();
		assert!(report.summary.queries_analyzed);
		assert_eq!(
			report.summary.errors, 1,
			"an own-line directive covers the next line, not its own"
		);
	}

	fn extra(name: &str, text: &str, comments: Option<Vec<Range<usize>>>) -> ExtraFile {
		ExtraFile {
			path: std::path::PathBuf::from(format!("/p/{name}")),
			text: text.to_string(),
			comments,
		}
	}

	#[tokio::test]
	async fn extras_are_suppressed_settled_and_counted_per_target() {
		let host = "// surreal: allow(K9201)\nA;\nB;\n";
		let config = "[target.prod]\n[target.stage]\n";
		let warning = |name: &str, span: Range<usize>| {
			Finding::new(
				Code::kit(9105),
				Severity::Warning,
				Level::Warn,
				"m",
				std::path::PathBuf::from(format!("/p/{name}")),
				span,
			)
		};
		let (host_a, host_b) = (host.find("A;").unwrap(), host.find("B;").unwrap());
		let extras = Extras {
			files: vec![
				extra("h.ts", host, Some(vec![0..24])),
				// No comments: a directive-looking line here suppresses nothing.
				extra("surrealkit.toml", config, None),
			],
			findings: vec![
				at("h.ts", Code::kit(9201), host_a..host_a + 1).1,
				at("h.ts", Code::kit(9201), host_b..host_b + 1).1,
			],
			host: Some(HostCounts {
				files: 1,
				queries: 2,
			}),
			targets: vec![
				TargetFindings {
					target: "prod".into(),
					findings: vec![
						warning("surrealkit.toml", 0..13),
						warning("surrealkit.toml", 0..13),
					],
				},
				TargetFindings {
					target: "stage".into(),
					findings: vec![],
				},
			],
		};
		let mut mock = Mock::default();
		let sources = sources(vec![], vec![]);
		let warn = policy("[lints]\nK9105 = \"warn\"\n");
		let report =
			run_with(&mut mock, &sources, &warn, &TemplateVars::default(), extras).await.unwrap();
		let summary = &report.summary;
		assert_eq!(summary.suppressed, 1, "the directive covers the next line of the host file");
		assert_eq!((summary.errors, summary.warnings), (1, 2));
		assert_eq!((summary.host_read, summary.host_files, summary.host_queries), (true, 1, 2));
		assert_eq!(summary.targets_checked, 2);
		assert_eq!(summary.drift["prod"], 2);
		assert_eq!(summary.drift["stage"], 0);
		assert_eq!(report.texts.len(), 2, "host file and config: the files with findings");

		// A target whose findings policy allows still counts as compared, with none reported.
		let quiet = policy("[lints]\nK9105 = \"allow\"\n");
		let extras = Extras {
			files: vec![extra("surrealkit.toml", config, None)],
			targets: vec![TargetFindings {
				target: "prod".into(),
				findings: vec![warning("surrealkit.toml", 0..13)],
			}],
			..Extras::default()
		};
		let report =
			run_with(&mut mock, &sources, &quiet, &TemplateVars::default(), extras).await.unwrap();
		assert_eq!((report.summary.targets_checked, report.summary.drift["prod"]), (1, 0));
		assert_eq!(report.summary.allowed, 1);
		assert!(!report.summary.host_read);
	}
}
