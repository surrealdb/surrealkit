//! `surrealkit check` and `surrealkit watch`: a [`surrealkit::check`] run, shown to a person or
//! a machine, and the loop that repeats it when a file changes.
//!
//! Neither reads a connection setting, but for `check --target`, which compares the files with
//! the named targets and so resolves and connects to them. The project root is the working
//! directory, the way `surrealkit compile` has it.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use notify::{EventKind, RecursiveMode, Watcher};
use surrealkit::check::discover::Ignore;
use surrealkit::check::{Prepared, Report, RunOptions, render};
use surrealkit::project::{ProjectConfig, Target};
use surrealkit::variables::{TemplateVars, build_project_vars, load_typegen_config};
use tokio::sync::mpsc::unbounded_channel;

use crate::output;

/// How long to keep collecting changes after the first, so one save is one run.
const DEBOUNCE: Duration = Duration::from_millis(200);

/// `--format`: how `check` shows its report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CheckFormat {
	/// Findings for a person, on stderr.
	Human,
	/// One JSON document on stdout.
	Json,
	/// One SARIF 2.1.0 document on stdout, for code scanning.
	Sarif,
}

impl CheckFormat {
	fn name(self) -> &'static str {
		match self {
			Self::Human => "human",
			Self::Json => "json",
			Self::Sarif => "sarif",
		}
	}
}

/// The format `check` runs with: `-j` is `--format json`, and it cannot be another one.
pub fn resolve_format(json: bool, format: Option<CheckFormat>) -> Result<CheckFormat> {
	match (json, format) {
		(false, format) => Ok(format.unwrap_or(CheckFormat::Human)),
		(true, None | Some(CheckFormat::Json)) => Ok(CheckFormat::Json),
		(true, Some(other)) => bail!("`--json` and `--format {}` disagree; pass one", other.name()),
	}
}

/// What the command line says a check reads.
pub struct Opts {
	pub folder: String,
	pub schemas: Vec<String>,
	pub all: bool,
	pub no_deps: bool,
	/// `--var KEY=VALUE`.
	pub vars: Vec<(String, String)>,
	/// The targets `--target` names, resolved: each is compared with the files. Empty for a
	/// check that connects to nothing.
	pub targets: Vec<Target>,
	/// `--show-suppressed`: also show the findings a directive silenced.
	pub show_suppressed: bool,
}

/// What a run reads from `surrealkit.toml`, loaded again when the file changes.
fn load(opts: &Opts) -> Result<Prepared> {
	let project = ProjectConfig::load(None)?;
	// The same capabilities `compile` runs the engine with. Template variables come from the
	// config file and `--var` only, never from `SURREALKIT_VAR_*`.
	let typegen = load_typegen_config(None)?;
	let vars = TemplateVars {
		vars: build_project_vars(&opts.vars, None)?,
	};
	Prepared::new(project, &typegen, &opts.schemas, opts.all, opts.no_deps, vars)
}

/// One check of the project at `root`, with a fresh engine: the library's check of the whole
/// project, with the targets in `opts` compared with the files. The one function `watch` calls
/// for every change, which has no target.
async fn run_once(loaded: &Prepared, opts: &Opts, root: &Path) -> Result<Report> {
	let options = RunOptions {
		show_suppressed: opts.show_suppressed,
	};
	loaded.run_with_options(root, &opts.folder, &opts.targets, options).await
}

/// Colour on stderr: when it is a terminal and `NO_COLOR` is unset or empty.
fn color() -> bool {
	std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())
}

/// `surrealkit check`: run once, show the report, and return the exit code: 1 when a finding is
/// an error after policy. A configuration or backend failure, and a target that cannot be
/// reached, is an `Err`.
///
/// Human output goes to stderr whole, like rustc's, so redirecting stdout cannot hide an
/// error. JSON and SARIF are one document on stdout and nothing else. With
/// `opts.show_suppressed` the report also holds what a directive silenced, which changes no
/// count and not the exit code.
pub async fn check(opts: &Opts, format: CheckFormat) -> Result<i32> {
	let root = std::env::current_dir().context("reading the working directory")?;
	let loaded = load(opts)?;
	let report = run_once(&loaded, opts, &root).await?;
	match format {
		CheckFormat::Human => eprint!("{}", render::human(&report, &root, color())),
		CheckFormat::Json => println!("{}", output::pretty(&render::json(&report, &root))),
		CheckFormat::Sarif => println!("{}", output::pretty(&render::sarif(&report, &root))),
	}
	Ok(if report.passed() {
		0
	} else {
		1
	})
}

/// Whether a change to `path` should run a check: a `.surql` file, `surrealkit.toml`, `.env` or
/// `.env.local`, not under an ignored path.
fn relevant(path: &Path, root: &Path, ignore: Option<&Ignore>) -> bool {
	let named = path
		.file_name()
		.and_then(|name| name.to_str())
		.is_some_and(|name| matches!(name, "surrealkit.toml" | ".env" | ".env.local"));
	if !named && path.extension().and_then(|ext| ext.to_str()) != Some("surql") {
		return false;
	}
	match (path.strip_prefix(root), ignore) {
		(Ok(relative), Some(ignore)) => !ignore.matches(relative),
		_ => true,
	}
}

/// `surrealkit watch`, and `check --watch`: check, then check again after every change to a
/// source file or `surrealkit.toml`, until Ctrl-C. Changes within 200 ms of the first make one
/// run. Each run starts from a fresh engine and re-reads `surrealkit.toml`; when that fails to
/// load, the last configuration that did is kept. Only the watcher failing ends the command
/// early.
pub async fn watch(opts: &Opts) -> Result<i32> {
	let root = std::env::current_dir().context("reading the working directory")?;
	// Events carry the canonical path (on macOS `/private/var/...`).
	let events_root = root.canonicalize().unwrap_or_else(|_| root.clone());
	let (sender, mut events) = unbounded_channel();
	let mut watcher = notify::recommended_watcher(move |event| {
		let _ = sender.send(event);
	})
	.context("starting the file watcher")?;
	watcher
		.watch(&events_root, RecursiveMode::Recursive)
		.with_context(|| format!("watching {}", root.display()))?;
	eprintln!("watching {} — Ctrl-C to stop", root.display());

	let ctrl_c = tokio::signal::ctrl_c();
	tokio::pin!(ctrl_c);
	let mut loaded: Option<Prepared> = None;
	let mut reason = String::from("start");
	let mut number = 0;
	loop {
		number += 1;
		eprintln!("run {number} · {reason}");
		match load(opts) {
			Ok(fresh) => loaded = Some(fresh),
			Err(err) if loaded.is_some() => {
				eprintln!(
					"surrealkit.toml did not reload; keeping the previous configuration: {err:#}"
				);
			}
			Err(err) => eprintln!("error: {err:#}"),
		}
		if let Some(current) = &loaded {
			let outcome = tokio::select! {
				outcome = run_once(current, opts, &root) => outcome,
				_ = &mut ctrl_c => return Ok(0),
			};
			match outcome {
				Ok(report) => eprint!("{}", render::human(&report, &root, color())),
				Err(err) => eprintln!("error: {err:#}"),
			}
		}

		let ignore = loaded.as_ref().map(|l| &l.ignore);
		let changed = |event: notify::Result<notify::Event>| -> Option<PathBuf> {
			match event {
				Ok(event) if !matches!(event.kind, EventKind::Access(_)) => {
					event.paths.into_iter().find(|path| relevant(path, &events_root, ignore))
				}
				Ok(_) => None,
				Err(err) => {
					eprintln!("watch error: {err}");
					None
				}
			}
		};
		let first = loop {
			tokio::select! {
				_ = &mut ctrl_c => return Ok(0),
				event = events.recv() => {
					let Some(event) = event else {
						bail!("the file watcher stopped");
					};
					if let Some(path) = changed(event) {
						break path;
					}
				}
			}
		};
		let window = tokio::time::sleep(DEBOUNCE);
		tokio::pin!(window);
		loop {
			tokio::select! {
				_ = &mut ctrl_c => return Ok(0),
				_ = &mut window => break,
				event = events.recv() => {
					if event.is_none() {
						bail!("the file watcher stopped");
					}
				}
			}
		}
		reason =
			first.strip_prefix(&events_root).unwrap_or(&first).to_string_lossy().replace('\\', "/");
	}
}
