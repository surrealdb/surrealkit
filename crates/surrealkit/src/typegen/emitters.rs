//! External emitters: code generators SurrealKit runs over schema document v2.
//!
//! An emitter is configured as `[typegen.emit.<name>]` with a `command` and an `out`
//! directory. SurrealKit runs the command with the document on stdin and reads back, on
//! stdout, the set of files to write. It then publishes them with the same guarantees as its
//! own outputs: everything is rendered before anything is written, each file is replaced
//! atomically and left alone when unchanged, `--check` compares instead of writing (and fails
//! when an emitter could not run), and a strict run fails on any emitter problem. The protocol
//! is specified in `docs/typegen/external-emitters.md`.
//!
//! SurrealKit owns `out`. It records the files it wrote in a manifest,
//! `out/.surrealkit-emit.json`, deletes the ones a later run no longer produces, and never
//! touches a file it did not write: `--check` reports such a file as extra, and publishing
//! next to one fails in strict mode.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use super::diagnostics::{Diagnostic, Severity};
use super::output::{self, FileStatus, WriteOutcome};

/// The emitter protocol version this SurrealKit speaks.
pub const PROTOCOL: u32 = 1;

/// The manifest SurrealKit keeps in each emitter's `out` directory.
pub const MANIFEST: &str = ".surrealkit-emit.json";

/// A configured external emitter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Emitter {
	/// The `<name>` of `[typegen.emit.<name>]`.
	pub name: String,
	/// The program and its arguments.
	pub command: Vec<String>,
	/// The directory the emitter's files go to.
	pub out: PathBuf,
}

/// What an emitter writes on stdout.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
	protocol: u32,
	files: Vec<ReplyFile>,
	#[serde(default)]
	diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyFile {
	path: String,
	contents: String,
}

/// The manifest's contents.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
	emitter: String,
	protocol: u32,
	files: Vec<String>,
}

/// One emitter's rendered output, ready to publish or check.
#[derive(Debug)]
pub(crate) struct EmitPlan {
	emitter: String,
	out: PathBuf,
	/// `(path relative to out, bytes)`, sorted by path.
	files: Vec<(String, Vec<u8>)>,
	manifest: Vec<u8>,
}

/// Whether `path` is a safe relative output path: `/`-separated, no empty, `.` or `..`
/// segment, not absolute, no backslash, not the manifest.
fn valid_path(path: &str) -> bool {
	!path.is_empty()
		&& !path.starts_with('/')
		&& !path.contains('\\')
		&& !path.contains(':')
		&& path != MANIFEST
		&& path.split('/').all(|s| !s.is_empty() && s != "." && s != "..")
}

/// Run `emitter` with `input` (schema document v2) on stdin and parse its reply.
/// An emitter's files (path relative to its `out`, bytes) and its diagnostics.
type EmitterOutput = (Vec<(String, Vec<u8>)>, Vec<Diagnostic>);

fn run(
	emitter: &Emitter,
	input: &[u8],
	acknowledge: &BTreeMap<String, String>,
) -> Result<EmitterOutput> {
	let (program, args) =
		emitter.command.split_first().ok_or_else(|| anyhow!("its command is empty"))?;
	let mut child = Command::new(program)
		.args(args)
		.env("SURREALKIT_EMIT_PROTOCOL", PROTOCOL.to_string())
		.env("SURREALKIT_EMIT_NAME", &emitter.name)
		.env("SURREALKIT_EMIT_OUT", &emitter.out)
		.env("SURREALKIT_EMIT_ACKNOWLEDGE", serde_json::to_string(acknowledge)?)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::inherit())
		.spawn()
		.with_context(|| format!("starting `{}`", emitter.command.join(" ")))?;
	// Feed stdin from another thread, so a large document cannot deadlock against a
	// child that writes its reply before reading all of its input.
	let mut stdin = child.stdin.take().ok_or_else(|| anyhow!("no stdin pipe"))?;
	let input = input.to_vec();
	let writer = std::thread::spawn(move || {
		// A child that exits without reading everything closes the pipe; its exit
		// status, not the broken pipe, says what went wrong.
		let _ = stdin.write_all(&input);
	});
	let output = child.wait_with_output().context("waiting for the emitter")?;
	let _ = writer.join();
	if !output.status.success() {
		bail!("`{}` exited with {}", emitter.command.join(" "), output.status);
	}
	let reply: Reply = serde_json::from_slice(&output.stdout).map_err(|err| {
		anyhow!(
			"its output is not an emitter reply ({err}); it must print {{\"protocol\": {PROTOCOL}, \
			 \"files\": [{{\"path\", \"contents\"}}]}} on stdout"
		)
	})?;
	if reply.protocol != PROTOCOL {
		bail!("it speaks emitter protocol {}, this SurrealKit speaks {PROTOCOL}", reply.protocol);
	}
	let mut seen = BTreeSet::new();
	let mut files = Vec::with_capacity(reply.files.len());
	for file in reply.files {
		if !valid_path(&file.path) {
			bail!(
				"it returned the path {:?}; paths must be relative, `/`-separated, without `.` or \
				 `..` segments, and not {MANIFEST}",
				file.path
			);
		}
		if !seen.insert(file.path.clone()) {
			bail!("it returned {:?} twice", file.path);
		}
		files.push((file.path, file.contents.into_bytes()));
	}
	files.sort_by(|a, b| a.0.cmp(&b.0));
	Ok((files, reply.diagnostics))
}

/// How [`render`] treats problems.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Policy<'a> {
	/// Fail on an emitter's error diagnostics that are not acknowledged, and on any emitter
	/// that cannot run.
	pub strict: bool,
	/// Fail on an emitter that cannot run (spawn, exit status, invalid reply) even when not
	/// `strict`. `compile` sets it: `--lenient` tolerates diagnostics, not broken emitters.
	pub fatal_failures: bool,
	/// `[typegen] acknowledge`: error codes that do not fail, with the reason. Passed to the
	/// emitters as `SURREALKIT_EMIT_ACKNOWLEDGE`.
	pub acknowledge: &'a BTreeMap<String, String>,
}

/// The emitters' rendered output, and the emitters that could not run.
#[derive(Debug, Default)]
pub(crate) struct Rendered {
	pub plans: Vec<EmitPlan>,
	/// Emitters that failed and were skipped (only when neither `strict` nor
	/// `fatal_failures`). `--check` reports them: their output cannot be checked.
	pub failed: Vec<String>,
}

/// Run every emitter over `input` and plan its output. A failing emitter is an error when
/// `policy` says so, otherwise it is logged and recorded in [`Rendered::failed`], leaving its
/// files as they were.
pub(crate) fn render(emitters: &[Emitter], input: &[u8], policy: Policy<'_>) -> Result<Rendered> {
	let mut rendered = Rendered::default();
	for emitter in emitters {
		let result = run(emitter, input, policy.acknowledge).and_then(|(files, diagnostics)| {
			let errors: Vec<String> = diagnostics
				.iter()
				.filter(|d| d.is_error() && !policy.acknowledge.contains_key(&d.code))
				.map(|d| format!("  {d}"))
				.collect();
			for d in &diagnostics {
				match (d.severity, policy.acknowledge.get(&d.code)) {
					(Severity::Error, Some(reason)) => {
						log::debug!("emit.{}: {d} (acknowledged: {reason})", emitter.name);
					}
					(Severity::Error | Severity::Warning, _) => {
						log::warn!("emit.{}: {d}", emitter.name);
					}
					(Severity::Info, _) => log::debug!("emit.{}: {d}", emitter.name),
				}
			}
			if policy.strict && !errors.is_empty() {
				bail!("it reported {} error diagnostic(s):\n{}", errors.len(), errors.join("\n"));
			}
			let manifest = Manifest {
				emitter: emitter.name.clone(),
				protocol: PROTOCOL,
				files: files.iter().map(|(p, _)| p.clone()).collect(),
			};
			let mut manifest = serde_json::to_string_pretty(&manifest)?;
			manifest.push('\n');
			Ok(EmitPlan {
				emitter: emitter.name.clone(),
				out: emitter.out.clone(),
				files,
				manifest: manifest.into_bytes(),
			})
		});
		match result {
			Ok(plan) => rendered.plans.push(plan),
			Err(err) if policy.strict || policy.fatal_failures => {
				return Err(err.context(format!("emitter [typegen.emit.{}]", emitter.name)));
			}
			Err(err) => {
				log::error!(
					"emitter [typegen.emit.{}]: {err:#}; its outputs were left as they were",
					emitter.name
				);
				rendered.failed.push(emitter.name.clone());
			}
		}
	}
	Ok(rendered)
}

/// Every file under `dir`, relative and `/`-separated, the manifest excluded.
fn files_under(dir: &Path) -> Result<BTreeSet<String>> {
	let mut out = BTreeSet::new();
	if !dir.is_dir() {
		return Ok(out);
	}
	for entry in walkdir::WalkDir::new(dir) {
		let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
		if !entry.file_type().is_file() {
			continue;
		}
		let rel = entry
			.path()
			.strip_prefix(dir)
			.map_err(|_| anyhow!("{} is outside {}", entry.path().display(), dir.display()))?;
		let rel =
			rel.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/");
		if rel != MANIFEST {
			out.insert(rel);
		}
	}
	Ok(out)
}

/// The files the previous publish recorded, if `out` has a manifest.
fn previous(plan: &EmitPlan) -> Result<Option<BTreeSet<String>>> {
	let path = plan.out.join(MANIFEST);
	match std::fs::read(&path) {
		Ok(bytes) => {
			let manifest: Manifest = serde_json::from_slice(&bytes).with_context(|| {
				format!("{} is not a SurrealKit emitter manifest", path.display())
			})?;
			Ok(Some(manifest.files.into_iter().collect()))
		}
		Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
	}
}

/// What `--check` reports for `plan`: missing, stale and extra files.
pub(crate) fn check(plan: &EmitPlan) -> Result<Vec<String>> {
	let mut problems = Vec::new();
	let wanted: BTreeSet<&str> = plan.files.iter().map(|(p, _)| p.as_str()).collect();
	let entries = plan
		.files
		.iter()
		.map(|(p, b)| (plan.out.join(p), b))
		.chain(std::iter::once((plan.out.join(MANIFEST), &plan.manifest)));
	for (path, bytes) in entries {
		match output::status(&path, bytes)? {
			FileStatus::UpToDate => {}
			FileStatus::Stale => problems.push(format!("  stale:   {}", path.display())),
			FileStatus::Missing => problems.push(format!("  missing: {}", path.display())),
		}
	}
	for extra in files_under(&plan.out)? {
		if !wanted.contains(extra.as_str()) {
			problems.push(format!(
				"  extra:   {} (emitter {})",
				plan.out.join(extra).display(),
				plan.emitter
			));
		}
	}
	Ok(problems)
}

/// The number of files `plan` checks: its files and its manifest.
pub(crate) fn file_count(plan: &EmitPlan) -> usize {
	plan.files.len() + 1
}

/// Refuse to publish `plan` where it would overwrite or sit next to files SurrealKit did not
/// write. Overwriting one is always refused; sitting next to one is refused when `strict`.
pub(crate) fn preflight(plan: &EmitPlan, strict: bool) -> Result<()> {
	let previous = previous(plan)?.unwrap_or_default();
	let wanted: BTreeSet<&str> = plan.files.iter().map(|(p, _)| p.as_str()).collect();
	let mut foreign = Vec::new();
	for existing in files_under(&plan.out)? {
		if previous.contains(&existing) {
			continue;
		}
		if wanted.contains(existing.as_str()) {
			let path = plan.out.join(&existing);
			let bytes = plan.files.iter().find(|(p, _)| *p == existing).map(|(_, b)| b);
			if bytes
				.is_some_and(|b| output::status(&path, b).is_ok_and(|s| s == FileStatus::UpToDate))
			{
				continue;
			}
			bail!(
				"emitter [typegen.emit.{}] would overwrite {}, which SurrealKit did not write; move \
				 it, or point `out` at a directory of its own",
				plan.emitter,
				path.display()
			);
		}
		foreign.push(plan.out.join(existing).display().to_string());
	}
	if !foreign.is_empty() {
		let message = format!(
			"emitter [typegen.emit.{}] owns {}, which also holds files SurrealKit did not write:\n  {}",
			plan.emitter,
			plan.out.display(),
			foreign.join("\n  ")
		);
		if strict {
			bail!("{message}");
		}
		log::warn!("{message}");
	}
	Ok(())
}

/// Write `plan`'s files, remove the ones the previous publish wrote that are no longer
/// produced, then write the manifest.
pub(crate) fn publish(plan: &EmitPlan, level: log::Level) -> Result<()> {
	let previous = previous(plan)?.unwrap_or_default();
	let mut written = 0usize;
	for (path, bytes) in &plan.files {
		if output::write_atomic(&plan.out.join(path), bytes)? == WriteOutcome::Written {
			written += 1;
		}
	}
	let wanted: BTreeSet<&str> = plan.files.iter().map(|(p, _)| p.as_str()).collect();
	for stale in previous.iter().filter(|p| !wanted.contains(p.as_str())) {
		if !valid_path(stale) {
			continue;
		}
		let path = plan.out.join(stale);
		match std::fs::remove_file(&path) {
			Ok(()) => {
				written += 1;
				remove_empty_parents(&plan.out, &path);
			}
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
			Err(err) => return Err(err).with_context(|| format!("removing {}", path.display())),
		}
	}
	output::write_atomic(&plan.out.join(MANIFEST), &plan.manifest)?;
	if written == 0 {
		log::log!(
			level,
			"typegen: emitter {} is up to date ({})",
			plan.emitter,
			plan.out.display()
		);
	} else {
		log::log!(
			level,
			"typegen: emitter {} updated {} file(s) in {} ({} generated)",
			plan.emitter,
			written,
			plan.out.display(),
			plan.files.len()
		);
	}
	Ok(())
}

/// Remove the directories between `file` and `root` that are now empty.
fn remove_empty_parents(root: &Path, file: &Path) {
	let mut dir = file.parent();
	while let Some(d) = dir {
		if d == root || !d.starts_with(root) || std::fs::remove_dir(d).is_err() {
			break;
		}
		dir = d.parent();
	}
}

/// Whether any emitter has never published (its manifest is missing), so `sync` must run it
/// even when no schema changed.
pub(crate) fn any_unpublished(emitters: &[Emitter]) -> bool {
	emitters.iter().any(|e| !e.out.join(MANIFEST).is_file())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn output_paths_must_stay_inside_out() {
		for ok in ["person.ts", "tables/person.ts", "a-b_c.d.ts"] {
			assert!(valid_path(ok), "{ok}");
		}
		for bad in [
			"",
			"/abs.ts",
			"../up.ts",
			"a/../b.ts",
			"a//b.ts",
			"./a.ts",
			"a\\b.ts",
			"C:x.ts",
			MANIFEST,
			"dir/",
		] {
			assert!(!valid_path(bad), "{bad}");
		}
	}
}
