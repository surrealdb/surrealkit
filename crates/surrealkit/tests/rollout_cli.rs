//! CLI-surface regression tests for the `rollout` command family.
//!
//! Every rollout test before 1.0.0-beta.1 called the library functions directly
//! with a hand-built `RolloutExecutionOpts`, so nothing exercised clap. That is
//! how beta.1 shipped with a positional named `target` shadowing the global
//! `-t/--target`: the rollout id was hoisted into the global `Vec<String>` and
//! rejected as an unknown database target before `rollout::run_start` was ever
//! reached, and `--target` vanished from those subcommands entirely.
//!
//! These tests drive the real binary. `rollout lint` and `rollout plan` never
//! open a connection, so they are fast, hermetic parser probes.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

/// Every rollout subcommand that takes a rollout id.
const ID_SUBCOMMANDS: [&str; 6] = ["start", "complete", "rollback", "status", "lint", "repair"];

const ROLLOUT_ID: &str = "20260302153045__demo";

fn write_project(root: &Path, toml: &str) {
	fs::create_dir_all(root.join("database/schema")).expect("schema dir");
	fs::create_dir_all(root.join("database/rollouts")).expect("rollouts dir");
	fs::write(root.join("database/schema/001_fixture.surql"), "DEFINE TABLE fixture SCHEMAFULL;\n")
		.expect("schema source");
	fs::write(root.join("surrealkit.toml"), toml).expect("project marker");
}

fn run(root: &Path, args: &[&str]) -> Output {
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command.current_dir(root);
	// A port nothing listens on: any command that gets as far as connecting fails
	// fast and locally, which is all these assertions need.
	command.args(["--host", "http://127.0.0.1:1"]);
	command.args(args);
	command.output().expect("run surrealkit")
}

fn combined(output: &Output) -> String {
	format!(
		"{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	)
}

/// The regression itself: a rollout id must never be read as a database target.
#[test]
fn rollout_id_is_not_parsed_as_a_database_target() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "");

	for subcommand in ID_SUBCOMMANDS {
		let output = run(root, &["rollout", subcommand, ROLLOUT_ID]);
		let text = combined(&output);
		assert!(
			!text.contains("unknown target"),
			"`rollout {subcommand} <id>` parsed the rollout id as a --target name: {text}"
		);
		assert!(
			!text.contains(ROLLOUT_ID) || !text.contains("declared targets"),
			"`rollout {subcommand} <id>` leaked the rollout id into target resolution: {text}"
		);
	}
}

/// `lint` never connects, so its error proves the id reached the rollout code.
#[test]
fn rollout_lint_receives_the_rollout_id() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "");

	let output = run(root, &["rollout", "lint", ROLLOUT_ID]);
	let text = combined(&output);
	assert!(
		text.contains("unable to find rollout") && text.contains(ROLLOUT_ID),
		"expected the rollout loader to report the missing manifest, got: {text}"
	);
}

/// `--target` must be usable on the subcommands that actually connect. Before the
/// fix clap refused it outright with `unexpected argument '--target' found`.
#[test]
fn rollout_execution_subcommands_accept_target() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "[target.prod]\nns = \"acme\"\ndb = \"prod\"\n");

	for subcommand in ["start", "complete", "rollback", "status", "repair"] {
		let output = run(root, &["rollout", subcommand, ROLLOUT_ID, "--target", "prod"]);
		let text = combined(&output);
		assert!(
			!text.contains("unexpected argument"),
			"`rollout {subcommand}` rejected --target: {text}"
		);
		assert!(
			!text.contains("unknown target"),
			"`rollout {subcommand}` failed to resolve the declared target: {text}"
		);
	}
}

/// `--help` is the cheapest possible detector for the shadowing: when it happens,
/// the global silently disappears from the subcommand.
#[test]
fn rollout_help_lists_both_the_id_and_the_target_flag() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "");

	for subcommand in ID_SUBCOMMANDS {
		let output = run(root, &["rollout", subcommand, "--help"]);
		let text = combined(&output);
		assert!(text.contains("ROLLOUT_ID"), "`rollout {subcommand} --help` lost the id: {text}");
		assert!(
			text.contains("--target"),
			"`rollout {subcommand} --help` lost the global --target: {text}"
		);
	}
}

/// Rollout execution is a locked, resumable, per-database state machine, so it
/// must refuse a multi-target selection rather than half-apply across them.
#[test]
fn rollout_execution_refuses_multiple_targets() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "[target.a]\ndb = \"a\"\n[target.b]\ndb = \"b\"\n");

	let output = run(root, &["rollout", "start", ROLLOUT_ID, "--target", "a", "--target", "b"]);
	let text = combined(&output);
	assert!(
		text.contains("one database target at a time"),
		"expected a refusal to fan out a rollout, got: {text}"
	);
}

/// `plan` and `lint` read only the filesystem, so a target selection cannot mean
/// anything to them. They say so rather than either failing (which breaks CI
/// wrappers that pass the same flags everywhere) or staying silent (which reads
/// as "applied to that target").
#[test]
fn offline_rollout_subcommands_warn_about_a_target_selection() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "[target.prod]\ndb = \"prod\"\n");

	for args in [
		vec!["rollout", "plan", "--target", "prod"],
		vec!["rollout", "lint", ROLLOUT_ID, "--target", "prod"],
	] {
		let output = run(root, &args);
		let text = combined(&output);
		assert!(
			text.contains("has no effect here and was ignored"),
			"expected {args:?} to warn about the target selection, got: {text}"
		);
		assert!(
			!text.contains("error:"),
			"the warning must not become a hard failure for {args:?}: {text}"
		);
	}
}

/// The connect deadline. Before 1.0.0-beta.2 nothing between the CLI and the
/// database had one, so an endpoint that accepted the socket and never completed
/// the handshake blocked until CI killed the job — the reported "rollout start
/// hung for 25 minutes", which left `__rollout` in an intermediate state.
#[test]
fn connect_timeout_bounds_a_wedged_endpoint() {
	use std::net::TcpListener;
	use std::time::Instant;

	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "");

	// Accept connections and never answer. Dropping the listener would refuse
	// them instead, which is a different (already fast) failure.
	let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
	let host = format!("ws://{}", listener.local_addr().expect("addr"));
	let accepted = std::thread::spawn(move || {
		let mut held = Vec::new();
		while let Ok((stream, _)) = listener.accept() {
			held.push(stream);
			if held.len() > 8 {
				break;
			}
		}
	});

	let started = Instant::now();
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(root)
		.args(["--host", &host, "--connect-timeout-secs", "2"])
		.args(["rollout", "status"])
		.output()
		.expect("run surrealkit");
	let elapsed = started.elapsed();

	let text = combined(&output);
	assert!(!output.status.success(), "a wedged endpoint must not report success: {text}");
	assert!(
		text.contains("timed out after 2s") && text.contains(&host),
		"expected a timeout naming the endpoint, got: {text}"
	);
	assert!(elapsed.as_secs() < 15, "the deadline did not bound the wait: {elapsed:?}");

	drop(accepted);
}

/// Manifest portability across environments.
///
/// Tracking keys used to be relative to the process working directory, and the
/// manifest's `target_schema_hash` covers those keys. A rollout planned from a
/// repo root (`database/schema/a.surql`) therefore failed `target schema hash
/// mismatch` when started inside a container resolving `/database/schema/a.surql`
/// — for byte-identical SQL. Keys are folder-relative now, so the same manifest
/// verifies from anywhere.
#[test]
fn a_manifest_verifies_from_a_different_working_directory() {
	let temp = TempDir::new().expect("tempdir");
	let root = temp.path();
	write_project(root, "");
	fs::write(
		root.join("database/schema/001_person.surql"),
		"DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD age ON person TYPE int ASSERT $value >= 0;\n",
	)
	.expect("schema source");

	let planned = run(root, &["rollout", "plan", "--name", "portable"]);
	assert!(planned.status.success(), "plan failed: {}", combined(&planned));

	let manifest = fs::read_dir(root.join("database/rollouts"))
		.expect("rollouts dir")
		.filter_map(|e| e.ok())
		.map(|e| e.file_name().to_string_lossy().replace(".toml", ""))
		.next()
		.expect("a generated manifest");

	// The manifest must record folder-relative paths, not the folder- or
	// cwd-prefixed ones: those are what the container fails to resolve.
	let manifest_body =
		fs::read_to_string(root.join("database/rollouts").join(format!("{manifest}.toml")))
			.expect("manifest");
	assert!(
		manifest_body.contains("\"schema/001_person.surql\""),
		"manifest should record folder-relative paths, got: {manifest_body}"
	);

	// The container case: a different working directory and an absolute --folder.
	let elsewhere = TempDir::new().expect("other cwd");
	let absolute_folder = root.join("database");
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(elsewhere.path())
		.args(["--host", "http://127.0.0.1:1"])
		.args(["rollout", "lint", &manifest])
		.args(["--folder", &absolute_folder.to_string_lossy()])
		.output()
		.expect("run surrealkit");

	let text = combined(&output);
	assert!(
		!text.contains("target schema hash mismatch"),
		"the manifest did not survive the environment change: {text}"
	);
	assert!(output.status.success(), "lint failed from another directory: {text}");
}
