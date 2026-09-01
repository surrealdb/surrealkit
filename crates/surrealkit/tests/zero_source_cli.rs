use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

static CWD_LOCK: Mutex<()> = Mutex::new(());

struct RestoreDir {
	original: PathBuf,
	_guard: MutexGuard<'static, ()>,
}

impl RestoreDir {
	fn enter(path: &Path) -> Self {
		let guard = CWD_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		let original = std::env::current_dir().expect("current directory");
		std::env::set_current_dir(path).expect("enter fixture");
		Self {
			original,
			_guard: guard,
		}
	}
}

impl Drop for RestoreDir {
	fn drop(&mut self) {
		std::env::set_current_dir(&self.original).expect("restore current directory");
	}
}

fn write_project(root: &Path, with_source: bool) {
	fs::create_dir_all(root.join("database/schema")).expect("schema dir");
	fs::write(root.join("surrealkit.toml"), "").expect("project marker");
	if with_source {
		fs::write(
			root.join("database/schema/001_fixture.surql"),
			"DEFINE TABLE fixture SCHEMAFULL;\n",
		)
		.expect("schema source");
	}
}

fn write_multi_module_project(root: &Path) {
	fs::create_dir_all(root.join("database/modules/core/schema")).expect("core schema dir");
	fs::create_dir_all(root.join("database/modules/empty/schema")).expect("empty schema dir");
	fs::write(
		root.join("database/modules/core/schema/001_core.surql"),
		"DEFINE TABLE core SCHEMAFULL;\n",
	)
	.expect("core schema source");
	fs::write(root.join("surrealkit.toml"), "[schema.core]\n[schema.empty]\n")
		.expect("project modules");
}

fn write_custom_path_project(root: &Path) {
	fs::create_dir_all(root.join("database/custom/core")).expect("custom schema dir");
	fs::write(
		root.join("database/custom/core/001_core.surql"),
		"DEFINE TABLE custom_core SCHEMAFULL;\n",
	)
	.expect("custom schema source");
	fs::write(root.join("surrealkit.toml"), "[schema.core]\npath = \"custom/core\"\n")
		.expect("custom path config");
}

fn write_filtered_target_project(root: &Path) {
	fs::create_dir_all(root.join("database/modules/core/schema")).expect("core schema dir");
	fs::create_dir_all(root.join("database/modules/billing/schema")).expect("billing schema dir");
	fs::write(
		root.join("database/modules/core/schema/001_core.surql"),
		"DEFINE TABLE core SCHEMAFULL;\n",
	)
	.expect("core schema source");
	fs::write(
		root.join("database/modules/billing/schema/001_billing.surql"),
		"DEFINE TABLE billing SCHEMAFULL;\n",
	)
	.expect("billing schema source");
	fs::write(
		root.join("surrealkit.toml"),
		"[schema.billing]\n[schema.core]\n[target.prod]\nschemas = [\"core\"]\n",
	)
	.expect("filtered target config");
}

fn write_mixed_targets_project(root: &Path) {
	fs::create_dir_all(root.join("database/modules/core/schema")).expect("core schema dir");
	fs::write(
		root.join("database/modules/core/schema/001_core.surql"),
		"DEFINE TABLE core SCHEMAFULL;\n",
	)
	.expect("core schema source");
	fs::write(
		root.join("surrealkit.toml"),
		"[schema.core]\n\
		 [target.aaa_empty]\n\
		 host = \"not-a-surrealdb-endpoint\"\n\
		 schemas = []\n\
		 [target.zzz_valid]\n\
		 schemas = [\"core\"]\n",
	)
	.expect("mixed targets config");
}

fn run_sync(
	root: &Path,
	host: &str,
	selection_args: &[&str],
	allow_empty_prune: bool,
) -> std::process::Output {
	let _restore = RestoreDir::enter(root);
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command.args(["--host", host, "--user", "fixture", "--pass", "fixture"]);
	command.args(selection_args);
	command.args(["sync", "--dry-run"]);
	if allow_empty_prune {
		command.arg("--allow-empty-prune");
	}
	command.output().expect("run surrealkit")
}

fn wait_for_connections(count: &AtomicUsize, minimum: usize) -> bool {
	let deadline = Instant::now() + Duration::from_secs(2);
	while Instant::now() < deadline {
		if count.load(Ordering::SeqCst) >= minimum {
			return true;
		}
		thread::sleep(Duration::from_millis(10));
	}
	false
}

fn remove_statement_count(stdout: &str, stderr: &str) -> usize {
	stdout
		.lines()
		.chain(stderr.lines())
		.filter(|line| line.trim_start().starts_with("REMOVE "))
		.count()
}

fn assert_no_connections(count: &AtomicUsize, context: &str) {
	// Give the listener thread time to accept a connection already queued by the
	// kernel; an immediate counter read would permit a fast-connect/fast-exit race.
	thread::sleep(Duration::from_millis(100));
	assert_eq!(count.load(Ordering::SeqCst), 0, "{context} connected before refusal");
}

#[test]
fn filesystem_cli_refuses_zero_sources_before_connecting() {
	let temp = TempDir::new().expect("tempdir");
	let corpus = temp.path().join("corpus");
	write_project(&corpus, true);
	let one_file = temp.path().join("one-file");
	write_project(&one_file, true);
	let empty = temp.path().join("empty");
	write_project(&empty, false);
	let mixed_modules = temp.path().join("mixed-modules");
	write_multi_module_project(&mixed_modules);
	let custom_path = temp.path().join("custom-path");
	write_custom_path_project(&custom_path);
	let filtered_target = temp.path().join("filtered-target");
	write_filtered_target_project(&filtered_target);
	let mixed_targets = temp.path().join("mixed-targets");
	write_mixed_targets_project(&mixed_targets);
	let database = corpus.join("database");

	let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
	listener.set_nonblocking(true).expect("nonblocking");
	let host = format!("http://{}", listener.local_addr().expect("address"));
	let connection_count = Arc::new(AtomicUsize::new(0));
	let stop = Arc::new(AtomicBool::new(false));
	let server_count = Arc::clone(&connection_count);
	let server_stop = Arc::clone(&stop);
	let server = thread::spawn(move || {
		while !server_stop.load(Ordering::SeqCst) {
			match listener.accept() {
				Ok((_stream, _address)) => {
					server_count.fetch_add(1, Ordering::SeqCst);
				}
				Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
					thread::sleep(Duration::from_millis(5));
				}
				Err(error) => panic!("listener failed: {error}"),
			}
		}
	});

	let zero_source_fixtures = [
		("parent-root", temp.path()),
		("database-subdirectory", database.as_path()),
		("empty-schema", empty.as_path()),
	];
	for (id, root) in zero_source_fixtures {
		let output = run_sync(root, &host, &[], false);
		let stdout = String::from_utf8_lossy(&output.stdout);
		let stderr = String::from_utf8_lossy(&output.stderr);
		let prune_line_count = remove_statement_count(&stdout, &stderr);
		assert!(!output.status.success(), "{id} unexpectedly succeeded");
		assert!(stderr.contains("refusing filesystem sync"), "{id}: {stderr}");
		assert!(stderr.contains("schema_module=default"), "{id}: {stderr}");
		assert!(stderr.contains("resolved_schema_dir="), "{id}: {stderr}");
		assert!(stderr.contains("source_count=0"), "{id}: {stderr}");
		assert_eq!(prune_line_count, 0, "{id}: stdout={stdout} stderr={stderr}");
		assert_no_connections(&connection_count, id);
	}

	// v1 fans one command out across schema modules. A valid earlier module must
	// not cause a connection before a later empty module is discovered.
	let output = run_sync(&mixed_modules, &host, &[], false);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "mixed modules unexpectedly succeeded");
	assert!(stderr.contains("schema_module=empty"), "{stderr}");
	assert!(stderr.contains("source_count=0"), "{stderr}");
	assert_no_connections(&connection_count, "fan-out");

	// A target/module filter that leaves no effective pair is also an empty
	// filesystem sync, not a successful no-op and not a reason to connect.
	let output =
		run_sync(&filtered_target, &host, &["--schema", "billing", "--target", "prod"], false);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "empty selection unexpectedly succeeded");
	assert!(stderr.contains("accept none of the selected schema modules"), "{stderr}");
	assert!(stderr.contains("source_count=0"), "{stderr}");
	assert_no_connections(&connection_count, "empty selection");

	// A target accepting no selected modules is skipped in a mixed fan-out. Its
	// deliberately invalid endpoint must not prevent the applicable target from
	// reaching the listener.
	let expected_connections = connection_count.load(Ordering::SeqCst) + 1;
	let output = run_sync(&mixed_targets, &host, &["--all"], false);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!stderr.contains("not-a-surrealdb-endpoint"), "empty target connected: {stderr}");
	assert!(
		wait_for_connections(&connection_count, expected_connections),
		"applicable target never attempted a database connection"
	);

	// The explicit v1 escape hatch remains meaningful: an intentional empty
	// source set advances to the connection boundary instead of being preflighted.
	let expected_connections = connection_count.load(Ordering::SeqCst) + 1;
	let output = run_sync(&empty, &host, &[], true);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!stderr.contains("source_count=0"), "explicit override was refused: {stderr}");
	assert!(
		wait_for_connections(&connection_count, expected_connections),
		"explicit empty prune never attempted a database connection"
	);

	let source_fixtures = [
		("correct-corpus-root", corpus.as_path()),
		("one-file-schema", one_file.as_path()),
		("custom-schema-path", custom_path.as_path()),
	];
	for (id, root) in source_fixtures {
		let expected_connections = connection_count.load(Ordering::SeqCst) + 1;
		let output = run_sync(root, &host, &[], false);
		let stdout = String::from_utf8_lossy(&output.stdout);
		let stderr = String::from_utf8_lossy(&output.stderr);
		let prune_line_count = remove_statement_count(&stdout, &stderr);
		assert!(!stderr.contains("source_count=0"), "{id} was refused: {stderr}");
		assert_eq!(prune_line_count, 0, "{id}: stdout={stdout} stderr={stderr}");
		assert!(
			wait_for_connections(&connection_count, expected_connections),
			"{id} never attempted database connection"
		);
	}

	stop.store(true, Ordering::SeqCst);
	server.join().expect("listener thread");
}
