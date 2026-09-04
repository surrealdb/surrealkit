use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

fn drain_connections(listener: &TcpListener, count: &mut usize) {
	loop {
		match listener.accept() {
			Ok((_stream, _address)) => *count += 1,
			Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
			Err(error) => panic!("listener failed: {error}"),
		}
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

fn write_cross_target_project(root: &Path) {
	fs::create_dir_all(root.join("database/modules/core/schema")).expect("core schema dir");
	fs::create_dir_all(root.join("database/modules/empty/schema")).expect("empty schema dir");
	fs::write(
		root.join("database/modules/core/schema/001_core.surql"),
		"DEFINE TABLE core SCHEMAFULL;\n",
	)
	.expect("core schema source");
	fs::write(
		root.join("surrealkit.toml"),
		"[schema.core]\n\
		 [schema.empty]\n\
		 [target.aaa_valid]\n\
		 schemas = [\"core\"]\n\
		 [target.zzz_empty]\n\
		 schemas = [\"empty\"]\n",
	)
	.expect("cross-target config");
}

#[cfg(unix)]
fn write_partially_unreadable_project(root: &Path) {
	write_project(root, true);
	std::os::unix::fs::symlink(
		root.join("database/schema"),
		root.join("database/schema/recursive-link"),
	)
	.expect("recursive schema symlink");
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
	selection_args: &[&str],
	sync_args: &[&str],
) -> (std::process::Output, usize) {
	let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
	listener.set_nonblocking(true).expect("nonblocking");
	let host = format!("http://{}", listener.local_addr().expect("address"));
	let (stop, stopped) = mpsc::channel();
	let server = thread::spawn(move || {
		let mut connections = 0;
		loop {
			drain_connections(&listener, &mut connections);
			match stopped.recv_timeout(Duration::from_millis(5)) {
				Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
					// The command has exited, so a final accept-queue drain is a
					// deterministic boundary for every connection it could have made.
					drain_connections(&listener, &mut connections);
					return connections;
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {}
			}
		}
	});

	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command.current_dir(root);
	command.args(["--host", &host, "--user", "fixture", "--pass", "fixture"]);
	command.args(selection_args);
	command.arg("sync");
	command.args(sync_args);
	let output = command.output().expect("run surrealkit");
	stop.send(()).expect("stop listener");
	let connections = server.join().expect("listener thread");
	(output, connections)
}

fn assert_no_connections(connections: usize, context: &str) {
	assert_eq!(connections, 0, "{context} connected before refusal");
}

#[test]
fn filesystem_cli_refuses_zero_sources_before_connecting() {
	let temp = TempDir::new().expect("tempdir");
	let corpus = temp.path().join("corpus");
	write_project(&corpus, true);
	let empty = temp.path().join("empty");
	write_project(&empty, false);
	let mixed_modules = temp.path().join("mixed-modules");
	write_multi_module_project(&mixed_modules);
	let cross_targets = temp.path().join("cross-targets");
	write_cross_target_project(&cross_targets);
	let custom_path = temp.path().join("custom-path");
	write_custom_path_project(&custom_path);
	let filtered_target = temp.path().join("filtered-target");
	write_filtered_target_project(&filtered_target);
	let mixed_targets = temp.path().join("mixed-targets");
	write_mixed_targets_project(&mixed_targets);
	let database = corpus.join("database");

	let zero_source_fixtures: [(&str, &Path, &[&str]); 3] = [
		("parent-root", temp.path(), &[]),
		("database-subdirectory", database.as_path(), &["--no-prune"]),
		("empty-schema", empty.as_path(), &["--watch"]),
	];
	for (id, root, sync_args) in zero_source_fixtures {
		let (output, connections) = run_sync(root, &[], sync_args);
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert!(!output.status.success(), "{id} unexpectedly succeeded");
		assert!(stderr.contains("refusing filesystem sync"), "{id}: {stderr}");
		assert!(stderr.contains("schema_module=default"), "{id}: {stderr}");
		assert!(stderr.contains("resolved_schema_dir="), "{id}: {stderr}");
		assert!(stderr.contains("source_count=0"), "{id}: {stderr}");
		assert_no_connections(connections, id);
	}

	// v1 fans one command out across schema modules. A valid earlier module must
	// not cause a connection before a later empty module is discovered.
	let (output, connections) = run_sync(&mixed_modules, &[], &["--dry-run"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "mixed modules unexpectedly succeeded");
	assert!(stderr.contains("schema_module=empty"), "{stderr}");
	assert!(stderr.contains("source_count=0"), "{stderr}");
	assert_no_connections(connections, "fan-out");

	// Preflight spans the complete target/module matrix. A valid module on the
	// alphabetically first target must not connect before a later target's empty
	// module is discovered.
	let (output, connections) = run_sync(&cross_targets, &["--all"], &["--dry-run"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "cross-target fan-out unexpectedly succeeded");
	assert!(stderr.contains("schema_module=empty"), "{stderr}");
	assert!(stderr.contains("source_count=0"), "{stderr}");
	assert_no_connections(connections, "cross-target fan-out");

	// A target/module filter that leaves no effective pair is also an empty
	// filesystem sync, not a successful no-op and not a reason to connect.
	let (output, connections) =
		run_sync(&filtered_target, &["--schema", "billing", "--target", "prod"], &["--dry-run"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "empty selection unexpectedly succeeded");
	assert!(stderr.contains("accept none of the selected schema modules"), "{stderr}");
	assert!(stderr.contains("applicable_pair_count=0"), "{stderr}");
	assert_no_connections(connections, "empty selection");

	// Discovery errors must not be silently converted into a partial source set.
	// With one valid file present, swallowing the recursive-link error would pass
	// the zero-source guard and connect with an incomplete view of the schema.
	#[cfg(unix)]
	{
		let partial = temp.path().join("partial-tree");
		write_partially_unreadable_project(&partial);
		let (output, connections) = run_sync(&partial, &[], &["--dry-run"]);
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert!(!output.status.success(), "partial traversal unexpectedly succeeded");
		assert!(stderr.contains("walking schema directory"), "{stderr}");
		assert_no_connections(connections, "partial traversal");
	}

	// A target accepting no selected modules is skipped in a mixed fan-out. Its
	// deliberately invalid endpoint must not prevent the applicable target from
	// reaching the listener.
	let (output, connections) = run_sync(&mixed_targets, &["--all"], &["--dry-run"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!stderr.contains("not-a-surrealdb-endpoint"), "empty target connected: {stderr}");
	assert!(connections > 0, "applicable target never attempted a database connection");

	// The explicit v1 escape hatch remains meaningful: an intentional empty
	// source set advances to the connection boundary instead of being preflighted.
	let (output, connections) = run_sync(&empty, &[], &["--dry-run", "--allow-empty-prune"]);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(!stderr.contains("source_count=0"), "explicit override was refused: {stderr}");
	assert!(connections > 0, "explicit empty prune never attempted a database connection");

	let source_fixtures =
		[("correct-corpus-root", corpus.as_path()), ("custom-schema-path", custom_path.as_path())];
	for (id, root) in source_fixtures {
		let (output, connections) = run_sync(root, &[], &["--dry-run"]);
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert!(!stderr.contains("source_count=0"), "{id} was refused: {stderr}");
		assert!(connections > 0, "{id} never attempted database connection");
	}
}
