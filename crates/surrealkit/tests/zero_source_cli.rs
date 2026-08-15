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
	fs::write(root.join("surrealkit.toml"), "[project]\nname='fixture'\n").expect("project marker");
	if with_source {
		fs::write(
			root.join("database/schema/001_fixture.surql"),
			"DEFINE TABLE fixture SCHEMAFULL;\n",
		)
		.expect("schema source");
	}
}

fn run_sync(root: &Path, host: &str) -> std::process::Output {
	let _restore = RestoreDir::enter(root);
	Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.args(["--host", host, "--user", "fixture", "--pass", "fixture", "sync", "--dry-run"])
		.output()
		.expect("run surrealkit")
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

#[test]
fn filesystem_cli_refuses_zero_sources_before_connecting() {
	let temp = TempDir::new().expect("tempdir");
	let corpus = temp.path().join("corpus");
	write_project(&corpus, true);
	let one_file = temp.path().join("one-file");
	write_project(&one_file, true);
	let empty = temp.path().join("empty");
	write_project(&empty, false);
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
		let output = run_sync(root, &host);
		let stdout = String::from_utf8_lossy(&output.stdout);
		let stderr = String::from_utf8_lossy(&output.stderr);
		let prune_line_count = stdout
			.lines()
			.chain(stderr.lines())
			.filter(|line| line.contains("REMOVE") || line.to_ascii_lowercase().contains("prune"))
			.count();
		assert!(!output.status.success(), "{id} unexpectedly succeeded");
		assert!(stderr.contains("refusing filesystem sync"), "{id}: {stderr}");
		assert!(stderr.contains("expected_marker=surrealkit.toml"), "{id}: {stderr}");
		assert!(stderr.contains("source_count=0"), "{id}: {stderr}");
		assert_eq!(prune_line_count, 0, "{id}: stdout={stdout} stderr={stderr}");
		assert_eq!(connection_count.load(Ordering::SeqCst), 0, "{id} connected before refusal");
		println!(
			"fixture={id} exit_code={} diagnostic=zero-source source_count=0 database_queries=0 connections=0 prune_lines={prune_line_count}",
			output.status.code().unwrap_or(-1)
		);
	}

	let source_fixtures =
		[("correct-corpus-root", corpus.as_path()), ("one-file-schema", one_file.as_path())];
	for (id, root) in source_fixtures {
		let expected_connections = connection_count.load(Ordering::SeqCst) + 1;
		let output = run_sync(root, &host);
		let stdout = String::from_utf8_lossy(&output.stdout);
		let stderr = String::from_utf8_lossy(&output.stderr);
		let prune_line_count = stdout
			.lines()
			.chain(stderr.lines())
			.filter(|line| line.contains("REMOVE") || line.to_ascii_lowercase().contains("prune"))
			.count();
		assert!(!stderr.contains("source_count=0"), "{id} was refused: {stderr}");
		assert_eq!(prune_line_count, 0, "{id}: stdout={stdout} stderr={stderr}");
		assert!(
			wait_for_connections(&connection_count, expected_connections),
			"{id} never attempted database connection"
		);
		println!(
			"fixture={id} exit_code={} diagnostic=connection-rejected source_count=1 database_queries=0 connections=1 prune_lines={prune_line_count}",
			output.status.code().unwrap_or(-1)
		);
	}

	stop.store(true, Ordering::SeqCst);
	server.join().expect("listener thread");
}
