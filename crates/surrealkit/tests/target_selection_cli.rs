//! The single-database commands connect to the target the selection names.
//!
//! `setup`, `seed`, `status` and `apply` once connected through the ambient
//! connection whatever `--target` said, so `seed -t staging` seeded the ambient
//! database and `apply -t prod` ran its file there. These tests run the binary
//! with the ambient connection and the target pointing at different places and
//! check which one each command reached.
#![expect(
	clippy::print_stderr,
	reason = "the live test says on stderr that it skipped without a server"
)]

use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
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

/// A TCP listener that never answers. Joining the handle after the sender is
/// used gives the number of connections made to it.
fn silent_server() -> (String, mpsc::Sender<()>, thread::JoinHandle<usize>) {
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
					// The command has exited, so a final drain counts every
					// connection it could have made.
					drain_connections(&listener, &mut connections);
					return connections;
				}
				Err(mpsc::RecvTimeoutError::Timeout) => {}
			}
		}
	});
	(host, stop, server)
}

fn write_project(root: &Path, config: &str, seed_table: &str) {
	fs::create_dir_all(root.join("database/schema")).expect("schema dir");
	fs::create_dir_all(root.join("database/seed")).expect("seed dir");
	fs::write(root.join("database/schema/001_marker.surql"), "DEFINE TABLE marker SCHEMALESS;\n")
		.expect("schema file");
	fs::write(
		root.join("database/seed/001_marker.surql"),
		format!("CREATE {seed_table}:one SET seeded = true;\n"),
	)
	.expect("seed file");
	fs::write(root.join("apply.surql"), "CREATE marker:applied;\n").expect("apply file");
	fs::write(root.join("surrealkit.toml"), config).expect("surrealkit.toml");
}

/// The binary in `root`, with nothing from the caller's environment but `PATH`.
fn surrealkit(root: &Path) -> Command {
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command.current_dir(root).env_clear();
	if let Some(path) = std::env::var_os("PATH") {
		command.env("PATH", path);
	}
	command
}

/// Run `args` with the ambient connection at `ambient`, which never answers.
fn run_against(root: &Path, ambient: &str, args: &[&str]) -> Output {
	surrealkit(root)
		.env("SURREALDB_HOST", ambient)
		.env("SURREALDB_USER", "root")
		.env("SURREALDB_PASSWORD", "root")
		.env("SURREALDB_CONNECT_TIMEOUT_SECS", "1")
		.args(args)
		.output()
		.expect("run surrealkit")
}

#[test]
fn each_single_database_command_connects_to_the_named_target() {
	// `compat raise` takes `--yes` and no `--wait`: it connects before it looks at
	// `--yes`, and without `--wait` it has nothing to poll, so the connect timeout
	// is the only wait.
	for command in [
		["setup"].as_slice(),
		&["seed"],
		&["status"],
		&["apply", "apply.surql"],
		&["compat", "status"],
		&["compat", "raise", "--yes"],
	] {
		let temp = TempDir::new().expect("tempdir");
		let (ambient, stop_ambient, ambient_server) = silent_server();
		let (target, stop_target, target_server) = silent_server();
		write_project(
			temp.path(),
			&format!(
				"[target.local]\nhost = {target:?}\nns = \"elsewhere\"\ndb = \"elsewhere\"\n\
				 connect_timeout_secs = 1\n"
			),
			"marker",
		);
		let args: Vec<&str> = ["-t", "local"].iter().chain(command).copied().collect();
		let output = run_against(temp.path(), &ambient, &args);
		stop_ambient.send(()).expect("stop ambient");
		stop_target.send(()).expect("stop target");
		let ambient = ambient_server.join().expect("ambient listener");
		let target = target_server.join().expect("target listener");
		// Neither listener answers, so the command fails once it times out; only
		// where it tried to connect matters here.
		assert!(!output.status.success(), "{command:?}: nothing answered, so it cannot succeed");
		assert_eq!(ambient, 0, "{command:?} connected to the ambient database");
		assert!(target > 0, "{command:?} never connected to target `local`");
	}
}

#[test]
fn a_primary_target_is_used_without_a_target_flag() {
	let temp = TempDir::new().expect("tempdir");
	let (ambient, stop_ambient, ambient_server) = silent_server();
	let (target, stop_target, target_server) = silent_server();
	write_project(
		temp.path(),
		&format!(
			"[target.local]\nhost = {target:?}\nprimary = true\nconnect_timeout_secs = 1\n\
			 [target.other]\nns = \"other\"\n"
		),
		"marker",
	);
	let output = run_against(temp.path(), &ambient, &["seed"]);
	stop_ambient.send(()).expect("stop ambient");
	stop_target.send(()).expect("stop target");
	assert!(!output.status.success());
	assert_eq!(
		ambient_server.join().expect("ambient listener"),
		0,
		"seed used the ambient database"
	);
	assert!(target_server.join().expect("target listener") > 0, "seed skipped the primary target");
}

#[test]
fn several_targets_at_once_are_refused_before_connecting() {
	let temp = TempDir::new().expect("tempdir");
	let (ambient, stop_ambient, ambient_server) = silent_server();
	write_project(temp.path(), "[target.a]\nns = \"a\"\n[target.b]\nns = \"b\"\n", "marker");
	for args in [["--all", "seed"].as_slice(), &["-t", "a", "-t", "b", "apply", "apply.surql"]] {
		let output = run_against(temp.path(), &ambient, args);
		let stderr = String::from_utf8_lossy(&output.stderr);
		assert!(!output.status.success(), "{args:?} succeeded");
		assert!(
			stderr.contains("one database target at a time") && stderr.contains("--target"),
			"{args:?}: {stderr}"
		);
	}
	stop_ambient.send(()).expect("stop ambient");
	assert_eq!(ambient_server.join().expect("ambient listener"), 0, "connected before refusing");
}

async fn count_in(db: &Surreal<Any>, ns: &str, name: &str, table: &str) -> i64 {
	db.use_ns(ns).use_db(name).await.expect("select database");
	let mut response =
		db.query(format!("SELECT count() FROM {table} GROUP ALL;")).await.expect("count rows");
	let count: Option<i64> = response.take((0, "count")).unwrap_or_default();
	count.unwrap_or(0)
}

/// With a server at `SURREALKIT_TEST_URL`: `seed -t` writes into the target's
/// namespace and database, and nothing into the ambient one.
#[tokio::test]
async fn seed_writes_into_the_target_namespace_and_database() {
	let Some(url) = std::env::var("SURREALKIT_TEST_URL").ok().filter(|url| !url.is_empty()) else {
		eprintln!("SKIP: set SURREALKIT_TEST_URL to run against a server");
		return;
	};
	let user = std::env::var("SURREALKIT_TEST_USER").unwrap_or_else(|_| "root".into());
	let pass = std::env::var("SURREALKIT_TEST_PASS").unwrap_or_else(|_| "secret".into());
	let id = std::process::id();
	let (target_ns, ambient_ns, table) =
		(format!("kit_target_{id}"), format!("kit_ambient_{id}"), format!("marker_{id}"));

	let temp = TempDir::new().expect("tempdir");
	write_project(
		temp.path(),
		&format!("[target.local]\nns = {target_ns:?}\ndb = {target_ns:?}\n"),
		&table,
	);
	let output = surrealkit(temp.path())
		.env("SURREALDB_HOST", &url)
		.env("SURREALDB_NAMESPACE", &ambient_ns)
		.env("SURREALDB_NAME", &ambient_ns)
		.env("SURREALDB_USER", &user)
		.env("SURREALDB_PASSWORD", &pass)
		.args(["-t", "local", "seed"])
		.output()
		.expect("run surrealkit");
	assert!(output.status.success(), "seed failed: {}", String::from_utf8_lossy(&output.stderr));

	let db = connect(url.as_str()).await.expect("connect to SURREALKIT_TEST_URL");
	db.signin(Root {
		username: user,
		password: pass,
	})
	.await
	.expect("signin to SURREALKIT_TEST_URL");
	let in_target = count_in(&db, &target_ns, &target_ns, &table).await;
	let in_ambient = count_in(&db, &ambient_ns, &ambient_ns, &table).await;
	for namespace in [&target_ns, &ambient_ns] {
		let _ = db.query(format!("REMOVE NAMESPACE IF EXISTS {namespace};")).await;
	}
	assert_eq!(in_target, 1, "the seed did not land in target `local` ({target_ns})");
	assert_eq!(in_ambient, 0, "the seed landed in the ambient database");
}
