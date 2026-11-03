//! The regression test that matters most for the stdio transport.
//!
//! On stdio, stdout *is* the JSON-RPC channel. SurrealKit's library modules
//! report progress through `log`, and the CLI's logger writes `info` to stdout --
//! so a single mis-wired logger, one reintroduced `println!`, or a subprocess
//! spawned with inherited stdio silently corrupts every message.
//!
//! Nothing else catches that: the code still compiles, the tools still work in
//! isolation, and the failure shows up as an unparseable frame in somebody's
//! editor. So this drives the real binary and asserts that **every** byte on
//! stdout is JSON-RPC.

use std::io::Write;
use std::process::{Command, Stdio};

use tempfile::TempDir;

/// A scaffolded project to serve.
fn project() -> TempDir {
	let dir = TempDir::new().expect("tempdir");
	let status = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.arg("init")
		.arg("--yes")
		.current_dir(dir.path())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.status()
		.expect("running init");
	assert!(status.success(), "init failed");
	dir
}

/// Drive `surrealkit mcp` with `requests` and return `(stdout, stderr)`.
fn drive(dir: &TempDir, requests: &[&str], verbose: bool) -> (String, String) {
	let mut cmd = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	cmd.arg("mcp").current_dir(dir.path());
	if verbose {
		cmd.arg("--verbose");
	}
	// A host that cannot be reached is fine: the point is what the server writes,
	// not whether the database answered.
	cmd.env("SURREALDB_HOST", "http://127.0.0.1:9");

	let mut child = cmd
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("spawning the server");

	{
		let stdin = child.stdin.as_mut().expect("stdin");
		for request in requests {
			writeln!(stdin, "{request}").expect("writing a request");
		}
	}
	// Dropping stdin closes it, which is how a stdio MCP client says goodbye.
	drop(child.stdin.take());

	let out = child.wait_with_output().expect("waiting for the server");
	(
		String::from_utf8_lossy(&out.stdout).into_owned(),
		String::from_utf8_lossy(&out.stderr).into_owned(),
	)
}

const INITIALIZE: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"purity","version":"0"}}}"#;
const INITIALIZED: &str = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;

/// Assert every non-empty stdout line is a JSON-RPC message, and return them.
fn assert_pure(stdout: &str) -> Vec<serde_json::Value> {
	let mut messages = Vec::new();
	for (index, line) in stdout.lines().enumerate() {
		if line.trim().is_empty() {
			continue;
		}
		let value: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|err| {
			panic!(
				"stdout line {index} is not JSON ({err}); something wrote to stdout on the \
				 JSON-RPC channel:\n{line}"
			)
		});
		assert_eq!(
			value.get("jsonrpc").and_then(|v| v.as_str()),
			Some("2.0"),
			"stdout line {index} is JSON but not JSON-RPC:\n{line}"
		);
		messages.push(value);
	}
	messages
}

#[test]
fn a_tool_call_that_logs_heavily_keeps_stdout_pure() {
	let dir = project();
	// `init` is the loudest tool that needs no database: its template engine
	// reports every file it writes, and those calls used to be `println!`.
	let (stdout, _stderr) = drive(
		&dir,
		&[
			INITIALIZE,
			INITIALIZED,
			r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"init","arguments":{}}}"#,
		],
		false,
	);
	let messages = assert_pure(&stdout);
	assert_eq!(messages.len(), 2, "expected one response per request, got {messages:#?}");
}

#[test]
fn the_progress_a_tool_logs_is_returned_rather_than_printed() {
	let dir = project();
	let (stdout, _stderr) = drive(
		&dir,
		&[
			INITIALIZE,
			INITIALIZED,
			r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"init","arguments":{}}}"#,
		],
		false,
	);
	let messages = assert_pure(&stdout);
	let call =
		messages.iter().find(|m| m.get("id") == Some(&serde_json::json!(2))).expect("response");
	let text = serde_json::to_string(call).expect("serialize");
	assert!(
		text.contains("Using template"),
		"the template engine's progress should reach the caller in the tool result, \
		 not vanish: {text}"
	);
}

#[test]
fn verbose_mode_does_not_leak_debug_logging_onto_stdout() {
	// `-v` raises the log level to debug. If the MCP path ever installed the
	// CLI's logger, this is where it would show.
	let dir = project();
	let (stdout, _stderr) = drive(
		&dir,
		&[
			INITIALIZE,
			INITIALIZED,
			r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"project_info","arguments":{}}}"#,
		],
		true,
	);
	assert_pure(&stdout);
}

#[test]
fn a_failing_tool_call_keeps_stdout_pure() {
	// The error path runs different code: context chains, a different result
	// shape, and a connection failure logged from deep inside the library.
	let dir = project();
	let (stdout, _stderr) = drive(
		&dir,
		&[
			INITIALIZE,
			INITIALIZED,
			r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"setup","arguments":{}}}"#,
		],
		false,
	);
	let messages = assert_pure(&stdout);
	assert_eq!(messages.len(), 2);
}

#[test]
fn the_servers_own_startup_logging_goes_to_stderr() {
	let dir = project();
	let (stdout, stderr) = drive(&dir, &[INITIALIZE, INITIALIZED], false);
	assert_pure(&stdout);
	assert!(
		stderr.contains("serving"),
		"the server should say what it is serving, on stderr: {stderr:?}"
	);
}
