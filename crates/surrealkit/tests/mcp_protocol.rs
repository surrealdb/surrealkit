//! End-to-end protocol tests, driven by a real MCP client.
//!
//! The client and server are joined by an in-memory duplex, so these exercise
//! genuine `initialize` / `tools/list` / `tools/call` / `resources/read` /
//! `prompts/get` round trips -- serde round-tripping, capability declaration and
//! handler routing included -- without a socket or a database.

use std::collections::BTreeSet;

use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, GetPromptRequestParams, ReadResourceRequestParams};
use rmcp::service::{RoleClient, RunningService};
use surrealkit::config::DbOverrides;
use surrealkit::mcp::{ServerConfig, SurrealKitMcp};
use tempfile::TempDir;

/// A scaffolded project, plus a client wired to a server rooted in it.
async fn connect(dir: &TempDir) -> RunningService<RoleClient, ()> {
	let config = ServerConfig::new(
		dir.path().to_path_buf(),
		DbOverrides {
			// Nothing here should reach a database; point somewhere closed so a
			// mistake fails fast instead of hanging.
			host: Some("http://127.0.0.1:9".to_string()),
			..DbOverrides::default()
		},
		Vec::new(),
	)
	.expect("server config");
	let server = SurrealKitMcp::new(config);

	let (server_side, client_side) = tokio::io::duplex(8192);
	tokio::spawn(async move {
		if let Ok(running) = server.serve(server_side).await {
			let _ = running.waiting().await;
		}
	});
	().serve(client_side).await.expect("client handshake")
}

/// These request params are `#[non_exhaustive]`, so they are built by mutation
/// rather than a struct literal.
fn call_params(
	name: &str,
	arguments: serde_json::Map<String, serde_json::Value>,
) -> CallToolRequestParams {
	let mut params = CallToolRequestParams::default();
	params.name = name.to_string().into();
	params.arguments = Some(arguments);
	params
}

fn read_params(uri: &str) -> ReadResourceRequestParams {
	ReadResourceRequestParams::new(uri)
}

fn prompt_params(
	name: &str,
	arguments: serde_json::Map<String, serde_json::Value>,
) -> GetPromptRequestParams {
	let mut params = GetPromptRequestParams::default();
	params.name = name.to_string();
	params.arguments = Some(arguments);
	params
}

fn project() -> TempDir {
	let dir = TempDir::new().expect("tempdir");
	let status = std::process::Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.arg("init")
		.arg("--yes")
		.current_dir(dir.path())
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::null())
		.status()
		.expect("running init");
	assert!(status.success());
	dir
}

#[tokio::test]
async fn the_server_declares_what_it_supports() {
	let dir = project();
	let client = connect(&dir).await;
	let info = client.peer_info().expect("server info");

	assert_eq!(info.server_info.name, "surrealkit");
	assert!(info.capabilities.tools.is_some(), "tools must be advertised");
	assert!(info.capabilities.resources.is_some(), "resources must be advertised");
	assert!(info.capabilities.prompts.is_some(), "prompts must be advertised");

	let instructions = info.instructions.as_deref().unwrap_or_default();
	assert!(
		instructions.contains("sync") && instructions.contains("ollout"),
		"the instructions should explain the sync-vs-rollout choice: {instructions}"
	);
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn every_advertised_tool_is_listed_once() {
	let dir = project();
	let client = connect(&dir).await;
	let tools = client.list_all_tools().await.expect("tools/list");

	let names: BTreeSet<String> = tools.iter().map(|t| t.name.to_string()).collect();
	assert_eq!(names.len(), tools.len(), "duplicate tool names");
	assert!(names.contains("sync"), "have: {names:?}");
	assert!(names.contains("rollout_status"), "have: {names:?}");
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn a_read_only_tool_returns_structured_content() {
	let dir = project();
	let client = connect(&dir).await;

	let result = client
		.call_tool(call_params("project_info", serde_json::Map::new()))
		.await
		.expect("tools/call");

	assert_ne!(result.is_error, Some(true), "project_info should succeed: {result:?}");
	let structured = result.structured_content.expect("structured content");
	// Canonicalised: on macOS a temp dir under /var resolves to /private/var, and
	// the server pins the real path so containment checks cannot be fooled by a
	// symlink.
	let expected = dir.path().canonicalize().expect("canonicalize");
	assert_eq!(
		structured.get("project_root").and_then(|v| v.as_str()),
		Some(expected.to_string_lossy().as_ref()),
		"the report should name the project it resolved"
	);
	assert!(
		structured.get("folder").and_then(|v| v.as_str()).is_some_and(|f| f.ends_with("database")),
		"the folder should be absolute and rooted in the project: {structured:?}"
	);
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn a_tool_result_never_carries_the_password() {
	// The connection is reported so an agent can see what it is about to touch.
	// The credential is not part of that.
	let dir = project();
	let config = ServerConfig::new(
		dir.path().to_path_buf(),
		DbOverrides {
			host: Some("http://127.0.0.1:9".to_string()),
			pass: Some("hunter2-should-never-appear".to_string()),
			..DbOverrides::default()
		},
		Vec::new(),
	)
	.expect("config");
	let server = SurrealKitMcp::new(config);
	let (server_side, client_side) = tokio::io::duplex(8192);
	tokio::spawn(async move {
		if let Ok(running) = server.serve(server_side).await {
			let _ = running.waiting().await;
		}
	});
	let client = ().serve(client_side).await.expect("handshake");

	let result = client
		.call_tool(call_params("project_info", serde_json::Map::new()))
		.await
		.expect("tools/call");

	let rendered = serde_json::to_string(&result).expect("serialize");
	assert!(
		!rendered.contains("hunter2-should-never-appear"),
		"the password leaked into a tool result: {rendered}"
	);
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn a_destructive_tool_refuses_without_confirm_and_says_why() {
	let dir = project();
	let client = connect(&dir).await;

	let result =
		client.call_tool(call_params("sync", serde_json::Map::new())).await.expect("tools/call");

	assert_eq!(result.is_error, Some(true), "sync must refuse without confirm");
	let text = serde_json::to_string(&result.content).expect("serialize");
	assert!(text.contains("confirm"), "the refusal must name the flag: {text}");
	assert!(
		text.contains("REMOVE") || text.contains("no undo"),
		"the refusal must say what is at stake: {text}"
	);
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn resources_are_listed_and_readable() {
	let dir = project();
	let client = connect(&dir).await;

	let resources = client.list_all_resources().await.expect("resources/list");
	let uris: BTreeSet<String> = resources.iter().map(|r| r.uri.clone()).collect();
	assert!(uris.contains("surrealkit://config"), "have: {uris:?}");

	let read =
		client.read_resource(read_params("surrealkit://config")).await.expect("resources/read");
	assert!(!read.contents.is_empty(), "surrealkit.toml should have content");
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn a_resource_uri_cannot_escape_the_project() {
	let dir = project();
	let client = connect(&dir).await;

	let result =
		client.read_resource(read_params("surrealkit://schema/../../../../etc/passwd")).await;

	assert!(result.is_err(), "path traversal must be refused, got {result:?}");
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn prompts_are_listed_and_rendered() {
	let dir = project();
	let client = connect(&dir).await;

	let prompts = client.list_all_prompts().await.expect("prompts/list");
	let names: BTreeSet<String> = prompts.iter().map(|p| p.name.clone()).collect();
	assert!(names.contains("author_rollout"), "have: {names:?}");

	let mut arguments = serde_json::Map::new();
	arguments.insert("change".to_string(), serde_json::json!("rename person.name"));
	let rendered =
		client.get_prompt(prompt_params("author_rollout", arguments)).await.expect("prompts/get");

	let text = serde_json::to_string(&rendered.messages).expect("serialize");
	assert!(text.contains("rename person.name"), "the argument should be interpolated: {text}");
	assert!(text.contains("rollback"), "the prompt should cover the phases: {text}");
	client.cancel().await.expect("shutdown");
}

#[tokio::test]
async fn an_unknown_tool_is_an_error_rather_than_a_panic() {
	let dir = project();
	let client = connect(&dir).await;
	let result = client.call_tool(call_params("not_a_tool", serde_json::Map::new())).await;
	assert!(result.is_err(), "expected a protocol error, got {result:?}");
	client.cancel().await.expect("shutdown");
}
