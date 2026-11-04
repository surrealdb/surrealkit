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

#[tokio::test]
async fn rollout_discard_refuses_a_manifest_whose_id_leaves_the_rollouts_dir() {
	// `discard` deletes `rollouts/<id>/` recursively, naming it from the id inside
	// the manifest. A manifest is a project file, so its id is untrusted input: one
	// sitting inside rollouts/ can still say `../../src`.
	let dir = project();
	let victim = dir.path().join("src");
	std::fs::create_dir_all(&victim).expect("mkdir");
	std::fs::write(victim.join("keep.txt"), "do not delete").expect("write");

	// `<folder>/rollouts/../../src` is `<root>/src`.
	let rollouts = dir.path().join("database").join("rollouts");
	std::fs::create_dir_all(&rollouts).expect("mkdir");
	std::fs::write(rollouts.join("crafted.toml"), "id = \"../../src\"\nname = \"crafted\"\n")
		.expect("write");

	let client = connect(&dir).await;
	let mut arguments = serde_json::Map::new();
	arguments.insert("rollout_id".to_string(), serde_json::json!("crafted.toml"));
	// Skips the snapshot checks, which is the shortest path to the delete.
	arguments.insert("keep_snapshots".to_string(), serde_json::json!(true));
	arguments.insert("confirm".to_string(), serde_json::json!(true));
	let result =
		client.call_tool(call_params("rollout_discard", arguments)).await.expect("tools/call");

	assert_eq!(result.is_error, Some(true), "a crafted id must be refused: {result:?}");
	let text = serde_json::to_string(&result.content).expect("serialize");
	assert!(text.contains("not a plain name"), "the refusal should say why: {text}");
	assert!(victim.join("keep.txt").exists(), "the crafted id reached the recursive delete");
	assert!(rollouts.join("crafted.toml").exists(), "nothing should have been deleted at all");
	client.cancel().await.expect("shutdown");
}

/// The check tools, which need `analyze` as well as `mcp`.
#[cfg(feature = "analyze")]
mod check {
	use std::time::{Duration, Instant};

	use super::*;

	/// A project written by hand: `surrealkit.toml` and `(path, text)` files under the root.
	fn written(config: &str, files: &[(&str, &str)]) -> TempDir {
		let dir = TempDir::new().expect("tempdir");
		if !config.is_empty() {
			std::fs::write(dir.path().join("surrealkit.toml"), config).expect("config");
		}
		for (path, text) in files {
			let path = dir.path().join(path);
			std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
			std::fs::write(path, text).expect("file");
		}
		dir
	}

	async fn call(
		client: &RunningService<RoleClient, ()>,
		tool: &str,
		arguments: serde_json::Value,
	) -> rmcp::model::CallToolResult {
		let serde_json::Value::Object(arguments) = arguments else {
			panic!("arguments are an object");
		};
		client.call_tool(call_params(tool, arguments)).await.expect("tools/call")
	}

	fn text(result: &rmcp::model::CallToolResult) -> String {
		serde_json::to_string(&result.content).expect("serialize")
	}

	const BAD_DIRECTIVE: &str = "SELECT 1;\n-- surreal: allow(*)\nSELECT 2;\n";

	/// `core` is clean and uses a template variable the config sets, so a call that read
	/// another directory's `surrealkit.toml` (this crate's, which has none) fails it as K9002;
	/// `billing` never applies. `dev` accepts only `core`.
	const MODULES: &str = "[schema.core]\n[schema.billing]\n[target.dev]\nschemas = [\"core\"]\n\
	                       [target.prod]\n[variables]\nPREFIX = \"app\"\n";

	fn modules() -> TempDir {
		written(
			MODULES,
			&[
				(
					"database/modules/core/schema/a.surql",
					"DEFINE TABLE ${PREFIX}_t SCHEMAFULL;\nDEFINE FIELD a ON ${PREFIX}_t TYPE string;\n",
				),
				(
					"database/modules/billing/schema/b.surql",
					"DEFINE FIELD x.y ON nope TYPE string;\n",
				),
			],
		)
	}

	#[tokio::test]
	async fn check_query_passes_a_clean_query_and_says_it_was_not_analyzed() {
		let dir = written("", &[("database/schema/a.surql", "DEFINE TABLE t SCHEMAFULL;\n")]);
		let client = connect(&dir).await;
		let result =
			call(&client, "check_query", serde_json::json!({"query": "SELECT * FROM t;"})).await;
		assert_ne!(result.is_error, Some(true), "{result:?}");
		let found = result.structured_content.clone().expect("structured content");
		assert_eq!(found["diagnostics"], serde_json::json!([]));
		assert_eq!(found["passed"], true);
		assert_eq!(found["analyzed"], false);
		assert_eq!(found["schema_errors"], 0);
		assert!(text(&result).contains("not analyzed: needs SurrealDB 4.1"), "{}", text(&result));
		client.cancel().await.expect("shutdown");
	}

	#[tokio::test]
	async fn check_query_positions_a_finding_within_the_query_text() {
		let dir = written("", &[]);
		let client = connect(&dir).await;
		let result = call(
			&client,
			"check_query",
			serde_json::json!({"query": BAD_DIRECTIVE, "name": "list.surql"}),
		)
		.await;
		let found = result.structured_content.expect("structured content");
		let diagnostics = found["diagnostics"].as_array().expect("diagnostics");
		assert_eq!(diagnostics.len(), 1, "{found}");
		let diagnostic = &diagnostics[0];
		assert_eq!(diagnostic["code"], "L7013");
		assert_eq!(diagnostic["path"], "list.surql");
		let span = &diagnostic["span"];
		let (start, end) = (span["start"].as_u64().unwrap(), span["end"].as_u64().unwrap());
		assert!(
			BAD_DIRECTIVE[start as usize..end as usize].contains("allow(*)"),
			"the span is within the query: {span}"
		);
		assert_eq!((span["line"].as_u64(), span["column"].as_u64()), (Some(2), Some(1)));
		assert_eq!(found["warnings"], 1);
		assert_eq!(found["passed"], true);
		client.cancel().await.expect("shutdown");
	}

	#[tokio::test]
	async fn check_query_refuses_a_target_the_project_does_not_declare() {
		let dir = modules();
		let client = connect(&dir).await;
		let result = call(
			&client,
			"check_query",
			serde_json::json!({"query": "SELECT 1;", "target": "staging"}),
		)
		.await;
		assert_eq!(result.is_error, Some(true), "{result:?}");
		let text = text(&result);
		assert!(
			text.contains("staging") && text.contains("dev") && text.contains("prod"),
			"{text}"
		);
		client.cancel().await.expect("shutdown");
	}

	#[tokio::test]
	async fn check_query_applies_only_the_modules_a_target_accepts() {
		let dir = modules();
		let client = connect(&dir).await;
		let errors = |target: Option<&str>| {
			let mut arguments = serde_json::json!({"query": "SELECT 1;"});
			if let Some(target) = target {
				arguments["target"] = serde_json::json!(target);
			}
			let client = &client;
			async move {
				let result = call(client, "check_query", arguments).await;
				assert_ne!(result.is_error, Some(true), "{result:?}");
				let found = result.structured_content.expect("structured content");
				assert_eq!(found["diagnostics"], serde_json::json!([]), "no K findings: {found}");
				found["schema_errors"].as_u64().expect("schema_errors")
			}
		};
		// `billing`'s broken file is the one error; `core` applies with the config's variable.
		assert_eq!(errors(None).await, 1);
		assert_eq!(errors(Some("prod")).await, 1);
		assert_eq!(errors(Some("dev")).await, 0, "dev never sees billing");
		client.cancel().await.expect("shutdown");
	}

	#[tokio::test]
	async fn check_reports_a_broken_schema_file_as_a_failing_report() {
		let dir = written(
			"",
			&[
				("database/schema/a.surql", "DEFINE TABLE t SCHEMAFULL;\n"),
				("database/schema/b.surql", "DEFINE FIELD x.y ON t TYPE string;\n"),
			],
		);
		let client = connect(&dir).await;
		let result = call(&client, "check", serde_json::json!({})).await;
		assert_ne!(result.is_error, Some(true), "a failing report is not a tool error");
		let report = result.structured_content.expect("structured content");
		assert_eq!(report["summary"]["passed"], false, "{report}");
		assert_eq!(report["summary"]["schema_files"], 2);
		let diagnostics = report["diagnostics"].as_array().expect("diagnostics");
		assert_eq!(diagnostics[0]["code"], "K9003", "{report}");
		assert_eq!(diagnostics[0]["path"], "database/schema/b.surql");
		client.cancel().await.expect("shutdown");
	}

	#[tokio::test]
	async fn check_without_a_target_connects_to_nothing() {
		let dir = written(
			"[target.prod]\nhost = \"ws://127.0.0.1:1\"\nns = \"app\"\ndb = \"prod\"\nprimary = true\n",
			&[("database/schema/a.surql", "DEFINE TABLE t SCHEMAFULL;\n")],
		);
		let client = connect(&dir).await;
		let started = Instant::now();
		let result = call(&client, "check", serde_json::json!({})).await;
		assert_ne!(result.is_error, Some(true), "{result:?}");
		assert_eq!(result.structured_content.expect("report")["summary"]["passed"], true);
		assert!(started.elapsed() < Duration::from_secs(20), "waited on a connection");
		client.cancel().await.expect("shutdown");
	}

	#[tokio::test]
	async fn check_reads_the_variables_of_the_project_it_was_asked_about() {
		// Cargo runs this from the crate directory, which has no surrealkit.toml: a check that
		// read the working directory's would find `PREFIX` unset (K9002) and fail.
		let dir = modules();
		let client = connect(&dir).await;
		let result = call(&client, "check", serde_json::json!({"schema": ["core"]})).await;
		let report = result.structured_content.expect("report");
		assert_eq!(report["summary"]["passed"], true, "{report}");
		assert_eq!(report["diagnostics"], serde_json::json!([]));
		client.cancel().await.expect("shutdown");
	}
}
