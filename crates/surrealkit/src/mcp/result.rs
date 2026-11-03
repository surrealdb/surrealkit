//! The tool-result envelope.
//!
//! Every tool returns the same shape: a human-readable summary, the structured
//! report as JSON, and -- because progress is batched rather than streamed over
//! MCP -- whatever the call logged along the way.

use std::sync::Arc;

use rmcp::model::{CallToolResult, Content};
use serde::Serialize;

use crate::progress::{ProgressSink, capture};

/// Run `f` with progress capture active, and package the outcome as a tool result.
///
/// The captured lines are the same ones the CLI would have printed. They cannot
/// be streamed: a `CallToolResult` is a single message, and on a stdio transport
/// the alternative -- MCP logging notifications -- would interleave with the
/// JSON-RPC response. So they are collected and returned alongside the report.
pub async fn run_captured<T, F, Fut>(summary: impl FnOnce(&T) -> String, f: F) -> CallToolResult
where
	T: Serialize,
	F: FnOnce() -> Fut,
	Fut: Future<Output = anyhow::Result<T>>,
{
	let sink = ProgressSink::new();
	let outcome = capture(Arc::clone(&sink), f()).await;
	let progress = sink.render();

	match outcome {
		Ok(report) => {
			let mut content = vec![Content::text(summary(&report))];
			if !progress.is_empty() {
				content.push(Content::text(format!("Progress:\n{progress}")));
			}
			match serde_json::to_value(&report) {
				Ok(value) => {
					// Also emit the JSON as text: hosts that predate
					// `structuredContent` would otherwise see only the summary.
					if let Ok(pretty) = serde_json::to_string_pretty(&value) {
						content.push(Content::text(pretty));
					}
					let mut result = CallToolResult::success(content);
					result.structured_content = Some(value);
					result
				}
				Err(err) => {
					content.push(Content::text(format!(
						"(the result could not be serialized: {err})"
					)));
					CallToolResult::success(content)
				}
			}
		}
		Err(err) => {
			let mut text = crate::mcp::error::render(&err);
			if !progress.is_empty() {
				text.push_str(&format!("\n\nProgress before the failure:\n{progress}"));
			}
			CallToolResult::error(vec![Content::text(text)])
		}
	}
}
