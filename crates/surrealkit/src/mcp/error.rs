//! Turning SurrealKit's errors into MCP tool results.
//!
//! MCP has two failure channels and the split is not arbitrary: a JSON-RPC error
//! is for the *client* (a malformed request it should never have sent), while
//! `isError: true` content is for the *model* (something it can read and act on).
//! Almost every SurrealKit failure is the second kind.
//!
//! The messages are passed through **verbatim**. This codebase writes genuinely
//! good errors -- the empty-prune refusal names the flag that overrides it, the
//! stuck-lock error hands you the exact `DELETE` statement and the TTL, the
//! orphaned-`DATABASE_*` error explains why it refuses rather than ignores.
//! Summarising those for a model would be a strict downgrade.

use rmcp::model::{CallToolResult, Content};

/// Render an `anyhow` error chain the way the CLI prints it.
///
/// `{err:#}` is the alternate form that joins the whole `.context()` chain with
/// `: ` -- the same rendering `main.rs` uses, so a tool result and a terminal
/// report the same thing.
pub fn render(err: &anyhow::Error) -> String {
	format!("{err:#}")
}

/// A failed tool call, reported to the model rather than to the client.
pub fn tool_error(err: &anyhow::Error) -> CallToolResult {
	CallToolResult::error(vec![Content::text(render(err))])
}

/// A refusal to run a destructive operation without `confirm: true`.
///
/// `confirm` is **not** an authorization boundary -- it is a field the caller
/// fills in, so it stops neither a confused model nor a malicious one. What it
/// does is make the blast radius impossible to miss at the moment of choosing,
/// and give the host's approval UI something concrete to show. The real
/// boundaries are that UI, the transport (stdio inherits the user's identity),
/// and the database user the server signs in as.
///
/// The message teaches in a fixed order: destination, enumerated effect,
/// reversibility, the exact call to make, and the safe alternative.
pub fn confirmation_required(
	tool: &str,
	destination: &str,
	effect: &str,
	reversibility: &str,
	alternative: Option<&str>,
) -> CallToolResult {
	let mut text = format!(
		"refusing to run `{tool}` without confirmation.\n\n\
		 Destination: {destination}\n\n\
		 {effect}\n\n\
		 {reversibility}\n\n\
		 To proceed, call `{tool}` again with:\n    confirm: true\n"
	);
	if let Some(alternative) = alternative {
		text.push_str(&format!("\nSafer alternative:\n    {alternative}\n"));
	}
	CallToolResult::error(vec![Content::text(text)])
}
