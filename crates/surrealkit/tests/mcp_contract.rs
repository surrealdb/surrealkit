//! Contract tests for the MCP surface.
//!
//! These assert the *invariants* rather than the exact catalog, so they keep
//! holding as tools are added. The one that matters most is the annotation
//! contract: a host's approval prompt is driven entirely by `destructiveHint`
//! and `readOnlyHint`, so a tool that can drop a table while advertising itself
//! as read-only would silently bypass the only real safety boundary there is.

use std::collections::BTreeSet;

use surrealkit::config::DbOverrides;
use surrealkit::mcp::{ServerConfig, SurrealKitMcp};

fn server() -> SurrealKitMcp {
	let root = std::env::current_dir().expect("cwd");
	let config = ServerConfig::new(root, DbOverrides::default(), Vec::new()).expect("config");
	SurrealKitMcp::new(config)
}

fn tools() -> Vec<rmcp::model::Tool> {
	server().tool_list()
}

/// Tools that can remove data or overwrite files. Kept as an explicit list so
/// that adding a destructive tool without gating it fails here rather than in
/// someone's database.
const DESTRUCTIVE: &[&str] =
	&["apply", "rollout_baseline", "rollout_complete", "rollout_rollback", "sync", "test"];

#[test]
fn every_tool_declares_all_three_behaviour_hints() {
	for tool in tools() {
		let annotations =
			tool.annotations.as_ref().unwrap_or_else(|| panic!("{} has no annotations", tool.name));
		assert!(
			annotations.read_only_hint.is_some(),
			"{} does not declare readOnlyHint; the host cannot tell whether to prompt",
			tool.name
		);
		assert!(
			annotations.destructive_hint.is_some(),
			"{} does not declare destructiveHint",
			tool.name
		);
		assert!(
			annotations.idempotent_hint.is_some(),
			"{} does not declare idempotentHint",
			tool.name
		);
	}
}

#[test]
fn destructive_tools_are_annotated_destructive_and_not_read_only() {
	let expected: BTreeSet<&str> = DESTRUCTIVE.iter().copied().collect();
	for tool in tools() {
		let annotations = tool.annotations.as_ref().expect("annotations");
		let name: &str = &tool.name;
		if expected.contains(name) {
			assert_eq!(
				annotations.destructive_hint,
				Some(true),
				"{name} can remove data but is not annotated destructive"
			);
			assert_eq!(
				annotations.read_only_hint,
				Some(false),
				"{name} can remove data but claims to be read-only"
			);
		}
	}
}

#[test]
fn read_only_tools_never_claim_to_be_destructive() {
	for tool in tools() {
		let annotations = tool.annotations.as_ref().expect("annotations");
		if annotations.read_only_hint == Some(true) {
			assert_eq!(
				annotations.destructive_hint,
				Some(false),
				"{} is both read-only and destructive, which cannot be true",
				tool.name
			);
		}
	}
}

#[test]
fn destructive_tools_take_a_confirm_flag() {
	// The gate is not an authorization boundary -- the caller fills `confirm` in
	// itself -- but a destructive tool that cannot be gated at all has no way to
	// surface its blast radius before running.
	for tool in tools() {
		let name: &str = &tool.name;
		if !DESTRUCTIVE.contains(&name) {
			continue;
		}
		// `test` creates and drops its own isolated namespaces rather than
		// touching the caller's data, so it is deliberately not gated.
		if name == "test" {
			continue;
		}
		let schema = serde_json::to_value(&tool.input_schema).expect("schema");
		let properties = schema.get("properties").expect("properties");
		assert!(
			properties.get("confirm").is_some(),
			"{name} is destructive but has no `confirm` parameter"
		);
	}
}

#[test]
fn every_tool_has_a_description_that_says_more_than_its_name() {
	for tool in tools() {
		let description =
			tool.description.as_ref().unwrap_or_else(|| panic!("{} has no description", tool.name));
		assert!(
			description.len() > 40,
			"{}'s description is too thin to choose from: {description:?}",
			tool.name
		);
	}
}

#[test]
fn tool_names_are_unique_and_host_safe() {
	let mut seen = BTreeSet::new();
	for tool in tools() {
		assert!(seen.insert(tool.name.clone()), "duplicate tool name {}", tool.name);
		assert!(
			tool.name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
			"{} is not a safe tool name: many hosts restrict these to [A-Za-z0-9_-]",
			tool.name
		);
		assert!(tool.name.len() <= 64, "{} is too long for some hosts", tool.name);
	}
}

#[test]
fn the_catalog_covers_every_cli_capability() {
	// Parity with the CLI is the whole point: a capability reachable from the
	// terminal but not from an agent is a gap, not a design choice.
	let names: BTreeSet<String> = tools().into_iter().map(|t| t.name.to_string()).collect();
	for expected in [
		"init",
		"setup",
		"sync",
		"seed",
		"apply",
		"typegen",
		"test",
		"project_info",
		"rollout_baseline",
		"rollout_plan",
		"rollout_lint",
		"rollout_start",
		"rollout_complete",
		"rollout_rollback",
		"rollout_repair",
		"rollout_status",
	] {
		assert!(names.contains(expected), "missing tool {expected}; have {names:?}");
	}
}
