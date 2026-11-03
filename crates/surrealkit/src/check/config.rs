//! The `[analyze]` section of `surrealkit.toml`.
//!
//! Plain serde, parsed whatever the cargo features: a build without `analyze` still accepts the
//! section, so one config file serves every build. What the keys mean is in
//! [`Policy`](super::policy::Policy) and [`Ignore`](super::discover::Ignore).

use std::collections::BTreeMap;

use serde::Deserialize;

/// The `[analyze]` section of `surrealkit.toml`.
///
/// Every key is optional; an absent section checks the project with each code at its default
/// level.
///
/// ```toml
/// [analyze]
/// ignore = ["dist", "src/generated"]
/// warnings_as_errors = false
/// require_suppression_reason = true
///
/// [analyze.lints]
/// "7xxx" = "warn"
/// E1002 = "allow"
/// K9003 = "deny"
/// ```
///
/// There is deliberately no `strict` or `surrealdb_version` key (a check runs against the
/// engine SurrealKit embeds, so there is no version to gate on), and no `out` (that belongs to
/// `generate`). `deny_unknown_fields` rejects each, so adding one later is additive.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyzeConfig {
	/// Paths excluded from the query files a check reads, beyond the always-excluded `target`,
	/// `node_modules` and `.git`. An entry without a `/` is a name, matching any path component
	/// under the project root; one with a `/` is a path relative to the project root, matching
	/// it and everything under it. A trailing `/**` or `/` is dropped. There are no globs: an
	/// entry containing `*`, `?` or `[` is a configuration error.
	#[serde(default)]
	pub ignore: Vec<String>,
	/// Report every warning as an error, except for a code with a level of its own in `lints`.
	#[serde(default)]
	pub warnings_as_errors: bool,
	/// Require `reason="..."` on every `-- surreal: allow(...)` suppression; one without it is
	/// the warning `L7013`.
	#[serde(default)]
	pub require_suppression_reason: bool,
	/// Per-code levels: `allow`, `warn` or `deny`. The key is a code (`E1002`, or the bare
	/// number `1002`) or a family wildcard (`7xxx`, `7XXX`, `7*`).
	#[serde(default)]
	pub lints: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
	use crate::project::ProjectConfig;

	#[test]
	fn the_section_parses_every_key() {
		let project = ProjectConfig::parse(
			"[analyze]\nignore = [\"dist\"]\nwarnings_as_errors = true\n\
			 require_suppression_reason = true\n[analyze.lints]\n\"7xxx\" = \"warn\"\n",
		)
		.expect("parses");
		assert_eq!(project.analyze.ignore, vec!["dist"]);
		assert!(project.analyze.warnings_as_errors);
		assert!(project.analyze.require_suppression_reason);
		assert_eq!(project.analyze.lints["7xxx"], "warn");
	}

	#[test]
	fn an_absent_section_is_the_default() {
		let project = ProjectConfig::parse("").expect("parses");
		assert!(project.analyze.ignore.is_empty());
		assert!(!project.analyze.warnings_as_errors);
		assert!(project.analyze.lints.is_empty());
	}

	#[test]
	fn keys_the_contract_leaves_out_are_rejected() {
		// `strict` does nothing, a check has no version to gate on, and `out` belongs to
		// `generate`: a key that silently does nothing is worse than one that does not exist.
		for key in ["strict = true", "surrealdb_version = \"4.0\"", "out = \"x.ts\""] {
			let raw = format!("[analyze]\n{key}\n");
			assert!(ProjectConfig::parse(&raw).is_err(), "{key} must be rejected");
		}
		assert!(ProjectConfig::parse("[analyze]\nstrictness = true\n").is_err());
	}
}
