//! Severity policy and in-source suppressions.
//!
//! Both follow the engine contract, so that the engine's own checker is a drop-in behind the
//! same configuration:
//!
//! - [`Policy`] resolves the severity a finding is reported with, from `[analyze.lints]`,
//!   `warnings_as_errors` and the code's own default.
//! - [`Suppressions`] reads `-- surreal: allow(E1002) reason="..."` comments, which silence
//!   one code on one line.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;

use anyhow::{Result, bail};

use super::config::AnalyzeConfig;
use super::finding::{Code, Finding, L7013, LineIndex, Note, NoteKind, Severity};

/// What a code does when no more specific rule names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
	/// The finding is dropped.
	Allow,
	/// The finding is a warning.
	Warn,
	/// The finding is an error.
	Deny,
}

impl Level {
	fn parse(key: &str, value: &str) -> Result<Self> {
		match value {
			"allow" => Ok(Self::Allow),
			"warn" => Ok(Self::Warn),
			"deny" => Ok(Self::Deny),
			other => bail!(
				"[analyze.lints] {key} = {other:?}: a level is `allow`, `warn` or `deny` (lowercase)"
			),
		}
	}
}

/// The prefix of a suppression comment, after its `--`, `//` or `#`.
pub const SUPPRESSION_PREFIX: &str = "surreal:";
/// The prefix the analyzer's suppressions use, accepted as the same.
pub const ANALYZER_SUPPRESSION_PREFIX: &str = "surrealql-analyzer:";

/// The accepted forms of a lint key, for the messages that reject one.
const KEY_FORMS: &str = "a code such as E2001 or L7001, a bare number such as 2001, or a family \
	wildcard: 7xxx, 7XXX or 7*";

/// The number a code spelling names: `E2001`, `L7001`, `S0001`, `K9003` or the bare `2001`. The
/// letter must be one of the catalog's prefixes; whether the code exists is the engine's call.
fn parse_code(text: &str) -> Option<u16> {
	let digits = match text.chars().next()? {
		'E' | 'L' | 'S' | 'K' => &text[1..],
		_ => text,
	};
	if digits.len() != 4 || !digits.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	digits.parse().ok()
}

/// The family a wildcard names: `7xxx`, `7XXX` or `7*`.
fn parse_family(text: &str) -> Option<u16> {
	let digit = text.chars().next().filter(char::is_ascii_digit)?;
	match &text[1..] {
		"xxx" | "XXX" | "*" => digit.to_digit(10).and_then(|d| u16::try_from(d).ok()),
		_ => None,
	}
}

/// The severity policy: `[analyze]` parsed once.
#[derive(Debug, Clone, Default)]
pub struct Policy {
	exact: BTreeMap<u16, Level>,
	families: BTreeMap<u16, Level>,
	warnings_as_errors: bool,
	require_suppression_reason: bool,
}

impl Policy {
	/// Parse `config`. A lint key that is not a code or a family wildcard, a level that is not
	/// `allow`, `warn` or `deny`, and two keys that name the same code with different levels
	/// are configuration mistakes, not findings.
	pub fn from_config(config: &AnalyzeConfig) -> Result<Self> {
		let mut exact: BTreeMap<u16, (Level, &str)> = BTreeMap::new();
		let mut families: BTreeMap<u16, (Level, &str)> = BTreeMap::new();
		for (key, value) in &config.lints {
			let level = Level::parse(key, value)?;
			let (table, name) = if let Some(number) = parse_code(key) {
				(&mut exact, number)
			} else if let Some(family) = parse_family(key) {
				(&mut families, family)
			} else {
				bail!("[analyze.lints] key {key:?} is not understood: use {KEY_FORMS}");
			};
			if let Some((earlier_level, earlier)) = table.insert(name, (level, key.as_str()))
				&& earlier_level != level
			{
				bail!(
					"[analyze.lints] {earlier:?} and {key:?} name the same code with different \
					 levels; keep one"
				);
			}
		}
		Ok(Self {
			exact: exact.into_iter().map(|(number, (level, _))| (number, level)).collect(),
			families: families.into_iter().map(|(family, (level, _))| (family, level)).collect(),
			warnings_as_errors: config.warnings_as_errors,
			require_suppression_reason: config.require_suppression_reason,
		})
	}

	/// Whether a suppression without a `reason="..."` is a finding.
	pub fn requires_reason(&self) -> bool {
		self.require_suppression_reason
	}

	/// The severity `code` is reported with, or `None` when policy silences it.
	///
	/// First match wins: an entry for the exact code, then one for its family, then the code's
	/// own `default_level`. An entry's `warn` is a warning, and `deny` an error, whatever
	/// `warnings_as_errors` says; only the default level is raised by it. A code whose
	/// `intrinsic` severity is a hint stays a hint where it would be a warning.
	pub fn resolve(
		&self,
		code: Code,
		intrinsic: Severity,
		default_level: Level,
	) -> Option<Severity> {
		let named = self.exact.get(&code.number()).or_else(|| self.families.get(&code.family()));
		let (level, raise) = match named {
			Some(level) => (*level, false),
			None => (default_level, self.warnings_as_errors),
		};
		match level {
			Level::Allow => None,
			Level::Deny => Some(Severity::Error),
			Level::Warn if intrinsic == Severity::Hint => Some(Severity::Hint),
			Level::Warn if raise => Some(Severity::Error),
			Level::Warn => Some(Severity::Warning),
		}
	}
}

/// One valid `allow(...)` directive.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Directive {
	number: u16,
	/// The line whose findings it silences.
	line: usize,
}

/// The suppression directives of one file.
#[derive(Debug, Clone, Default)]
pub struct Suppressions {
	directives: Vec<Directive>,
}

impl Suppressions {
	/// The directives in `text`, and the `L7013` findings for the ones that are malformed or,
	/// when `policy` requires a reason, have none. A file that does not scan has no
	/// suppressions (it already carries its own finding).
	///
	/// Comments come from the scanner, so a `--` inside a string is not one, and block comments
	/// are not read. A directive on a line of its own silences matching findings that start on
	/// the next line; one that trails code silences those that start on its own line.
	pub fn read(path: &Path, text: &str, policy: &Policy) -> (Self, Vec<Finding>) {
		let Ok(comments) = crate::surql_scan::scan_comments(text) else {
			return (Self::default(), Vec::new());
		};
		let index = LineIndex::new(text);
		let mut directives = Vec::new();
		let mut findings = Vec::new();
		for range in comments {
			let Some(parsed) = parse_directive(&text[range.clone()]) else {
				continue;
			};
			let span = range.start..range.start + text[range.clone()].trim_end().len();
			let (line, _) = index.position(range.start);
			let own_line = text[index.line_start(line)..range.start].trim().is_empty();
			match parsed {
				Err(why) => findings.push(directive_finding(
					path,
					span,
					format!("malformed suppression directive: {why}"),
					"write `-- surreal: allow(E1002) reason=\"...\"`: one code, and a reason in \
					 double quotes",
				)),
				Ok((number, reason)) => {
					if policy.requires_reason() && reason.is_none() {
						findings.push(directive_finding(
							path,
							span,
							format!(
								"the suppression of {number:04} has no reason, and \
								 `require_suppression_reason` is set"
							),
							"add `reason=\"...\"` after the code",
						));
					}
					directives.push(Directive {
						number,
						line: if own_line {
							line + 1
						} else {
							line
						},
					});
				}
			}
		}
		(
			Self {
				directives,
			},
			findings,
		)
	}

	/// Whether a directive silences `finding`, which starts at `line` of the file.
	pub fn suppresses(&self, finding: &Finding, line: usize) -> bool {
		self.directives
			.iter()
			.any(|directive| directive.number == finding.code.number() && directive.line == line)
	}
}

fn directive_finding(path: &Path, span: Range<usize>, message: String, help: &str) -> Finding {
	Finding::new(L7013, Severity::Warning, Level::Warn, message, path.to_path_buf(), span)
		.with_note(Note {
			kind: NoteKind::Help,
			message: help.to_string(),
			span: None,
			replacement: None,
		})
}

/// `None` when `comment` is not a suppression directive; otherwise the code number and the
/// reason, or why it is malformed.
///
/// A directive is a `--`, `//` or `#` comment whose body starts with `surreal:` or
/// `surrealql-analyzer:` and then the word `allow`. Any other comment, including one that
/// merely starts with the prefix, is an ordinary comment.
fn parse_directive(comment: &str) -> Option<Result<(u16, Option<String>), String>> {
	let body = comment
		.strip_prefix("--")
		.or_else(|| comment.strip_prefix("//"))
		.or_else(|| comment.strip_prefix('#'))?;
	let body = body.trim_start();
	let rest = body
		.strip_prefix(ANALYZER_SUPPRESSION_PREFIX)
		.or_else(|| body.strip_prefix(SUPPRESSION_PREFIX))?
		.trim_start();
	let rest = rest.strip_prefix("allow")?;
	if rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
		return None;
	}
	Some(parse_allow(rest.trim_end()))
}

/// The text after `allow`: `(<code>) [reason="<text>"]`.
fn parse_allow(rest: &str) -> Result<(u16, Option<String>), String> {
	let rest = rest.trim_start();
	let Some(rest) = rest.strip_prefix('(') else {
		return Err("expected `(` after `allow`".to_string());
	};
	let Some((target, after)) = rest.split_once(')') else {
		return Err("the `)` after the code is missing".to_string());
	};
	let target = target.trim();
	if target.is_empty() {
		return Err("`allow()` names no code".to_string());
	}
	let Some(number) = parse_code(target) else {
		return Err(format!(
			"`{target}` is not one code: name one code such as E1002 (not `*`, a family or a lint \
			 name)"
		));
	};
	let after = after.trim();
	if after.is_empty() {
		return Ok((number, None));
	}
	let Some(reason) = after
		.strip_prefix("reason")
		.map(str::trim_start)
		.and_then(|rest| rest.strip_prefix('='))
		.map(str::trim_start)
		.and_then(|rest| rest.strip_prefix('"'))
	else {
		return Err(format!("`{after}` follows the code; only `reason=\"...\"` may"));
	};
	let mut text = String::new();
	let mut chars = reason.chars();
	loop {
		match chars.next() {
			Some('\\') => match chars.next() {
				Some('"') => text.push('"'),
				Some(other) => {
					text.push('\\');
					text.push(other);
				}
				None => return Err("the reason is never closed".to_string()),
			},
			Some('"') => break,
			Some(c) => text.push(c),
			None => return Err("the reason is never closed".to_string()),
		}
	}
	if !chars.as_str().trim().is_empty() {
		return Err(format!("`{}` follows the reason", chars.as_str().trim()));
	}
	let reason = Some(text).filter(|text| !text.trim().is_empty());
	Ok((number, reason))
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use super::*;
	use crate::check::finding::{K9003, Severity::*};

	fn policy(toml: &str) -> Result<Policy> {
		let raw = format!("[analyze]\n{}", toml.replace("[lints]", "[analyze.lints]"));
		let project = crate::project::ProjectConfig::parse(&raw).expect("parses");
		Policy::from_config(&project.analyze)
	}

	fn finding(code: Code, line_text: &str, span: Range<usize>) -> (Finding, usize) {
		let finding = Finding::kit(code, "m", PathBuf::from("/p/a.surql"), span.clone());
		let (line, _) = LineIndex::new(line_text).position(span.start);
		(finding, line)
	}

	#[test]
	fn the_default_level_decides_when_nothing_names_the_code() {
		let policy = policy("").unwrap();
		let e1002 = Code::from_number(1002);
		assert_eq!(policy.resolve(e1002, Error, Level::Deny), Some(Error));
		assert_eq!(policy.resolve(e1002, Warning, Level::Warn), Some(Warning));
		assert_eq!(policy.resolve(e1002, Warning, Level::Allow), None);
	}

	#[test]
	fn warnings_as_errors_raises_a_default_warn_only() {
		let policy = policy("warnings_as_errors = true\n").unwrap();
		let code = Code::from_number(7001);
		assert_eq!(policy.resolve(code, Warning, Level::Warn), Some(Error));
		assert_eq!(policy.resolve(code, Warning, Level::Allow), None, "allow stays silent");
		assert_eq!(policy.resolve(code, Error, Level::Deny), Some(Error));
	}

	#[test]
	fn an_intrinsic_hint_stays_a_hint_even_with_warnings_as_errors() {
		let code = Code::from_number(7005);
		let strict = policy("warnings_as_errors = true\n").unwrap();
		assert_eq!(strict.resolve(code, Hint, Level::Warn), Some(Hint));
		let named = policy("[lints]\nL7005 = \"warn\"\n").unwrap();
		assert_eq!(named.resolve(code, Hint, Level::Allow), Some(Hint));
		assert_eq!(named.resolve(code, Hint, Level::Warn), Some(Hint));
		// `deny` is an error whatever the intrinsic severity.
		let deny = policy("[lints]\nL7005 = \"deny\"\n").unwrap();
		assert_eq!(deny.resolve(code, Hint, Level::Warn), Some(Error));
	}

	#[test]
	fn a_named_level_is_not_raised_by_warnings_as_errors() {
		let policy = policy("warnings_as_errors = true\n[lints]\nE2001 = \"warn\"\n").unwrap();
		let code = Code::from_number(2001);
		assert_eq!(policy.resolve(code, Error, Level::Deny), Some(Warning));
		let family = policy_with("warnings_as_errors = true\n[lints]\n\"2xxx\" = \"warn\"\n");
		assert_eq!(family.resolve(code, Error, Level::Deny), Some(Warning));
	}

	fn policy_with(toml: &str) -> Policy {
		policy(toml).unwrap()
	}

	#[test]
	fn every_key_form_names_the_number() {
		for key in ["E2001", "2001", "L2001", "S2001"] {
			let policy = policy_with(&format!("[lints]\n{key:?} = \"allow\"\n"));
			assert_eq!(
				policy.resolve(Code::from_number(2001), Error, Level::Deny),
				None,
				"{key} names 2001"
			);
			assert_eq!(
				policy.resolve(Code::from_number(2002), Error, Level::Deny),
				Some(Error),
				"{key} leaves 2002 alone"
			);
		}
		let kit = policy_with("[lints]\nK9003 = \"warn\"\n");
		assert_eq!(kit.resolve(K9003, Error, Level::Deny), Some(Warning));
	}

	#[test]
	fn every_wildcard_spelling_names_the_family() {
		for key in ["7xxx", "7XXX", "7*"] {
			let policy = policy_with(&format!("[lints]\n{key:?} = \"deny\"\n"));
			assert_eq!(policy.resolve(Code::from_number(7001), Warning, Level::Warn), Some(Error));
			assert_eq!(policy.resolve(Code::from_number(7013), Warning, Level::Warn), Some(Error));
			assert_eq!(
				policy.resolve(Code::from_number(2001), Warning, Level::Warn),
				Some(Warning),
				"{key} is family 7 only"
			);
		}
	}

	#[test]
	fn an_exact_entry_beats_its_family() {
		let policy = policy_with("[lints]\n\"7xxx\" = \"deny\"\nL7001 = \"allow\"\n");
		assert_eq!(policy.resolve(Code::from_number(7001), Warning, Level::Warn), None);
		assert_eq!(policy.resolve(Code::from_number(7002), Warning, Level::Warn), Some(Error));
	}

	#[test]
	fn a_family_beats_the_default() {
		let policy = policy_with("[lints]\n\"1xxx\" = \"allow\"\n");
		assert_eq!(policy.resolve(Code::from_number(1002), Error, Level::Deny), None);
	}

	#[test]
	fn a_bad_key_is_a_configuration_error_that_lists_the_forms() {
		for key in
			["E2", "E20010", "X2001", "e2001", "7x", "77xxx", "xxxx", "*", "unused-let", "2xx"]
		{
			let error = policy(&format!("[lints]\n{key:?} = \"warn\"\n")).expect_err(key);
			let message = format!("{error:#}");
			assert!(message.contains(&format!("{key:?}")), "names the key: {message}");
			assert!(message.contains("7xxx"), "lists the forms: {message}");
		}
	}

	#[test]
	fn a_bad_level_is_a_configuration_error() {
		for level in ["Warn", "error", "off", ""] {
			let error = policy(&format!("[lints]\nE1002 = {level:?}\n")).expect_err(level);
			let message = format!("{error:#}");
			assert!(message.contains("`allow`, `warn` or `deny`"), "{message}");
		}
	}

	#[test]
	fn two_spellings_of_one_code_must_agree() {
		let error = policy("[lints]\nE2001 = \"warn\"\n\"2001\" = \"deny\"\n").unwrap_err();
		assert!(format!("{error:#}").contains("same code"), "{error:#}");
		policy("[lints]\nE2001 = \"warn\"\n\"2001\" = \"warn\"\n").expect("agreeing spellings");
	}

	fn read(text: &str, toml: &str) -> (Suppressions, Vec<Finding>) {
		Suppressions::read(Path::new("/p/a.surql"), text, &policy_with(toml))
	}

	fn silenced(suppressions: &Suppressions, text: &str, code: Code, at: usize) -> bool {
		let (finding, line) = finding(code, text, at..at + 1);
		suppressions.suppresses(&finding, line)
	}

	#[test]
	fn an_own_line_directive_covers_the_next_line() {
		let text = "-- surreal: allow(K9003)\nDEFINE X;\nDEFINE Y;\n";
		let (suppressions, findings) = read(text, "");
		assert!(findings.is_empty());
		assert!(silenced(&suppressions, text, K9003, 25), "the next line");
		assert!(!silenced(&suppressions, text, K9003, 35), "not the line after");
		assert!(!silenced(&suppressions, text, K9003, 0), "not its own line");
	}

	#[test]
	fn a_trailing_directive_covers_its_own_line() {
		let text = "DEFINE X; -- surreal: allow(K9003)\nDEFINE Y;\n";
		let (suppressions, _) = read(text, "");
		assert!(silenced(&suppressions, text, K9003, 0));
		assert!(!silenced(&suppressions, text, K9003, 36), "not the next line");
	}

	#[test]
	fn a_directive_covers_only_its_code() {
		let text = "-- surreal: allow(K9003)\nDEFINE X;\n";
		let (suppressions, _) = read(text, "");
		assert!(!silenced(&suppressions, text, Code::kit(9001), 25));
	}

	#[test]
	fn several_directives_each_cover_their_own_code() {
		// Stacking two own-line directives covers only the line below the second.
		let text = "-- surreal: allow(K9001)\n-- surreal: allow(K9003)\nDEFINE X; -- surreal: allow(9002)\n";
		let (suppressions, findings) = read(text, "");
		assert!(findings.is_empty());
		let statement = text.find("DEFINE").unwrap();
		assert!(silenced(&suppressions, text, K9003, statement));
		assert!(!silenced(&suppressions, text, Code::kit(9001), statement));
		assert!(silenced(&suppressions, text, Code::kit(9002), statement), "a bare number");
	}

	#[test]
	fn the_comment_prefixes_and_the_analyzer_spelling_are_accepted() {
		for text in [
			"-- surreal: allow(E1002)\nx",
			"// surreal: allow(E1002)\nx",
			"# surreal: allow(E1002)\nx",
			"--surreal:allow(E1002)\nx",
			"-- surrealql-analyzer: allow(E1002)\nx",
			"  \t-- surreal: allow( E1002 )  \nx",
		] {
			let (suppressions, findings) = read(text, "");
			assert!(findings.is_empty(), "{text:?}: {findings:?}");
			assert!(
				silenced(&suppressions, text, Code::from_number(1002), text.len() - 1),
				"{text:?}"
			);
		}
	}

	#[test]
	fn a_directive_in_a_string_or_a_block_comment_is_not_read() {
		for text in [
			"DEFINE FIELD a ON t DEFAULT '-- surreal: allow(E1002)';\nx",
			"/* surreal: allow(E1002) */\nx",
			"/*\n-- surreal: allow(E1002)\n*/\nx",
		] {
			let (suppressions, findings) = read(text, "");
			assert!(findings.is_empty(), "{text:?}");
			assert!(!silenced(&suppressions, text, Code::from_number(1002), text.len() - 1));
		}
	}

	#[test]
	fn an_ordinary_comment_is_not_a_directive() {
		for text in [
			"-- surreal: this is the surreal docs link\nx",
			"-- surreal: allowance(E1002)\nx",
			"-- allow(E1002)\nx",
			"-- note: surreal: allow(E1002)\nx",
		] {
			let (suppressions, findings) = read(text, "");
			assert!(findings.is_empty(), "{text:?}");
			assert!(!silenced(&suppressions, text, Code::from_number(1002), text.len() - 1));
		}
	}

	#[test]
	fn a_malformed_directive_is_l7013_and_suppresses_nothing() {
		for directive in [
			"allow(*)",
			"allow(7xxx)",
			"allow(7*)",
			"allow(unused_let)",
			"allow()",
			"allow(E1002",
			"allow E1002",
			"allow(E1002, E1003)",
			"allow(E1002) because",
			"allow(E1002) reason=no",
			"allow(E1002) reason=\"open",
			"allow(E1002) reason=\"a\" extra",
			"allow(E100)",
			"allow(X1002)",
		] {
			let text = format!("-- surreal: {directive}\nx");
			let (suppressions, findings) = read(&text, "");
			assert_eq!(findings.len(), 1, "{directive}: {findings:?}");
			assert_eq!(findings[0].code, L7013);
			assert_eq!(findings[0].severity, Warning);
			assert_eq!(findings[0].default_level(), Level::Warn);
			assert_eq!(findings[0].span, 0..text.find('\n').unwrap(), "the whole comment");
			assert!(findings[0].message.starts_with("malformed suppression directive"));
			assert!(
				!silenced(&suppressions, &text, Code::from_number(1002), text.len() - 1),
				"{directive}"
			);
		}
	}

	#[test]
	fn a_reason_is_parsed_and_may_hold_parentheses_and_escaped_quotes() {
		for text in [
			"-- surreal: allow(E1002) reason=\"legacy (to go) column\"\nx",
			"-- surreal: allow(E1002) reason = \"a \\\"quoted\\\" word\"\nx",
			"x -- surreal: allow(E1002) reason=\"t\"",
		] {
			let (suppressions, findings) = read(text, "require_suppression_reason = true\n");
			assert!(findings.is_empty(), "{text:?}: {findings:?}");
			assert!(
				silenced(&suppressions, text, Code::from_number(1002), text.len() - 1),
				"{text:?}"
			);
		}
		assert_eq!(
			parse_allow("(E1002) reason=\"a \\\"b\\\"\""),
			Ok((1002, Some("a \"b\"".to_string())))
		);
	}

	#[test]
	fn a_missing_reason_is_l7013_only_when_one_is_required_and_still_suppresses() {
		let text = "-- surreal: allow(E1002)\nx";
		let (_, findings) = read(text, "");
		assert!(findings.is_empty());
		for text in ["-- surreal: allow(E1002)\nx", "-- surreal: allow(E1002) reason=\"  \"\nx"] {
			let (suppressions, findings) = read(text, "require_suppression_reason = true\n");
			assert_eq!(findings.len(), 1, "{text:?}");
			assert_eq!(findings[0].code, L7013);
			assert!(findings[0].message.contains("no reason"));
			assert!(
				silenced(&suppressions, text, Code::from_number(1002), text.len() - 1),
				"the directive still suppresses"
			);
		}
	}

	#[test]
	fn a_file_that_does_not_scan_has_no_suppressions() {
		let text = "-- surreal: allow(K9003)\nDEFINE FIELD a ON t DEFAULT 'open;\n";
		let (suppressions, findings) = read(text, "");
		assert!(findings.is_empty());
		assert!(!silenced(&suppressions, text, K9003, 25));
	}

	#[test]
	fn a_finding_is_located_by_the_line_it_starts_on() {
		// A multi-line statement: the directive covers the line the finding starts on.
		let text = "-- surreal: allow(K9003)\nDEFINE FIELD a\n  ON t;\n";
		let (suppressions, _) = read(text, "");
		let start = text.find("DEFINE").unwrap();
		let finding = Finding::kit(K9003, "m", PathBuf::from("/p/a.surql"), start..text.len());
		let line = LineIndex::new(text).position(finding.span.start).0;
		assert!(suppressions.suppresses(&finding, line));
	}
}
