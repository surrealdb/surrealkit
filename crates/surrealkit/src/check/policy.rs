//! Severity policy and in-source suppressions.
//!
//! Both follow the engine contract, so that the engine's own checker is a drop-in behind the
//! same configuration:
//!
//! - [`Policy`] resolves the severity a finding is reported with, from `[analyze.lints]`,
//!   `warnings_as_errors` and the code's own default.
//! - [`Suppressions`] reads `-- surreal: allow(E1002, K9201) reason="..."` comments, which
//!   silence the rendered codes they list on one line. SurrealKit owns the `K` and 9-family
//!   targets and the engine owns the rest ([`Owner`]).

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;

use anyhow::{Result, bail};

use super::config::AnalyzeConfig;
use super::finding::{Code, Finding, L7013, LineIndex, Note, NoteKind, Severity, describe};

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
const KEY_FORMS: &str = "a code such as E2001, L7001 or C10001, a bare number such as 2001 or \
	10001, or a family wildcard: 7xxx, 7XXX or 7* (10xxx and 10* for a five-digit family)";

/// The number a code spelling names: `E2001`, `L7001`, `S0001`, `K9003`, `C10001` or the bare
/// `2001`. The letter must be one of the catalog's prefixes (`S`, `E`, `L`, `K`, `C`, `A`, `M`,
/// `D`, `P`) and the number has four or five digits; whether the code exists is the engine's
/// call.
fn parse_code(text: &str) -> Option<u16> {
	let digits = match text.chars().next()? {
		'S' | 'E' | 'L' | 'K' | 'C' | 'A' | 'M' | 'D' | 'P' => &text[1..],
		_ => text,
	};
	if !matches!(digits.len(), 4 | 5) || !digits.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	digits.parse().ok()
}

/// The family a wildcard names: `N` of one or two digits, then `xxx`, `XXX` or `*`, as in `7xxx`,
/// `7*` and `10xxx`. The family is the number's thousands.
fn parse_family(text: &str) -> Option<u16> {
	let digits = text
		.strip_suffix("xxx")
		.or_else(|| text.strip_suffix("XXX"))
		.or_else(|| text.strip_suffix('*'))?;
	if !matches!(digits.len(), 1 | 2) || !digits.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	digits.parse().ok()
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

/// Who reads a suppression target.
///
/// SurrealKit reads the targets it owns, and the engine's checker reads the rest. The engine
/// skips every kit-owned target silently: it neither suppresses with it nor reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
	/// A target whose first character is `K` or `k`, or one in the 9 family: a bare number of
	/// four digits starting with `9`, `9xxx`, `9XXX` or `9*`.
	Kit,
	/// Every other target.
	Engine,
}

/// One target of an `allow(...)` list, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
	/// The target's text, trimmed.
	pub text: String,
	/// Who reads it. Known for a bad target too.
	pub owner: Owner,
	/// The rendered code it names, or why it is not one. A kit target is `K` and four digits
	/// that `finding.rs` defines; an engine target is a letter from `S`, `E`, `L`, `C`, `A`,
	/// `M`, `D` or `P` and four or five digits. The kit letter may be lowercase, and the code
	/// then has the capital.
	pub code: Result<Code, String>,
}

impl Target {
	/// Classify and validate one target, `text` trimmed of whitespace.
	fn parse(text: &str) -> Self {
		let owner = if owned_by_kit(text) {
			Owner::Kit
		} else {
			Owner::Engine
		};
		let code = match owner {
			Owner::Kit => kit_code(text),
			Owner::Engine => engine_code(text),
		};
		Self {
			text: text.to_string(),
			owner,
			code,
		}
	}
}

/// Whether SurrealKit owns the target `text`: it starts with `K` or `k`, or it is in the 9
/// family (`9201`, `9xxx`, `9XXX`, `9*`).
fn owned_by_kit(text: &str) -> bool {
	text.starts_with(['K', 'k'])
		|| matches!(text, "9xxx" | "9XXX" | "9*")
		|| (text.len() == 4 && text.starts_with('9') && text.bytes().all(|b| b.is_ascii_digit()))
}

/// The kit code a kit-owned target names: `K` or `k`, four digits, and a code `finding.rs`
/// defines.
fn kit_code(text: &str) -> Result<Code, String> {
	let digits = text.strip_prefix(['K', 'k']).unwrap_or("");
	if digits.len() == 4
		&& digits.bytes().all(|b| b.is_ascii_digit())
		&& let Ok(number) = digits.parse()
	{
		let code = Code::kit(number);
		return describe(code)
			.map(|_| code)
			.ok_or_else(|| format!("`{text}` is not a SurrealKit code"));
	}
	Err(format!(
		"`{text}` is a SurrealKit target, and SurrealKit takes one rendered code such as K9201: \
		 no bare numbers and no wildcards"
	))
}

/// The engine code an engine-owned target names: a letter from `S`, `E`, `L`, `C`, `A`, `M`,
/// `D` or `P`, then four or five digits. The letter is as written and is not checked against
/// the number; whether the code exists is the engine's call.
fn engine_code(text: &str) -> Result<Code, String> {
	let mut chars = text.chars();
	if let Some(letter @ ('S' | 'E' | 'L' | 'C' | 'A' | 'M' | 'D' | 'P')) = chars.next() {
		let digits = chars.as_str();
		if matches!(digits.len(), 4 | 5)
			&& digits.bytes().all(|b| b.is_ascii_digit())
			&& let Ok(number) = digits.parse()
		{
			return Ok(Code::new(letter, number));
		}
	}
	Err(format!(
		"`{text}` is not a rendered engine code: write a letter and its four or five digits, such \
		 as E1002 or C10001, with no bare numbers, `*`, family wildcards or lint names"
	))
}

/// One `allow(...)` comment: its targets, reason and scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Directive {
	/// The targets in the order written, good and bad. For a directive with a [`fault`] they
	/// are what could be read from the list: the targets between the `(` and the first `)`
	/// (or the end of the comment), split at commas, with empty ones left out.
	///
	/// [`fault`]: Self::fault
	pub targets: Vec<Target>,
	/// The text of `reason="..."`, when there is one that is not blank.
	pub reason: Option<String>,
	/// Why the directive as a whole is malformed: bad syntax, an unclosed reason, text after the
	/// reason, or an empty list. A directive with a fault suppresses nothing. A bad target or a
	/// missing reason is not a fault of this kind.
	pub fault: Option<String>,
	/// Whether policy refuses the directive: it has no reason and `require_suppression_reason` is
	/// set. The engine treats such a directive as rejected, so it suppresses nothing, and
	/// [`Suppressions`] reports it as one `L7013`. It is set when the directive is read.
	pub rejected: bool,
	/// The line whose findings it silences.
	pub line: usize,
}

impl Directive {
	/// Whether any target is the engine's, valid or not. The engine reads such a directive, so
	/// it is the engine that reports the directive's faults.
	pub fn has_engine_target(&self) -> bool {
		self.targets.iter().any(|target| target.owner == Owner::Engine)
	}

	/// Whether the directive silences `code`, a finding's code on the line it covers. A
	/// directive with a fault, or one that policy rejects for want of a reason, silences nothing.
	fn silences(&self, code: Code) -> bool {
		self.fault.is_none()
			&& !self.rejected
			&& self.targets.iter().any(|target| target.code.as_ref().is_ok_and(|c| *c == code))
	}
}

/// The suppression directives of one file.
#[derive(Debug, Clone, Default)]
pub struct Suppressions {
	directives: Vec<Directive>,
}

impl Suppressions {
	/// The directives in the SurrealQL `text`, and the `L7013` findings for their faults: one
	/// per directive that is malformed as a whole, one per bad target in a directive that is not,
	/// and one per directive that has no reason when `policy` requires one, which is then
	/// [rejected](Directive::rejected) and suppresses nothing. A file that does not
	/// scan has no suppressions (it already carries its own finding).
	///
	/// Comments come from the SurrealQL scanner, so a `--` inside a string is not one, and block
	/// comments are not read. A directive on a line of its own silences matching findings that start on
	/// the next line; one that trails code silences those that start on its own line.
	pub fn read(path: &Path, text: &str, policy: &Policy) -> (Self, Vec<Finding>) {
		let Ok(comments) = crate::surql_scan::scan_comments(text) else {
			return (Self::default(), Vec::new());
		};
		Self::from_comments(path, text, &comments, policy)
	}

	/// The directives in the `comments` of `text`, byte ranges of it that another lexer found,
	/// with the same findings as [`read`](Self::read). A host-language file passes the comment
	/// ranges its grammar yields; only a `//` line comment can be a directive there, and a `/* */`
	/// block is never read.
	pub fn from_comments(
		path: &Path,
		text: &str,
		comments: &[Range<usize>],
		policy: &Policy,
	) -> (Self, Vec<Finding>) {
		let index = LineIndex::new(text);
		let mut directives = Vec::new();
		let mut findings = Vec::new();
		for range in comments.iter().cloned() {
			let (line, _) = index.position(range.start);
			let own_line = text[index.line_start(line)..range.start].trim().is_empty();
			let covers = if own_line {
				line + 1
			} else {
				line
			};
			let Some(mut directive) = parse_directive(&text[range.clone()], covers) else {
				continue;
			};
			directive.rejected = policy.requires_reason() && directive.reason.is_none();
			let span = range.start..range.start + text[range.clone()].trim_end().len();
			// A fault of the directive as a whole is one finding and hides the targets', which
			// were not read as a list.
			if let Some(why) = &directive.fault {
				findings.push(directive_finding(
					path,
					span,
					format!("malformed suppression directive: {why}"),
					DIRECTIVE_HELP,
				));
			} else {
				for target in &directive.targets {
					if let Err(why) = &target.code {
						findings.push(directive_finding(
							path,
							span.clone(),
							format!("malformed suppression directive: {why}"),
							DIRECTIVE_HELP,
						));
					}
				}
				if directive.rejected {
					findings.push(directive_finding(
						path,
						span,
						"the suppression has no reason, and `require_suppression_reason` is set, \
						 so it suppresses nothing"
							.to_string(),
						"add `reason=\"...\"` after the list for it to take effect",
					));
				}
			}
			directives.push(directive);
		}
		(
			Self {
				directives,
			},
			findings,
		)
	}

	/// Every directive in the file, in order, including those with a fault.
	pub fn directives(&self) -> &[Directive] {
		&self.directives
	}

	/// Whether a directive silences `finding`, which starts at `line` of the file. A directive
	/// names rendered codes, and it silences only a finding with one of them.
	pub fn suppresses(&self, finding: &Finding, line: usize) -> bool {
		self.directives
			.iter()
			.any(|directive| directive.line == line && directive.silences(finding.code))
	}
}

/// The help under every `L7013` for a directive.
const DIRECTIVE_HELP: &str = "write `-- surreal: allow(E1002, K9201) reason=\"...\"`: rendered \
	codes, no wildcards, and a reason in double quotes";

fn directive_finding(path: &Path, span: Range<usize>, message: String, help: &str) -> Finding {
	Finding::new(L7013, Severity::Warning, Level::Warn, message, path.to_path_buf(), span)
		.with_note(Note {
			kind: NoteKind::Help,
			message: help.to_string(),
			span: None,
			replacement: None,
		})
}

/// `None` when `comment` is not a suppression directive; otherwise the directive, which covers
/// `line`.
///
/// A directive is a `--`, `//` or `#` comment whose body starts with `surreal:` or
/// `surrealql-analyzer:`, then `allow`, optional whitespace and `(`. Any other comment,
/// including one that merely starts with the prefix, or has `allowx` or `allow E1002`, is an
/// ordinary comment.
fn parse_directive(comment: &str, line: usize) -> Option<Directive> {
	let body = comment
		.strip_prefix("--")
		.or_else(|| comment.strip_prefix("//"))
		.or_else(|| comment.strip_prefix('#'))?;
	let body = body.trim_start();
	let rest = body
		.strip_prefix(ANALYZER_SUPPRESSION_PREFIX)
		.or_else(|| body.strip_prefix(SUPPRESSION_PREFIX))?
		.trim_start();
	let rest = rest.strip_prefix("allow")?.trim_start().strip_prefix('(')?;
	let rest = rest.trim_end();
	let (list, after) = match rest.split_once(')') {
		Some((list, after)) => (list, Some(after)),
		None => (rest, None),
	};
	let parts: Vec<&str> = list.split(',').map(str::trim).collect();
	let targets: Vec<Target> =
		parts.iter().filter(|part| !part.is_empty()).map(|part| Target::parse(part)).collect();
	let fault = if after.is_none() {
		Some("the `)` after the targets is missing".to_string())
	} else if targets.is_empty() {
		Some("`allow()` names no code".to_string())
	} else if targets.len() < parts.len() {
		Some("a target is empty: remove the stray `,`".to_string())
	} else {
		None
	};
	let mut directive = Directive {
		targets,
		reason: None,
		fault,
		rejected: false,
		line,
	};
	if let Some(after) = after {
		match parse_reason(after) {
			Ok(reason) => directive.reason = reason,
			Err(why) => {
				directive.fault.get_or_insert(why);
			}
		}
	}
	Some(directive)
}

/// The text after the closing `)`: nothing, or `reason="<text>"`. A blank reason is `None`.
fn parse_reason(after: &str) -> Result<Option<String>, String> {
	let after = after.trim();
	if after.is_empty() {
		return Ok(None);
	}
	let Some(reason) = after
		.strip_prefix("reason")
		.map(str::trim_start)
		.and_then(|rest| rest.strip_prefix('='))
		.map(str::trim_start)
		.and_then(|rest| rest.strip_prefix('"'))
	else {
		return Err(format!("`{after}` follows the targets; only `reason=\"...\"` may"));
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
	Ok(Some(text).filter(|text| !text.trim().is_empty()))
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
		for key in [
			"E2",
			"E200100",
			"X2001",
			"e2001",
			"7x",
			"777xxx",
			"xxxx",
			"*",
			"unused-let",
			"2xx",
			"7xx",
			"10xx",
			"C100",
			"C1000100",
			"10",
			"XX7xxx",
		] {
			let error = policy(&format!("[lints]\n{key:?} = \"warn\"\n")).expect_err(key);
			let message = format!("{error:#}");
			assert!(message.contains(&format!("{key:?}")), "names the key: {message}");
			assert!(message.contains("7xxx"), "lists the forms: {message}");
			assert!(message.contains("C10001"), "has a five-digit example: {message}");
		}
	}

	#[test]
	fn five_digit_codes_and_families_are_keys() {
		for (key, number) in [
			("C10001", 10001),
			("10001", 10001),
			("A11001", 11001),
			("M12001", 12001),
			("D13001", 13001),
			("P14001", 14001),
			("E7001", 7001),
		] {
			let policy = policy_with(&format!("[lints]\n{key:?} = \"allow\"\n"));
			assert_eq!(
				policy.resolve(Code::from_number(number), Error, Level::Deny),
				None,
				"{key}"
			);
			assert_eq!(
				policy.resolve(Code::from_number(number + 1), Error, Level::Deny),
				Some(Error),
				"{key} is one code"
			);
		}
		for key in ["10xxx", "10XXX", "10*"] {
			let policy = policy_with(&format!("[lints]\n{key:?} = \"warn\"\n"));
			for number in [10001, 10999] {
				assert_eq!(
					policy.resolve(Code::from_number(number), Error, Level::Deny),
					Some(Warning),
					"{key} covers {number}"
				);
			}
			for number in [1002, 9101, 11001, 14001] {
				assert_eq!(
					policy.resolve(Code::from_number(number), Error, Level::Deny),
					Some(Error),
					"{key} leaves {number} alone"
				);
			}
		}
		// `1xxx` is family 1 and never reaches 10xxx to 14xxx.
		let one = policy_with("[lints]\n\"1xxx\" = \"allow\"\n");
		assert_eq!(one.resolve(Code::from_number(1002), Error, Level::Deny), None);
		assert_eq!(one.resolve(Code::from_number(10001), Error, Level::Deny), Some(Error));
		// A family wildcard has one or two digits, and `9xxx` is the only one over SurrealKit's
		// codes.
		let kit = policy_with("[lints]\n\"9xxx\" = \"warn\"\n");
		assert_eq!(kit.resolve(K9003, Error, Level::Deny), Some(Warning));
		assert_eq!(kit.resolve(Code::from_number(1002), Error, Level::Deny), Some(Error));
		let wide = policy_with("[lints]\n\"77xxx\" = \"allow\"\n");
		assert_eq!(wide.resolve(Code::from_number(7001), Error, Level::Deny), Some(Error));
	}

	#[test]
	fn an_exact_five_digit_entry_beats_its_family() {
		let policy = policy_with("[lints]\n\"10xxx\" = \"deny\"\nC10001 = \"allow\"\n");
		assert_eq!(policy.resolve(Code::from_number(10001), Warning, Level::Warn), None);
		assert_eq!(policy.resolve(Code::from_number(10002), Warning, Level::Warn), Some(Error));
	}

	#[test]
	fn a_suppression_names_a_five_digit_code() {
		let text = "-- surreal: allow(C10001) reason=\"x\"\nSELECT 1;\n";
		let (suppressions, findings) = read(text, "");
		assert!(findings.is_empty(), "{findings:?}");
		assert!(silenced(&suppressions, text, Code::from_number(10001), text.len() - 3));
		assert!(!silenced(&suppressions, text, Code::from_number(1000), text.len() - 3));
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
		let text = "-- surreal: allow(K9001)\n-- surreal: allow(K9003)\nDEFINE X; -- surreal: allow(K9002)\n";
		let (suppressions, findings) = read(text, "");
		assert!(findings.is_empty());
		let statement = text.find("DEFINE").unwrap();
		assert!(silenced(&suppressions, text, K9003, statement));
		assert!(!silenced(&suppressions, text, Code::kit(9001), statement));
		assert!(silenced(&suppressions, text, Code::kit(9002), statement), "a trailing K code");
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
			"-- surreal: allowx(E1002)\nx",
			"-- surreal: allow_(E1002)\nx",
			"-- surreal: allow E1002\nx",
			"-- surreal: allow\nx",
			"-- surreal: note\nx",
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
		let directive =
			parse_directive("-- surreal: allow(E1002) reason=\"a \\\"b\\\"\"", 0).unwrap();
		assert_eq!(directive.reason.as_deref(), Some("a \"b\""));
		assert_eq!(directive.fault, None);
	}

	#[test]
	fn a_missing_reason_is_l7013_only_when_one_is_required_and_then_suppresses_nothing() {
		let text = "-- surreal: allow(E1002)\nx";
		let (_, findings) = read(text, "");
		assert!(findings.is_empty());
		for text in ["-- surreal: allow(E1002)\nx", "-- surreal: allow(E1002) reason=\"  \"\nx"] {
			let (suppressions, findings) = read(text, "require_suppression_reason = true\n");
			assert_eq!(findings.len(), 1, "{text:?}");
			assert_eq!(findings[0].code, L7013);
			assert!(findings[0].message.contains("no reason"));
			assert!(
				!silenced(&suppressions, text, Code::from_number(1002), text.len() - 1),
				"the directive is rejected and suppresses nothing"
			);
		}
	}

	#[test]
	fn a_directive_without_a_reason_suppresses_when_none_is_required() {
		let text = "-- surreal: allow(E1002)\nx";
		let (suppressions, findings) = read(text, "");
		assert!(findings.is_empty());
		assert!(!suppressions.directives()[0].rejected);
		assert!(silenced(&suppressions, text, Code::from_number(1002), text.len() - 1));
	}

	/// The suppressions of host-language `text`, whose comment ranges `comments` gives.
	#[cfg(feature = "host-queries")]
	fn read_host(text: &str, comments: Vec<Range<usize>>) -> (Suppressions, Vec<Finding>) {
		Suppressions::from_comments(Path::new("/p/a.ts"), text, &comments, &policy_with(""))
	}

	#[cfg(feature = "host-queries")]
	#[test]
	fn host_comment_ranges_feed_the_same_reader() {
		let text = "// surreal: allow(K9201) reason=\"x\"\ndb.query(`a ${b}`);\ndb.query(`c ${d}`); // surreal: allow(k9201)\n";
		let ranges = crate::host_queries::comments("a.ts", text);
		let (suppressions, findings) = read_host(text, ranges);
		assert!(findings.is_empty(), "{findings:?}");
		let code = Code::kit(9201);
		assert!(silenced(&suppressions, text, code, text.find("db.query").unwrap()), "next line");
		assert!(silenced(&suppressions, text, code, text.rfind("db.query").unwrap()), "own line");
		assert!(!silenced(&suppressions, text, K9003, text.find("db.query").unwrap()));
	}

	#[cfg(feature = "host-queries")]
	#[test]
	fn a_host_block_comment_or_a_directive_in_a_string_is_not_read() {
		let text = "/* surreal: allow(K9201) */\nconst s = \"// surreal: allow(K9201)\";\ndb.query(`a ${b}`);\n";
		let ranges = crate::host_queries::comments("a.ts", text);
		let (suppressions, findings) = read_host(text, ranges);
		assert!(findings.is_empty());
		for at in [text.find("const").unwrap(), text.find("db.query").unwrap()] {
			assert!(!silenced(&suppressions, text, Code::kit(9201), at));
		}
	}

	#[cfg(feature = "host-queries")]
	#[test]
	fn a_malformed_host_directive_is_l7013_at_the_comment() {
		let text = "// surreal: allow(*)\ndb.query(`a ${b}`);\n";
		let ranges = crate::host_queries::comments("a.ts", text);
		let (suppressions, findings) = read_host(text, ranges);
		assert_eq!(findings.len(), 1);
		assert_eq!(findings[0].code, L7013);
		assert_eq!(findings[0].span, 0..text.find('\n').unwrap());
		assert!(!silenced(&suppressions, text, Code::kit(9201), text.find("db.query").unwrap()));
	}

	#[cfg(feature = "host-queries")]
	#[test]
	fn a_host_directive_without_a_required_reason_is_l7013_and_suppresses_nothing() {
		let text = "// surreal: allow(K9201)\ndb.query(`a ${b}`);\n";
		let ranges = crate::host_queries::comments("a.ts", text);
		let policy = policy_with("require_suppression_reason = true\n");
		let (suppressions, findings) =
			Suppressions::from_comments(Path::new("/p/a.ts"), text, &ranges, &policy);
		assert_eq!(findings.len(), 1, "{findings:?}");
		assert_eq!(findings[0].code, L7013);
		assert!(findings[0].message.contains("suppresses nothing"));
		assert!(!silenced(&suppressions, text, Code::kit(9201), text.find("db.query").unwrap()));
		// With a reason it applies.
		let text = "// surreal: allow(K9201) reason=\"x\"\ndb.query(`a ${b}`);\n";
		let ranges = crate::host_queries::comments("a.ts", text);
		let (suppressions, findings) =
			Suppressions::from_comments(Path::new("/p/a.ts"), text, &ranges, &policy);
		assert!(findings.is_empty(), "{findings:?}");
		assert!(silenced(&suppressions, text, Code::kit(9201), text.find("db.query").unwrap()));
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

	/// The suppressions and findings for the one-line directive `-- surreal: <body>`.
	fn read_body(body: &str, toml: &str) -> (String, Suppressions, Vec<Finding>) {
		let text = format!("-- surreal: {body}\nx");
		let (suppressions, findings) = read(&text, toml);
		(text, suppressions, findings)
	}

	/// Whether the directive `body` silences `code` on the line it covers.
	fn body_silences(body: &str, code: Code) -> bool {
		let (text, suppressions, _) = read_body(body, "");
		silenced(&suppressions, &text, code, text.len() - 1)
	}

	const E1002: Code = Code::from_number(1002);

	#[test]
	fn whitespace_is_optional_around_the_parentheses_commas_and_equals() {
		for body in [
			"allow(E1002,K9201)reason=\"x\"",
			"allow(E1002, K9201) reason=\"x\"",
			"allow ( E1002 , K9201 ) reason = \"x\" ",
			"allow\t(\tE1002\t,\tK9201\t)\treason\t=\t\"x\"\t",
			"allow(  E1002  ,  K9201  )   reason  =  \"x\"",
		] {
			let (text, suppressions, findings) =
				read_body(body, "require_suppression_reason = true\n");
			assert!(findings.is_empty(), "{body:?}: {findings:?}");
			for code in [E1002, Code::kit(9201)] {
				assert!(silenced(&suppressions, &text, code, text.len() - 1), "{body:?}: {code}");
			}
		}
	}

	#[test]
	fn both_prefixes_and_all_three_markers_take_a_list() {
		for marker in ["--", "//", "#", "-- ", "// ", "# "] {
			for prefix in [SUPPRESSION_PREFIX, ANALYZER_SUPPRESSION_PREFIX] {
				let text = format!("{marker}{prefix} allow(E1002, K9201)\nx");
				let (suppressions, findings) = read(&text, "");
				assert!(findings.is_empty(), "{text:?}: {findings:?}");
				for code in [E1002, Code::kit(9201)] {
					assert!(silenced(&suppressions, &text, code, text.len() - 1), "{text:?}");
				}
			}
		}
	}

	#[test]
	fn a_list_suppresses_each_of_its_codes_and_no_other() {
		let body = "allow(E1002, K9201) reason=\"x\"";
		let (text, suppressions, findings) = read_body(body, "");
		assert!(findings.is_empty());
		let at = text.len() - 1;
		assert!(silenced(&suppressions, &text, E1002, at));
		assert!(silenced(&suppressions, &text, Code::kit(9201), at));
		assert!(!silenced(&suppressions, &text, Code::from_number(1003), at));
		assert!(!silenced(&suppressions, &text, K9003, at));
	}

	#[test]
	fn a_bad_target_is_one_l7013_and_the_other_targets_still_apply() {
		let (text, suppressions, findings) = read_body("allow(E1002, 9201)", "");
		assert_eq!(findings.len(), 1, "{findings:?}");
		assert_eq!(findings[0].code, L7013);
		assert_eq!(findings[0].span, 0..text.find('\n').unwrap(), "the whole comment");
		assert!(findings[0].message.contains("`9201`"), "{}", findings[0].message);
		assert!(findings[0].message.contains("K9201"), "names the form: {}", findings[0].message);
		let at = text.len() - 1;
		assert!(silenced(&suppressions, &text, E1002, at));
		assert!(!silenced(&suppressions, &text, Code::kit(9201), at), "9201 names nothing");

		// Two bad targets are two findings.
		let (_, _, findings) = read_body("allow(1002, E1003, 9*)", "");
		assert_eq!(findings.len(), 2, "{findings:?}");
	}

	#[test]
	fn a_kit_target_is_one_defined_kit_code() {
		for body in ["allow(K9201)", "allow(k9201)", "allow( K9003 )"] {
			let (_, _, findings) = read_body(body, "");
			assert!(findings.is_empty(), "{body}: {findings:?}");
		}
		assert!(body_silences("allow(k9201)", Code::kit(9201)), "the letter is case-insensitive");
		for (body, says) in [
			("allow(9201)", "K9201"),
			("allow(K9xxx)", "no wildcards"),
			("allow(K9*)", "no wildcards"),
			("allow(9xxx)", "no wildcards"),
			("allow(9XXX)", "no wildcards"),
			("allow(9*)", "no wildcards"),
			("allow(K1234)", "is not a SurrealKit code"),
			("allow(K9999)", "is not a SurrealKit code"),
			("allow(K92010)", "SurrealKit"),
			("allow(K)", "SurrealKit"),
		] {
			let (text, suppressions, findings) = read_body(body, "");
			assert_eq!(findings.len(), 1, "{body}: {findings:?}");
			assert_eq!(findings[0].code, L7013);
			assert!(findings[0].message.contains(says), "{body}: {}", findings[0].message);
			assert!(!silenced(&suppressions, &text, Code::kit(9201), text.len() - 1), "{body}");
		}
	}

	#[test]
	fn an_engine_target_is_one_rendered_code() {
		for body in
			["allow(E1002)", "allow(C10001)", "allow(S0001)", "allow(L7001)", "allow(P14001)"]
		{
			let (_, _, findings) = read_body(body, "");
			assert!(findings.is_empty(), "{body}: {findings:?}");
		}
		assert!(body_silences("allow(E1002)", E1002));
		assert!(body_silences("allow(C10001)", Code::from_number(10001)));
		assert!(!body_silences("allow(L1002)", E1002), "the letter is part of the code");
		assert!(!body_silences("allow(e1002)", E1002), "an engine letter is a capital");
		for body in [
			"allow(1002)",
			"allow(E1xxx)",
			"allow(E1*)",
			"allow(1xxx)",
			"allow(*)",
			"allow(unused-let)",
			"allow(e1002)",
			"allow(E100)",
			"allow(E100200)",
			"allow(X1002)",
			"allow(E1002 E1003)",
			"allow(E99999)",
		] {
			let (text, suppressions, findings) = read_body(body, "");
			assert_eq!(findings.len(), 1, "{body}: {findings:?}");
			assert_eq!(findings[0].code, L7013);
			assert!(
				findings[0].message.contains("rendered engine code"),
				"{body}: {}",
				findings[0].message
			);
			assert!(!silenced(&suppressions, &text, E1002, text.len() - 1), "{body}");
		}
	}

	#[test]
	fn a_fault_of_the_directive_is_one_l7013_whatever_the_targets() {
		for body in [
			"allow()",
			"allow( )",
			"allow(E1002",
			"allow(E1002) reasn=\"x\"",
			"allow(E1002) reason=\"open",
			"allow(E1002) reason=\"a\" extra",
			"allow(E1002) reason",
			"allow(E1002,)",
			"allow(,E1002)",
			"allow(E1002,,K9201)",
			// The bad target is not reported on its own: the directive is one finding.
			"allow(E1002, 9201) reason=\"open",
			"allow(E1002, 9201",
		] {
			let (text, suppressions, findings) = read_body(body, "");
			assert_eq!(findings.len(), 1, "{body}: {findings:?}");
			assert_eq!(findings[0].code, L7013);
			assert!(findings[0].message.starts_with("malformed suppression directive"), "{body}");
			assert_eq!(findings[0].span, 0..text.find('\n').unwrap());
			for code in [E1002, Code::kit(9201)] {
				assert!(!silenced(&suppressions, &text, code, text.len() - 1), "{body}: {code}");
			}
		}
	}

	#[test]
	fn a_missing_reason_is_one_l7013_per_directive_not_per_target_and_every_target_is_refused() {
		let toml = "require_suppression_reason = true\n";
		let (text, suppressions, findings) = read_body("allow(E1002, K9201)", toml);
		assert_eq!(findings.len(), 1, "{findings:?}");
		assert!(findings[0].message.contains("no reason"));
		for code in [E1002, Code::kit(9201)] {
			assert!(!silenced(&suppressions, &text, code, text.len() - 1), "{code}");
		}
		// A bad target and a missing reason are two faults.
		let (_, _, findings) = read_body("allow(E1002, 9201)", toml);
		assert_eq!(findings.len(), 2, "{findings:?}");
		// A fault of the directive is one finding, with the reason moot.
		let (_, _, findings) = read_body("allow(E1002", toml);
		assert_eq!(findings.len(), 1, "{findings:?}");
	}

	#[test]
	fn a_target_has_the_owner_its_text_decides() {
		use Owner::{Engine, Kit};
		for (text, owner) in [
			("K9201", Kit),
			("k9201", Kit),
			("K9xxx", Kit),
			("K9*", Kit),
			("K1234", Kit),
			("K", Kit),
			("kit", Kit),
			("9201", Kit),
			("9xxx", Kit),
			("9XXX", Kit),
			("9*", Kit),
			("E1002", Engine),
			("C10001", Engine),
			("1002", Engine),
			("E1xxx", Engine),
			("1xxx", Engine),
			("*", Engine),
			("unused-let", Engine),
			("E9201", Engine),
			("92010", Engine),
			("9x", Engine),
			("9xx", Engine),
			("", Engine),
		] {
			assert_eq!(Target::parse(text).owner, owner, "{text:?}");
		}
		let code = |text| Target::parse(text).code;
		assert_eq!(code("K9201"), Ok(Code::kit(9201)));
		assert_eq!(code("k9201"), Ok(Code::kit(9201)), "rendered with the capital");
		assert_eq!(code("E1002"), Ok(E1002));
		assert_eq!(code("C10001"), Ok(Code::from_number(10001)));
		assert_eq!(code("E9201"), Ok(Code::new('E', 9201)), "not K9201");
		assert!(code("9201").is_err() && code("K9*").is_err() && code("1002").is_err());
	}

	#[test]
	fn an_engine_letter_on_a_9_number_is_not_the_kit_code() {
		let (text, suppressions, findings) = read_body("allow(E9201)", "");
		assert!(findings.is_empty(), "{findings:?}");
		assert!(!silenced(&suppressions, &text, Code::kit(9201), text.len() - 1));
		assert!(silenced(&suppressions, &text, Code::new('E', 9201), text.len() - 1));
	}

	#[test]
	fn has_engine_target_is_true_when_the_engine_reads_any_target() {
		for (body, expected) in [
			("allow(K9201)", false),
			("allow(K9201, 9xxx)", false),
			("allow(K9201, K9003) reason=\"x\"", false),
			("allow(9201)", false),
			("allow(E1002)", true),
			("allow(E1002, C10001)", true),
			("allow(K9201, E1002)", true),
			("allow(K9201, 1002)", true),
			("allow(*)", true),
			// A directive with a fault keeps the targets that could be read.
			("allow(K9201, E1002", true),
			("allow(K9201) reasn=\"x\"", false),
			("allow(E1002) reasn=\"x\"", true),
			("allow()", false),
		] {
			let directive = directive(&format!("-- surreal: {body}"));
			assert_eq!(directive.has_engine_target(), expected, "{body}");
		}
	}

	fn directive(comment: &str) -> Directive {
		parse_directive(comment, 0).unwrap_or_else(|| panic!("{comment:?} is a directive"))
	}

	#[test]
	fn the_directives_of_a_file_are_exposed_with_the_line_they_cover() {
		let text = "-- surreal: allow(K9003, E1002)\nA;\nB; // surreal: allow(9*)\n";
		let (suppressions, _) = read(text, "");
		let lines: Vec<usize> = suppressions.directives().iter().map(|d| d.line).collect();
		assert_eq!(lines, [2, 3]);
		let first = &suppressions.directives()[0];
		assert_eq!(first.targets.len(), 2);
		assert!(first.fault.is_none());
		assert!(suppressions.directives()[1].targets[0].code.is_err());
	}
}
