//! How the CLI reports its version and its failures, for scripts and for the
//! `surreal` launcher, which runs this binary as `surreal kit`:
//!
//! - `-v`, `--version` (and clap's older `-V`, hidden) print one line, `<version> for <os> on
//!   <arch>`, which the launcher reads the installed version from. With `--json` it is one
//!   document, `{"arch", "os", "version"}`.
//! - `-j`, `--json` asks for machine-readable output on stdout. A failure is then the JSON error
//!   envelope on stderr instead of prose, whether clap refused the arguments or the command failed.
//! - Help names the command the way the launcher says it was typed, [`INVOKED_AS_VAR`].
//!
//! The help wording and the JSON shapes are a contract scripts rely on, so the
//! tests below pin them.

use std::ffi::OsString;

use serde::Serialize;

/// The help line for `-v, --version`.
pub const VERSION_HELP: &str = "Print version";

/// The help line for `-j, --json`.
pub const JSON_HELP: &str = "Emit machine-readable JSON on stdout";

/// The variable `--json` also reads, accepting clap's boolish values.
pub const JSON_VAR: &str = "SURREAL_KIT_JSON";

/// The variable the `surreal` launcher sets when it runs this binary, naming
/// how the user reached it: `surreal kit`. Read so `--help` says `Usage:
/// surreal kit sync` to somebody who typed exactly that.
pub const INVOKED_AS_VAR: &str = "SURREAL_INVOKED_AS";

/// The release, `X.Y.Z[-pre]`: the first word `--version` prints, which the
/// launcher checks an install against.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The `kind` of a bad invocation: clap's own errors.
pub const USAGE: &str = "usage";
/// The `kind` of any other failure.
pub const UNKNOWN: &str = "unknown";

/// What `--version --json` prints. The keys and their order are pinned:
/// additions only.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct VersionReport {
	/// [`std::env::consts::ARCH`], such as `aarch64`.
	pub arch: &'static str,
	/// [`std::env::consts::OS`], such as `macos`.
	pub os: &'static str,
	/// The release.
	pub version: &'static str,
}

impl VersionReport {
	/// This binary's report.
	pub fn current() -> Self {
		Self {
			arch: std::env::consts::ARCH,
			os: std::env::consts::OS,
			version: VERSION,
		}
	}
}

/// `<version> for <os> on <arch>`: what `--version` prints, the release first.
pub fn version_line() -> String {
	let VersionReport {
		arch,
		os,
		version,
	} = VersionReport::current();
	format!("{version} for {os} on {arch}")
}

/// Print the version, as one line or, under JSON, as one document.
pub fn print_version(json: bool) {
	if json {
		println!("{}", pretty(&VersionReport::current()));
	} else {
		println!("{}", version_line());
	}
}

/// A value as the CLI writes JSON: pretty-printed.
pub fn pretty(value: &impl Serialize) -> String {
	serde_json::to_string_pretty(value).expect("a report serializes")
}

/// One failure, as the `--json` error envelope: one object on stderr, with
/// every key present and `null` when unknown.
///
/// Field order is the order the keys are written in. `status`, `code`,
/// `request_id`, `docs` and `retry_after_secs` are always `null` here; they are
/// written rather than left out, so a consumer can index without checking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Envelope {
	pub kind: &'static str,
	pub message: String,
	pub status: Option<u16>,
	pub code: Option<String>,
	pub request_id: Option<String>,
	pub hint: Option<String>,
	pub command: Option<String>,
	pub docs: Option<String>,
	pub retry_after_secs: Option<u64>,
	pub exit_code: u8,
}

impl Envelope {
	/// A failure of `kind` with nothing else known about it.
	pub fn new(kind: &'static str, message: impl Into<String>, exit_code: u8) -> Self {
		Self {
			kind,
			message: message.into(),
			status: None,
			code: None,
			request_id: None,
			hint: None,
			command: None,
			docs: None,
			retry_after_secs: None,
			exit_code,
		}
	}

	/// clap's usage error as an envelope.
	///
	/// The message is clap's error without its `error: `: the first line, with
	/// the indented lines clap sets under it joined on, and without the usage
	/// block and the tip that follow. The command is the failing command's own
	/// `--help`, read off clap's usage line, so `surreal kit rollout start`
	/// points at `surreal kit rollout start --help`. `fallback` is the
	/// program's name, for an error that carries no usage line.
	pub fn usage(err: &clap::Error, fallback: &str) -> Self {
		use clap::error::ErrorKind;

		let rendered = err.to_string();
		let path = rendered
			.lines()
			.find_map(|line| line.trim().strip_prefix("Usage: "))
			.map(|usage| {
				usage
					.split_whitespace()
					.take_while(|word| !word.starts_with(['[', '<', '-']))
					.collect::<Vec<_>>()
					.join(" ")
			})
			.filter(|path| !path.is_empty());
		let message = match err.kind() {
			// What clap renders here is the help itself, whose first line is
			// the command's description: no use as a message.
			ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand | ErrorKind::MissingSubcommand => {
				match &path {
					Some(path) => format!("`{path}` needs a command"),
					None => String::from("a command is required"),
				}
			}
			_ => first_paragraph(&rendered),
		};
		let exit_code = u8::try_from(err.exit_code()).unwrap_or(2);
		Self {
			hint: Some(String::from("See the usage with:")),
			command: Some(format!("{} --help", path.as_deref().unwrap_or(fallback))),
			..Self::new(USAGE, message, exit_code)
		}
	}

	/// A command's failure as an envelope: the whole context chain as one
	/// line, exit code 1.
	pub fn failure(err: &anyhow::Error) -> Self {
		Self::new(UNKNOWN, format!("{err:#}"), 1)
	}

	/// Write the envelope to stderr, pretty-printed and ending in a newline.
	pub fn eprint(&self) {
		eprintln!("{}", pretty(self));
	}
}

/// clap's error text up to its first blank line, as one line: the first line
/// without its `error: `, then each indented line under it, trimmed.
fn first_paragraph(rendered: &str) -> String {
	let mut lines = rendered.lines().skip_while(|line| line.trim().is_empty());
	let first = lines.next().unwrap_or("");
	let first = first.strip_prefix("error: ").unwrap_or(first);
	std::iter::once(first)
		.chain(
			lines
				.take_while(|line| line.starts_with([' ', '\t']) && !line.trim().is_empty())
				.map(str::trim),
		)
		.collect::<Vec<_>>()
		.join(" ")
}

/// Whether the caller asked for JSON, when the parse that would have said so
/// failed or stopped early: [`JSON_VAR`] set to a yes, or `--json` or `-j`,
/// alone or in a cluster of short flags (`-qj`), among the arguments before a
/// `--`.
///
/// `command` says which short flags take a value: in a cluster, the letters
/// after one of those are its value, not flags, so `-sj` is a schema named `j`.
pub fn asks_for_json(command: &clap::Command, argv: &[OsString], var: Option<&str>) -> bool {
	if var.is_some_and(boolish) {
		return true;
	}
	let mut value_shorts = Vec::new();
	shorts_taking_values(command, &mut value_shorts);
	argv.iter().skip(1).take_while(|arg| *arg != "--").filter_map(|arg| arg.to_str()).any(|arg| {
		arg == "--json"
			|| arg
				.strip_prefix('-')
				.filter(|cluster| {
					!cluster.is_empty() && cluster.bytes().all(|b| b.is_ascii_alphabetic())
				})
				.is_some_and(|cluster| {
					cluster
						.chars()
						.take_while(|short| !value_shorts.contains(short))
						.any(|short| short == 'j')
				})
	})
}

/// clap's boolish yes: what `SURREAL_KIT_JSON=1` means to the parser too.
fn boolish(value: &str) -> bool {
	matches!(value.trim().to_ascii_lowercase().as_str(), "y" | "yes" | "t" | "true" | "on" | "1")
}

/// Every short flag in `command`'s tree that takes a value.
fn shorts_taking_values(command: &clap::Command, into: &mut Vec<char>) {
	for arg in command.get_arguments() {
		if arg.get_action().takes_values()
			&& let Some(shorts) = arg.get_short_and_visible_aliases()
		{
			into.extend(shorts);
		}
	}
	for subcommand in command.get_subcommands() {
		shorts_taking_values(subcommand, into);
	}
}

/// The name to show in help, when the launcher supplied a usable one.
///
/// Absent, empty or odd values keep the executable's own name. The value lands
/// in help text on stdout, so it is held to what a command name can plausibly
/// contain rather than trusted with control characters.
pub fn invoked_as(raw: Option<&str>) -> Option<String> {
	let name = raw?.trim();
	let plausible = !name.is_empty()
		&& name.len() <= 64
		&& name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '-' | '_' | '.'));
	plausible.then(|| name.to_owned())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn argv(args: &[&str]) -> Vec<OsString> {
		std::iter::once("surrealkit").chain(args.iter().copied()).map(OsString::from).collect()
	}

	/// A command with a value-taking short (`-s`) beside the flags.
	fn command() -> clap::Command {
		use clap::{Arg, ArgAction, Command};
		Command::new("surrealkit")
			.arg(Arg::new("json").long("json").short('j').action(ArgAction::SetTrue))
			.arg(Arg::new("schema").long("schema").short('s').action(ArgAction::Append))
			.subcommand(
				Command::new("init").arg(Arg::new("yes").short('y').action(ArgAction::SetTrue)),
			)
	}

	fn asks(args: &[&str]) -> bool {
		asks_for_json(&command(), &argv(args), None)
	}

	#[test]
	fn the_version_line_leads_with_the_release() {
		assert_eq!(
			version_line(),
			format!("{VERSION} for {} on {}", std::env::consts::OS, std::env::consts::ARCH)
		);
	}

	#[test]
	fn the_version_document_is_arch_os_version_in_that_order() {
		let rendered = pretty(&VersionReport::current());
		let value: serde_json::Value = serde_json::from_str(&rendered).expect("one object");
		let keys: Vec<&str> =
			value.as_object().expect("an object").keys().map(String::as_str).collect();
		assert_eq!(keys, ["arch", "os", "version"]);
		assert!(rendered.find("\"arch\"") < rendered.find("\"os\""));
		assert!(rendered.find("\"os\"") < rendered.find("\"version\""));
		assert_eq!(value["version"], VERSION);
	}

	/// Every key, in order, `null` when unknown: a consumer indexes the object
	/// without checking, so a vanished key is a second failure.
	#[test]
	fn the_envelope_writes_every_key_in_order() {
		let rendered = pretty(&Envelope::new(UNKNOWN, "connection refused", 1));
		let value: serde_json::Value = serde_json::from_str(&rendered).expect("one object");
		let keys = [
			"kind",
			"message",
			"status",
			"code",
			"request_id",
			"hint",
			"command",
			"docs",
			"retry_after_secs",
			"exit_code",
		];
		assert_eq!(value.as_object().expect("an object").len(), keys.len());
		let positions: Vec<usize> = keys
			.iter()
			.map(|key| {
				rendered
					.find(&format!("\"{key}\":"))
					.unwrap_or_else(|| panic!("{key} missing: {rendered}"))
			})
			.collect();
		assert!(positions.is_sorted(), "{rendered}");
		assert_eq!(value["status"], serde_json::Value::Null);
		assert_eq!(value["exit_code"], 1);
	}

	#[test]
	fn a_failure_carries_the_whole_context_chain() {
		let err = anyhow::anyhow!("connection refused").context("connecting to the database");
		let envelope = Envelope::failure(&err);
		assert_eq!(envelope.kind, "unknown");
		assert_eq!(envelope.message, "connecting to the database: connection refused");
		assert_eq!(envelope.exit_code, 1);
	}

	#[test]
	fn a_usage_error_names_the_failing_commands_help() {
		use clap::{Arg, Command};
		let mut command = Command::new("surrealkit")
			.bin_name("surreal kit")
			.subcommand(Command::new("apply").arg(Arg::new("path").required(true)));
		let err = command.try_get_matches_from_mut(["surrealkit", "apply", "--bogus"]).unwrap_err();
		let envelope = Envelope::usage(&err, "surreal kit");
		assert_eq!(envelope.kind, "usage");
		assert_eq!(envelope.message, "unexpected argument '--bogus' found");
		assert_eq!(envelope.hint.as_deref(), Some("See the usage with:"));
		assert_eq!(envelope.command.as_deref(), Some("surreal kit apply --help"));
		assert_eq!(envelope.exit_code, 2);
	}

	#[test]
	fn json_is_asked_for_only_before_a_double_dash() {
		assert!(asks(&["--json", "init"]));
		assert!(asks(&["init", "-j"]));
		assert!(!asks(&["init"]));
		assert!(!asks(&["init", "--", "--json"]));
		assert!(!asks(&["--jsonx"]));
	}

	/// In a cluster of shorts, `j` counts until a short that takes a value:
	/// the rest of the cluster is that value.
	#[test]
	fn j_in_a_cluster_of_short_flags_asks_for_json() {
		assert!(asks(&["init", "-yj"]));
		assert!(asks(&["-js", "core"]));
		assert!(!asks(&["-sj"]));
		assert!(!asks(&["-", "init"]));
		assert!(!asks(&["-j1"]));
	}

	#[test]
	fn the_variable_asks_for_json_with_clap_boolish_values() {
		for yes in ["1", "true", "yes", "on", "TRUE"] {
			assert!(asks_for_json(&command(), &argv(&[]), Some(yes)), "{yes}");
		}
		for no in ["0", "false", "no", "off", ""] {
			assert!(!asks_for_json(&command(), &argv(&[]), Some(no)), "{no}");
		}
	}

	#[test]
	fn a_launcher_name_is_used_only_when_it_is_plausible() {
		assert_eq!(invoked_as(Some("surreal kit")), Some("surreal kit".into()));
		assert_eq!(invoked_as(Some("  surreal kit ")), Some("surreal kit".into()));
		assert_eq!(invoked_as(None), None);
		assert_eq!(invoked_as(Some("")), None);
		assert_eq!(invoked_as(Some("surreal\nkit")), None);
		assert_eq!(invoked_as(Some("surreal \x1b[31mkit")), None);
		assert_eq!(invoked_as(Some(&"x".repeat(65))), None);
	}
}
