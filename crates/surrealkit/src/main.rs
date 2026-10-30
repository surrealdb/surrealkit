// The binary is the one place that may write to the console directly: it owns the
// CLI's output format. Library modules go through `log` (see CliLogger below).
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::builder::BoolishValueParser;
use clap::{ArgAction, CommandFactory, FromArgMatches, Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{DbCfg, DbOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::module::Module;
use surrealkit::project::{ProjectConfig, Target};
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts, RolloutUpOpts};
use surrealkit::setup::force_setup;
use surrealkit::sync::{self, SyncOpts};
use surrealkit::tester::{ReportFormat, TestOpts, run_test_to};
use surrealkit::typegen::{TypegenOpts, run_typegen};
use surrealkit::variables::{TemplateVars, build_vars, parse_var_flag};

use crate::output::Envelope;
use crate::templates::InitOpts;

// `--version`, `--json` and the JSON error envelope.
mod output;
// `init` templates are a CLI-only concern, so the module lives in the binary
// rather than the public library surface.
mod templates;

/// One line for `--help`, and the same line `surreal list` shows for `kit`.
const ABOUT: &str = "Schema migrations, seed data, typegen and tests";

const SELECTION: &str = "Selection";
const CONNECTION: &str = "Connection";
const PROJECT: &str = "Project";
const OUTPUT: &str = "Output";

/// The `surrealkit` executable, which the `surreal` launcher installs as
/// `surreal-kit` and runs as `surreal kit`.
///
/// No `name`: help and usage name the executable as it was started (or as the
/// launcher says it was typed, see [`output::INVOKED_AS_VAR`]), so they read
/// `surrealkit`, `surreal-kit` or `surreal kit` to whoever typed that.
#[derive(Parser, Debug)]
#[command(about = ABOUT, long_about = None)]
#[command(version = output::VERSION, disable_version_flag = true)]
#[command(arg_required_else_help = true)]
pub struct Cli {
	/// `-v`, `--version`, and clap's older `-V`, unlisted. clap stops parsing
	/// here, so a `--json` after it is read off the raw arguments, in
	/// [`parse_failure`].
	#[arg(
		short = 'v',
		long = "version",
		short_alias = 'V',
		action = ArgAction::Version,
		help = output::VERSION_HELP
	)]
	version: Option<bool>,

	/// Schema module to operate on (repeatable).
	///
	/// Modules are tracked independently: a module only ever prunes its own
	/// database objects. Files live in
	/// `<folder>/modules/<name>/schema`. Omit for the default module
	/// (`<folder>/schema`), which is the pre-1.0 layout.
	#[arg(short = 's', long, global = true, value_name = "NAME", help_heading = SELECTION)]
	schema: Vec<String>,

	/// Database target to operate on (repeatable).
	///
	/// A `[target.<name>]` from surrealkit.toml. Omit for the ambient connection
	/// (--host/--ns/--db and env).
	#[arg(short = 't', long, global = true, value_name = "NAME", help_heading = SELECTION)]
	target: Vec<String>,

	/// Every declared schema module against every declared database target.
	#[arg(long, global = true, help_heading = SELECTION)]
	all: bool,

	/// Continue to the next target after one fails, instead of stopping.
	#[arg(long, global = true, help_heading = SELECTION)]
	keep_going: bool,

	/// Don't pull in the `depends_on` modules of the ones selected with --schema.
	#[arg(long, global = true, help_heading = SELECTION)]
	no_deps: bool,

	/// Database host URL
	#[arg(long, global = true, help_heading = CONNECTION)]
	host: Option<String>,

	/// Database name
	#[arg(long, global = true, help_heading = CONNECTION)]
	db: Option<String>,

	/// Database namespace
	#[arg(long, global = true, help_heading = CONNECTION)]
	ns: Option<String>,

	/// Database user
	#[arg(long, global = true, help_heading = CONNECTION)]
	user: Option<String>,

	/// Database password
	#[arg(long, global = true, help_heading = CONNECTION)]
	pass: Option<String>,

	/// Authentication level: root (default), namespace/ns, or database/db
	#[arg(long, global = true, help_heading = CONNECTION)]
	auth_level: Option<String>,

	/// Seconds to wait for the connection and sign-in (default: 30).
	///
	/// `0` waits indefinitely, which was the behaviour through 1.0.0-beta.1.
	#[arg(long, global = true, value_name = "SECS", help_heading = CONNECTION)]
	connect_timeout_secs: Option<u64>,

	/// Seconds to wait for a single rollout step's SQL.
	///
	/// Unset waits indefinitely, because a legitimate index build can take
	/// hours.
	#[arg(long, global = true, value_name = "SECS", help_heading = CONNECTION)]
	query_timeout_secs: Option<u64>,

	/// Root folder for the database directory (default: `./database`).
	#[arg(long, global = true, help_heading = PROJECT)]
	folder: Option<String>,

	/// Set a template variable (repeatable): --var KEY=VALUE
	#[arg(long = "var", global = true, value_name = "KEY=VALUE", help_heading = PROJECT)]
	var: Vec<String>,

	/// Emit machine-readable JSON on stdout.
	#[arg(
		short = 'j',
		long,
		global = true,
		env = output::JSON_VAR,
		hide_env_values = true,
		value_parser = BoolishValueParser::new(),
		action = ArgAction::SetTrue,
		help = output::JSON_HELP,
		help_heading = OUTPUT
	)]
	json: bool,

	/// Show debug output as well as progress.
	#[arg(long, global = true, help_heading = OUTPUT)]
	verbose: bool,

	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
	/// Scaffold a new project from a template, selecting optional features.
	Init {
		/// Bundled template name (default: `default`). Ignored when --from is set.
		#[arg(long)]
		template: Option<String>,
		/// Use an external template: a git URL (optionally `url#rev` / `url#rev:subdir`)
		/// or a local path. Overrides --template.
		#[arg(long)]
		from: Option<String>,
		/// Enable a feature by id (repeatable). Implies non-interactive selection.
		#[arg(long = "feature", value_name = "ID")]
		feature: Vec<String>,
		/// Only scaffold the bare project; add no template features.
		#[arg(long)]
		minimal: bool,
		/// Don't prompt; accept the default features (non-interactive).
		#[arg(short = 'y', long)]
		yes: bool,
		/// Overwrite files that already exist.
		#[arg(long)]
		force: bool,
	},
	/// Redefine the metadata tables SurrealKit keeps its state in.
	Setup,
	/// Apply the schema folder to the database, removing what was deleted.
	Sync {
		/// Keep running, and sync again whenever a schema file changes.
		#[arg(long)]
		watch: bool,
		/// With --watch, how often to check for changes, in milliseconds.
		#[arg(long, value_name = "MS", default_value_t = 1000)]
		debounce_ms: u64,
		/// Report what would change, and change nothing.
		#[arg(long)]
		dry_run: bool,
		/// With --watch, stop at the first sync that fails.
		#[arg(long, default_value_t = true)]
		fail_fast: bool,
		/// Leave objects whose schema files were deleted in the database.
		#[arg(long)]
		no_prune: bool,
		/// Allow removing deleted objects from a shared database.
		#[arg(long)]
		allow_shared_prune: bool,
		/// Allow a sync with no schema files.
		///
		/// Required before an intentional prune of every managed entity.
		#[arg(long)]
		allow_empty_prune: bool,
		/// Allow statements other than DEFINE in schema files.
		///
		/// INSERT, UPDATE, CREATE and the like. Disables catalog entity tracking;
		/// only file-level hashes are tracked.
		#[arg(long)]
		allow_all_statements: bool,
	},
	/// Plan, run and roll back reviewed schema migrations.
	#[command(arg_required_else_help = true)]
	Rollout {
		#[command(subcommand)]
		command: RolloutCommands,
	},
	/// Run the seed files that are new or changed.
	///
	/// Each file runs only on first boot or when its content changes (tracked
	/// in the `__seed` table). Use `--force` to re-run all.
	Seed {
		/// Re-run every seed file, ignoring the `__seed` tracking table.
		#[arg(long)]
		force: bool,
	},
	/// Show the rollouts recorded in the database, and what is still pending.
	Status,
	/// Run one SurrealQL file against the database, with template variables.
	Apply {
		/// The `.surql` file to run.
		path: PathBuf,
	},
	/// Run the test suites, each against a database of its own.
	Test {
		/// Only the suites whose name matches this glob.
		#[arg(long, value_name = "GLOB")]
		suite: Option<String>,
		/// Only the cases whose name matches this glob.
		#[arg(long, value_name = "GLOB")]
		case: Option<String>,
		/// Only the suites and cases with this tag (repeatable).
		#[arg(long)]
		tag: Vec<String>,
		/// Stop at the first failing case.
		#[arg(long)]
		fail_fast: bool,
		/// How many suites to run at once.
		#[arg(long, value_name = "N", default_value_t = 1)]
		parallel: usize,
		/// Also write the report as JSON to this file.
		#[arg(long, value_name = "PATH")]
		json_out: Option<PathBuf>,
		/// Skip `setup` on each suite's database.
		#[arg(long)]
		no_setup: bool,
		/// Skip `sync` on each suite's database.
		#[arg(long)]
		no_sync: bool,
		/// Skip `seed` on each suite's database.
		#[arg(long)]
		no_seed: bool,
		/// The HTTP endpoint API cases call (default: `[defaults] base_url`).
		#[arg(long, value_name = "URL")]
		base_url: Option<String>,
		/// How long a single request may take, in milliseconds.
		#[arg(long, value_name = "MS")]
		timeout_ms: Option<u64>,
		/// Keep each suite's database afterwards, for inspection.
		#[arg(long)]
		keep_db: bool,
		/// How each suite's database gets its schema: `sync` from the schema
		/// folder (the default), `rollouts` replayed in order from empty, or
		/// `both`, running every suite once on each.
		#[arg(long, value_enum, value_name = "SOURCE")]
		schema_from: Option<surrealkit::tester::SchemaSource>,
	},
	/// Introspect the database and generate a typed schema document (JSON).
	Typegen {
		/// Output path (default: `[typegen] json`, else
		/// `{folder}/types/schema.json`).
		#[arg(long)]
		out: Option<PathBuf>,
		/// Print the JSON to stdout instead of writing a file.
		#[arg(long)]
		stdout: bool,
		/// Emit compact (single-line) JSON instead of pretty-printed.
		#[arg(long)]
		compact: bool,
		/// Also write TypeScript types, overriding `[typegen] typescript`: a
		/// `.ts` file, or a directory to write `index.ts` (or `[typegen]
		/// filename`) in.
		#[arg(long, value_name = "PATH")]
		typescript: Option<PathBuf>,
	},
}

/// Rollout subcommands.
///
/// The rollout-id positionals are named `rollout`, not `target`: clap derives an
/// arg id from the field name, and a positional named `target` collides with the
/// global `-t/--target`. That collision silently ate the rollout id in
/// 1.0.0-beta.1 (`unknown target "<rollout-id>"`) and hid `--target` from these
/// subcommands entirely. `no_subcommand_shadows_a_global_arg_id` pins it.
#[derive(Subcommand, Debug)]
enum RolloutCommands {
	/// Record an existing database's schema as the starting point for rollouts.
	///
	/// Changes no schema. Only once, before the first rollout.
	Baseline,
	/// Write a manifest of the schema folder's changes since the last rollout.
	Plan {
		/// A name for the rollout, used in its id.
		#[arg(long)]
		name: Option<String>,
		/// Print the plan, and write nothing.
		#[arg(long)]
		dry_run: bool,
		/// Plan changes to entities that already exist, too.
		///
		/// Such as tightening an ASSERT, not just additions and removals.
		/// Rollback restores the previous definition, which reverses the schema
		/// but not any data effect the change had.
		#[arg(long)]
		allow_modified: bool,
	},
	/// Apply a rollout's non-destructive changes, ready for cutover.
	Start {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	/// Finish a started rollout after cutover: apply its destructive changes.
	Complete {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	/// Undo a started rollout.
	Rollback {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	/// Show the rollouts recorded in the database, or one of them.
	Status {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: Option<String>,
	},
	/// Check rollout manifests without connecting.
	///
	/// With an id, that manifest; without one, every manifest and how they
	/// chain, which suits a CI check.
	Lint {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: Option<String>,
	},
	/// Run every rollout this database has not run yet, in order.
	///
	/// Each one is started and completed, except the newest, which is left ready
	/// to complete so the application can cut over first.
	Up {
		/// Complete the newest rollout too.
		#[arg(long)]
		complete: bool,
		/// The first rollout this database still needs, when its position cannot
		/// be worked out from its history.
		#[arg(long, value_name = "ROLLOUT_ID")]
		from: Option<String>,
		/// Report what would run, and run nothing.
		#[arg(long)]
		dry_run: bool,
	},
	/// Give a manifest planned before 1.0.0-beta.6 its own copy of its SQL.
	///
	/// Run it where the schema folder matches what it was planned from.
	Freeze {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	/// Drop the newest rollout from the project, as if it had never been planned.
	///
	/// Deletes its manifest and directory, and puts the snapshots back to where
	/// they were before it. Only for a rollout no database has completed.
	Discard {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
		/// Leave the snapshots as they are, for when they have been put right by
		/// hand.
		#[arg(long)]
		keep_snapshots: bool,
	},
	/// Heal a rollout stuck in an intermediate state, without re-running SQL.
	///
	/// Useful when `complete` was killed mid-flight (issue #55) and
	/// `__rollout.status` is still `running_complete` / `running_rollback`.
	Repair {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
}

/// The (schema module x database target) matrix one invocation operates on.
///
/// With no `[schema.*]`/`[target.*]` sections and no selection flags this is a
/// single pair -- the default module against the ambient connection -- which is
/// exactly the pre-1.0 behaviour.
#[derive(Debug)]
struct Selection {
	targets: Vec<Target>,
	/// Modules in dependency order. Applies to every target, then filtered by the
	/// target's own `schemas` list.
	modules: Vec<Module>,
}

impl Selection {
	fn resolve(
		project: &ProjectConfig,
		base: &DbCfg,
		schemas: &[String],
		targets: &[String],
		all: bool,
		no_deps: bool,
	) -> Result<Self> {
		// Modules: explicit --schema, else every declared module, else the default.
		let declared: Vec<String> = project.schema.keys().cloned().collect();
		let declared_list = if declared.is_empty() {
			"(none)".to_string()
		} else {
			declared.join(", ")
		};
		let wanted: Vec<String> = if !schemas.is_empty() {
			for name in schemas {
				if !project.schema.contains_key(name) && name != Module::DEFAULT_NAME {
					bail!("unknown schema module {name:?}; declared modules are: {declared_list}");
				}
			}
			schemas.to_vec()
		} else if all || !declared.is_empty() {
			declared
		} else {
			vec![Module::DEFAULT_NAME.to_string()]
		};

		// `--schema billing` pulls in `core` by default, like `cargo build -p`, so a
		// module is never applied before what it depends on. --no-deps opts out.
		let ordered = if no_deps {
			let mut only = wanted;
			only.sort();
			only
		} else {
			project.module_order(&wanted)?
		};
		let modules = ordered
			.into_iter()
			.map(Module::new)
			.collect::<Result<Vec<_>>>()
			.context("resolving selected schema modules")?;

		// Targets: explicit --target, else --all, else primary, else the ambient one.
		let resolved = if !targets.is_empty() {
			targets
				.iter()
				.map(|name| {
					let tc = project.target.get(name).ok_or_else(|| {
						anyhow::anyhow!(
							"unknown target {name:?}; declared targets are: {}",
							if project.target.is_empty() {
								"(none)".to_string()
							} else {
								project.target.keys().cloned().collect::<Vec<_>>().join(", ")
							}
						)
					})?;
					Target::resolve(name, tc, base)
				})
				.collect::<Result<Vec<_>>>()?
		} else if all && !project.target.is_empty() {
			project
				.target
				.iter()
				.map(|(n, tc)| Target::resolve(n, tc, base))
				.collect::<Result<Vec<_>>>()?
		} else if let Some((n, tc)) = project.target.iter().find(|(_, t)| t.primary).or_else(|| {
			// A single declared target is unambiguous without `primary`.
			(project.target.len() == 1).then(|| project.target.iter().next()).flatten()
		}) {
			vec![Target::resolve(n, tc, base)?]
		} else {
			vec![Target::implicit(base.clone())]
		};

		Ok(Self {
			targets: resolved,
			modules,
		})
	}

	fn targets(&self) -> &[Target] {
		&self.targets
	}

	/// The selected modules that `target` accepts, honouring its `schemas` list.
	fn modules_for(&self, target: &Target) -> Vec<Module> {
		self.modules.iter().filter(|m| target.allows(m.name())).cloned().collect()
	}

	fn pairs(&self) -> usize {
		self.targets.iter().map(|t| self.modules_for(t).len()).sum()
	}

	/// True when output should be grouped and summarised per pair.
	fn is_fan_out(&self) -> bool {
		self.pairs() > 1
	}

	/// The single selected module, for commands that cannot fan out.
	fn single_module(&self) -> Result<&Module> {
		match self.modules.as_slice() {
			[one] => Ok(one),
			other => bail!(
				"this command operates on one schema module at a time ({} selected); \
				 pass --schema <NAME>",
				other.len()
			),
		}
	}

	/// The single selected target, for commands that must not fan out.
	///
	/// Rollout execution is a locked, resumable state machine with one
	/// `__rollout` record per database. Fanning one rollout id across N targets
	/// would turn a partial failure into N databases sitting in different phases,
	/// so these commands refuse rather than loop.
	fn single_target(&self) -> Result<&Target> {
		match self.targets.as_slice() {
			[one] => Ok(one),
			other => bail!(
				"this command operates on one database target at a time ({} selected); \
				 pass --target <NAME>",
				other.len()
			),
		}
	}
}

/// Warn that `--target`/`--all` does nothing on a command that never connects.
///
/// These commands read only the filesystem, so silently accepting the flag reads
/// as "applied to that target" when nothing of the sort happened. A hard error
/// was the first attempt, but it breaks CI wrappers that pass the same flags to
/// every subcommand, and clap still advertises the globals in their `--help`
/// because they really are accepted. Warning says the true thing without
/// inventing a failure: there is no wrong-database hazard here, since there is no
/// database.
fn warn_unused_target_selection(command: &str, used: bool) {
	if used {
		log::warn!(
			"{command} reads only the filesystem and never connects, so --target/--all has \
			 no effect here and was ignored"
		);
	}
}

/// The outcome of applying one module to one target.
struct PairResult {
	module: String,
	target: String,
	error: Option<String>,
}

/// Print the per-pair summary shown after a fan-out run. Through the logger,
/// like the progress around it, so `--json` moves it off stdout.
fn report_pairs(results: &[PairResult]) {
	let mw = results.iter().map(|r| r.module.len()).max().unwrap_or(6).max("schema".len());
	let tw = results.iter().map(|r| r.target.len()).max().unwrap_or(6).max("target".len());
	log::info!("");
	log::info!("  {:<mw$}  {:<tw$}  status", "schema", "target", mw = mw, tw = tw);
	log::info!("  {}  {}  ------", "-".repeat(mw), "-".repeat(tw));
	for r in results {
		let status = if r.error.is_some() {
			"FAILED"
		} else {
			"ok"
		};
		log::info!("  {:<mw$}  {:<tw$}  {status}", r.module, r.target, mw = mw, tw = tw);
	}
	let failed = results.iter().filter(|r| r.error.is_some()).count();
	log::info!("");
	if failed == 0 {
		log::info!("{} ok", results.len());
	} else {
		log::info!("{} ok, {failed} failed", results.len() - failed);
	}
}

/// The CLI's logger.
///
/// SurrealKit's library modules emit progress through the `log` facade so that
/// library consumers get silence by default. The CLI installs this to turn that
/// back into exactly the output it printed before: **no level prefix, no
/// timestamp, no target**, `info` on stdout and `warn`/`error` on stderr —
/// matching the `println!`/`eprintln!` split those calls replaced.
///
/// Under `--json` stdout carries the command's JSON and nothing else, so every
/// line goes to stderr instead.
///
/// `env_logger` is deliberately not used: it writes everything to stderr with a
/// level prefix, which would be a visible CLI change.
struct CliLogger {
	level: log::LevelFilter,
	json: bool,
}

impl log::Log for CliLogger {
	fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
		// Only SurrealKit's own output; dependencies stay quiet.
		metadata.level() <= self.level && metadata.target().starts_with("surrealkit")
	}

	fn log(&self, record: &log::Record<'_>) {
		if !self.enabled(record.metadata()) {
			return;
		}
		match record.level() {
			_ if self.json => {
				let mut err = std::io::stderr().lock();
				let _ = writeln!(err, "{}", record.args());
			}
			log::Level::Error | log::Level::Warn => {
				let mut err = std::io::stderr().lock();
				let _ = writeln!(err, "{}", record.args());
			}
			_ => {
				// Flushed per line: progress must appear during long syncs and in
				// watch mode, where stdout is a pipe more often than a terminal.
				let mut out = std::io::stdout().lock();
				let _ = writeln!(out, "{}", record.args());
				let _ = out.flush();
			}
		}
	}

	fn flush(&self) {
		let _ = std::io::stdout().flush();
		let _ = std::io::stderr().flush();
	}
}

/// Install the CLI logger. `--verbose` raises the level to `debug`, and
/// `--json` sends everything to stderr.
fn init_logging(verbose: bool, json: bool) {
	let level = if verbose {
		log::LevelFilter::Debug
	} else {
		log::LevelFilter::Info
	};
	let logger = Box::leak(Box::new(CliLogger {
		level,
		json,
	}));
	// Only fails if a logger is already installed, which cannot happen here.
	let _ = log::set_logger(logger).map(|()| log::set_max_level(level));
}

/// Load `.env` / `.env.local` from the current working directory when present.
fn load_env() -> Option<DotEnv> {
	let has_env =
		std::path::Path::new(".env").exists() || std::path::Path::new(".env.local").exists();
	if has_env {
		Some(DotEnv::new(""))
	} else {
		None
	}
}

/// Parse the process's arguments, or leave through [`parse_failure`] when they
/// do not parse.
fn parse_args() -> Cli {
	let mut command = Cli::command();
	if let Some(invoked) = output::invoked_as(std::env::var(output::INVOKED_AS_VAR).ok().as_deref())
	{
		command = command.bin_name(invoked);
	}
	let argv: Vec<OsString> = std::env::args_os().collect();
	command
		.try_get_matches_from_mut(&argv)
		.and_then(|matches| Cli::from_arg_matches(&matches))
		.unwrap_or_else(|err| parse_failure(&err, &command, &argv))
}

/// Leave after a parse that did not produce a [`Cli`].
///
/// Help is clap's, on stdout with exit code 0. `--version` is
/// [`output::version_line`], or its document under JSON, exit code 0. A usage error is
/// exit code 2: under JSON (read off the raw arguments, because the parse that
/// would have decided it is the one that failed) it is the JSON error envelope
/// on stderr, otherwise clap's text.
fn parse_failure(err: &clap::Error, command: &clap::Command, argv: &[OsString]) -> ! {
	let json =
		|| output::asks_for_json(command, argv, std::env::var(output::JSON_VAR).ok().as_deref());
	if err.kind() == clap::error::ErrorKind::DisplayVersion {
		output::print_version(json());
		exit(0);
	}
	if err.use_stderr() && json() {
		let program = command.get_bin_name().unwrap_or_else(|| command.get_name());
		let envelope = Envelope::usage(err, program);
		envelope.eprint();
		exit(i32::from(envelope.exit_code));
	}
	err.exit()
}

/// Flush both streams and leave.
///
/// Belt-and-braces (issue #55): bypass tokio runtime shutdown so the HTTP
/// client's background connection-pool tasks can't keep the process alive
/// after the command has finished.
fn exit(code: i32) -> ! {
	let _ = std::io::stdout().flush();
	let _ = std::io::stderr().flush();
	std::process::exit(code);
}

#[tokio::main]
async fn main() {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let args = parse_args();
	let json = args.json;
	init_logging(args.verbose, json);
	let code = match run(args).await {
		Ok(code) => code,
		// The whole context chain on one line, or the envelope under JSON.
		// Either way nothing more reaches stdout.
		Err(err) if json => {
			let envelope = Envelope::failure(&err);
			envelope.eprint();
			i32::from(envelope.exit_code)
		}
		Err(err) => {
			eprintln!("error: {err:#}");
			1
		}
	};
	exit(code);
}

/// Run the command, returning the exit code it ends with.
async fn run(args: Cli) -> Result<i32> {
	let json = args.json;
	let env = load_env();
	let overrides = DbOverrides {
		host: args.host,
		ns: args.ns,
		db: args.db,
		user: args.user,
		pass: args.pass,
		auth_level: args.auth_level,
		folder: args.folder,
		connect_timeout_secs: args.connect_timeout_secs,
		query_timeout_secs: args.query_timeout_secs,
	};

	let raw_vars: Vec<(String, String)> =
		args.var.iter().map(|s| parse_var_flag(s)).collect::<anyhow::Result<_>>()?;
	let template_vars = TemplateVars {
		vars: build_vars(&raw_vars, None)?,
	};

	let cfg = DbCfg::from_env(env.as_ref(), &overrides)?;
	let folder = cfg.folder().to_owned();
	let project = ProjectConfig::load(None)?;
	let selection =
		Selection::resolve(&project, &cfg, &args.schema, &args.target, args.all, args.no_deps)?;
	// Read before `args.command` is moved. Offline commands use it to refuse a
	// target selection rather than accept one and silently ignore it.
	let target_selection_used = !args.target.is_empty() || args.all;

	match args.command {
		Commands::Init {
			template,
			from,
			feature,
			minimal,
			yes,
			force,
		} => templates::run_init(
			&folder,
			InitOpts {
				template,
				from,
				feature,
				minimal,
				yes,
				force,
			},
		)?,
		Commands::Setup => {
			let db = connect(&cfg).await?;
			force_setup(&db, &folder).await?;
		}
		Commands::Sync {
			watch,
			debounce_ms,
			dry_run,
			fail_fast,
			no_prune,
			allow_shared_prune,
			allow_empty_prune,
			allow_all_statements,
		} => {
			let typegen_cfg = surrealkit::variables::load_typegen_config(None)?;
			let typegen_ts_out = typegen_cfg.typescript_path()?;
			let typegen_json_out = typegen_cfg.json_path()?;
			if selection.pairs() == 0 {
				bail!(
					"refusing filesystem sync: the selected targets accept none of the selected \
					 schema modules (applicable_pair_count=0)"
				);
			}
			if watch && selection.pairs() > 1 {
				bail!(
					"--watch needs a single schema module and target ({} selected); \
					 watching a whole matrix on a timer is rarely what you want",
					selection.pairs()
				);
			}

			// Resolve every selected module before opening the first target. A wrong
			// folder must not become a database connection, setup, catalog read, or
			// prune simply because an earlier target happened to be valid.
			let mut filesystem_sources = BTreeMap::new();
			for target in selection.targets() {
				for module in selection.modules_for(target) {
					if let std::collections::btree_map::Entry::Vacant(entry) =
						filesystem_sources.entry(module.name().to_string())
					{
						let layout = project.layout_for(&folder, &module);
						let schema_dir = layout.schema_dir();
						let files = sync::collect_filesystem_schema_files(
							layout.folder(),
							&schema_dir,
							&module,
							allow_empty_prune,
						)?;
						entry.insert((layout, files));
					}
				}
			}

			let mut results: Vec<PairResult> = Vec::new();
			'targets: for target in selection.targets() {
				let modules = selection.modules_for(target);
				if modules.is_empty() {
					continue;
				}
				let db = connect(target.cfg()).await?;
				for module in modules {
					if selection.is_fan_out() {
						log::info!("→ {} → {}", module.name(), target.name());
					}
					let (layout, files) =
						filesystem_sources.get(module.name()).with_context(|| {
							format!(
								"missing preflight sources for schema module {:?}",
								module.name()
							)
						})?;
					let opts = SyncOpts {
						watch,
						debounce_ms,
						dry_run,
						fail_fast,
						prune: !no_prune,
						allow_shared_prune,
						allow_empty_prune,
						allow_all_statements,
						vars: template_vars.clone(),
						folder: folder.clone(),
						module: module.clone(),
						typegen_ts_out: typegen_ts_out.clone(),
						typegen_ts_format: typegen_cfg.format.clone(),
						typegen_json_out: typegen_json_out.clone(),
					};
					let outcome =
						sync::run_sync_with_filesystem_sources(&db, opts, layout, files).await;
					let failed = outcome.is_err();
					if let Err(err) = &outcome {
						eprintln!("error: {} → {}: {err:#}", module.name(), target.name());
					}
					results.push(PairResult {
						module: module.name().to_string(),
						target: target.name().to_string(),
						error: outcome.err().map(|e| format!("{e:#}")),
					});
					// Modules within a target are ordered by dependency, so applying
					// the rest after one fails would build on a broken base.
					if failed {
						if args.keep_going {
							continue 'targets;
						}
						break 'targets;
					}
				}
			}

			if selection.is_fan_out() {
				report_pairs(&results);
			}
			let failed: Vec<&PairResult> = results.iter().filter(|r| r.error.is_some()).collect();
			match failed.as_slice() {
				[] => {}
				// Each failure has been said already, on stderr. Under JSON the
				// envelope says it again, as the one document a script reads.
				_ if !json => return Ok(1),
				[one] => bail!(
					"{} → {}: {}",
					one.module,
					one.target,
					one.error.as_deref().unwrap_or_default()
				),
				many => bail!("{} of {} schema/target pairs failed", many.len(), results.len()),
			}
		}
		Commands::Rollout {
			command,
		} => match command {
			RolloutCommands::Baseline => {
				let db = connect(selection.single_target()?.cfg()).await?;
				rollout::run_baseline(&db, &folder, selection.single_module()?).await?;
			}
			RolloutCommands::Plan {
				name,
				dry_run,
				allow_modified,
			} => {
				warn_unused_target_selection("rollout plan", target_selection_used);
				rollout::run_plan(
					&folder,
					RolloutPlanOpts {
						name,
						dry_run,
						allow_modified,
					},
				)
				.await?;
			}
			RolloutCommands::Start {
				rollout,
			} => {
				let target = selection.single_target()?;
				let db = connect(target.cfg()).await?;
				rollout::run_start(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(rollout),
						query_timeout: target.cfg().query_timeout,
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Complete {
				rollout,
			} => {
				let target = selection.single_target()?;
				let db = connect(target.cfg()).await?;
				rollout::run_complete(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(rollout),
						query_timeout: target.cfg().query_timeout,
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Rollback {
				rollout,
			} => {
				let target = selection.single_target()?;
				let db = connect(target.cfg()).await?;
				rollout::run_rollback(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(rollout),
						query_timeout: target.cfg().query_timeout,
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Status {
				rollout,
			} => {
				// Status makes no rollout changes, so it is the one rollout command
				// that may fan out across every selected target. It is not free of
				// writes: like every other command it runs `setup` first, which
				// scaffolds `<folder>/setup.surql` and applies the metadata DDL.
				let fan_out = selection.targets().len() > 1;
				for target in selection.targets() {
					if fan_out {
						log::info!("=== target {} ===", target.name());
					}
					let db = connect(target.cfg()).await?;
					rollout::run_status(&db, &folder, rollout.clone()).await?;
				}
			}
			RolloutCommands::Lint {
				rollout,
			} => {
				warn_unused_target_selection("rollout lint", target_selection_used);
				rollout::run_lint(&folder, RolloutExecutionOpts::new(rollout)).await?;
			}
			RolloutCommands::Up {
				complete,
				from,
				dry_run,
			} => {
				let target = selection.single_target()?;
				let db = connect(target.cfg()).await?;
				rollout::run_up(
					&db,
					&folder,
					RolloutUpOpts {
						complete_newest: complete,
						from,
						dry_run,
						query_timeout: target.cfg().query_timeout,
						quiet: false,
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Discard {
				rollout,
				keep_snapshots,
			} => {
				warn_unused_target_selection("rollout discard", target_selection_used);
				rollout::run_discard(&folder, &rollout, keep_snapshots)?;
			}
			RolloutCommands::Freeze {
				rollout,
			} => {
				warn_unused_target_selection("rollout freeze", target_selection_used);
				rollout::run_freeze(&folder, &rollout)?;
			}
			RolloutCommands::Repair {
				rollout,
			} => {
				let target = selection.single_target()?;
				let db = connect(target.cfg()).await?;
				rollout::run_repair(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(rollout),
						query_timeout: target.cfg().query_timeout,
					},
				)
				.await?;
			}
		},
		Commands::Seed {
			force,
		} => {
			let db = connect(&cfg).await?;
			surrealkit::Seed::from_dir(folder.clone())
				.vars(template_vars)
				.force(force)
				.run(&db)
				.await?;
		}
		Commands::Status => {
			let db = connect(&cfg).await?;
			rollout::run_status(&db, &folder, None).await?;
		}
		Commands::Apply {
			path,
		} => {
			let db = connect(&cfg).await?;
			let sql = std::fs::read_to_string(&path)?;
			let sql = template_vars.apply(&sql)?;
			exec_surql(&db, &sql).await?;
		}
		Commands::Test {
			suite,
			case,
			tag,
			fail_fast,
			parallel,
			json_out,
			no_setup,
			no_sync,
			no_seed,
			base_url,
			timeout_ms,
			keep_db,
			schema_from,
		} => {
			// Under --json the run's report is the document on stdout, the one
			// `--json-out` writes, rather than the summary for a person.
			let format = if json {
				ReportFormat::Json
			} else {
				ReportFormat::Human
			};
			run_test_to(
				env.as_ref(),
				TestOpts {
					suite,
					case,
					tags: tag,
					fail_fast,
					parallel,
					json_out,
					no_setup,
					no_sync,
					no_seed,
					base_url,
					timeout_ms,
					keep_db,
					schema_from,
				},
				template_vars,
				&overrides,
				format,
				&mut std::io::stdout(),
			)
			.await?;
		}
		Commands::Typegen {
			out,
			stdout,
			compact,
			typescript,
		} => {
			let mut typegen_cfg = surrealkit::variables::load_typegen_config(None)?;
			if let Some(typescript) = typescript {
				// A file on the command line wins outright; a directory still takes
				// the configured file name.
				if surrealkit::variables::is_typescript_file(&typescript) {
					typegen_cfg.filename = None;
				}
				typegen_cfg.typescript = Some(typescript);
			}
			let ts_out = typegen_cfg.typescript_path()?;
			let json_out = typegen_cfg.json_path()?;
			let db = connect(&cfg).await?;
			if json {
				// The schema document is this command's data, so under --json it
				// is what stdout carries, as with --stdout, and no file is written.
				let doc = surrealkit::generate(&db).await?;
				let rendered = if compact {
					serde_json::to_string(&doc)?
				} else {
					output::pretty(&doc)
				};
				println!("{rendered}");
				return Ok(0);
			}
			run_typegen(
				&db,
				&folder,
				TypegenOpts {
					out: out.or(json_out),
					stdout,
					pretty: !compact,
					ts_out,
					ts_format: typegen_cfg.format,
				},
			)
			.await?;
		}
	}

	Ok(0)
}

#[cfg(test)]
mod selection_tests {
	use surrealkit::config::DbOverrides;

	use super::*;

	fn base() -> DbCfg {
		DbCfg::from_env(None, &DbOverrides::default()).expect("base cfg")
	}

	fn project(raw: &str) -> ProjectConfig {
		ProjectConfig::parse(raw).expect("parse config")
	}

	fn resolve(
		raw: &str,
		schemas: &[&str],
		targets: &[&str],
		all: bool,
		no_deps: bool,
	) -> Selection {
		let schemas: Vec<String> = schemas.iter().map(|s| s.to_string()).collect();
		let targets: Vec<String> = targets.iter().map(|s| s.to_string()).collect();
		Selection::resolve(&project(raw), &base(), &schemas, &targets, all, no_deps)
			.expect("resolve selection")
	}

	#[test]
	fn no_config_and_no_flags_is_one_default_pair() {
		// The pre-1.0 case: exactly today's behaviour, and not fan-out formatted.
		let sel = resolve("", &[], &[], false, false);
		assert_eq!(sel.pairs(), 1);
		assert!(!sel.is_fan_out());
		assert!(sel.modules_for(&sel.targets()[0])[0].is_default());
		assert_eq!(sel.targets()[0].name(), "default");
	}

	#[test]
	fn declared_modules_are_all_selected_by_default() {
		let sel = resolve("[schema.core]\n[schema.billing]\n", &[], &[], false, false);
		assert_eq!(sel.pairs(), 2, "both modules against the ambient target");
	}

	#[test]
	fn selecting_a_module_pulls_in_its_dependencies_in_order() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n",
			&["billing"],
			&[],
			false,
			false,
		);
		let names: Vec<String> =
			sel.modules_for(&sel.targets()[0]).iter().map(|m| m.name().to_string()).collect();
		assert_eq!(names, vec!["core", "billing"], "dependency must be applied first");
	}

	#[test]
	fn no_deps_selects_only_what_was_asked_for() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n",
			&["billing"],
			&[],
			false,
			true,
		);
		assert_eq!(sel.modules_for(&sel.targets()[0]).len(), 1);
		assert_eq!(sel.modules_for(&sel.targets()[0])[0].name(), "billing");
	}

	#[test]
	fn all_expands_to_the_full_matrix() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\n[target.acme]\n[target.globex]\n",
			&[],
			&[],
			true,
			false,
		);
		assert_eq!(sel.pairs(), 4, "2 modules x 2 targets");
		assert!(sel.is_fan_out());
	}

	#[test]
	fn a_targets_schema_list_filters_the_matrix() {
		let sel = resolve(
			"[schema.core]\n[schema.billing]\n\
			 [target.acme]\n[target.warehouse]\nschemas = [\"core\"]\n",
			&[],
			&[],
			true,
			false,
		);
		// acme takes both; warehouse only core.
		assert_eq!(sel.pairs(), 3);
	}

	#[test]
	fn a_single_declared_target_is_used_without_being_marked_primary() {
		let sel = resolve("[target.only]\nns = \"x\"\n", &[], &[], false, false);
		assert_eq!(sel.targets().len(), 1);
		assert_eq!(sel.targets()[0].name(), "only");
		assert_eq!(sel.targets()[0].cfg().ns(), "x");
	}

	#[test]
	fn primary_is_chosen_when_several_targets_exist() {
		let sel = resolve("[target.a]\n[target.b]\nprimary = true\n", &[], &[], false, false);
		assert_eq!(sel.targets().len(), 1);
		assert_eq!(sel.targets()[0].name(), "b");
	}

	#[test]
	fn unknown_module_is_rejected_and_lists_the_declared_ones() {
		let err = Selection::resolve(
			&project("[schema.core]\n"),
			&base(),
			&["ghost".to_string()],
			&[],
			false,
			false,
		)
		.unwrap_err()
		.to_string();
		assert!(err.contains("ghost"), "got: {err}");
		assert!(err.contains("core"), "should list declared modules: {err}");
	}

	#[test]
	fn unknown_target_is_rejected_and_lists_the_declared_ones() {
		let err = Selection::resolve(
			&project("[target.acme]\n"),
			&base(),
			&[],
			&["ghost".to_string()],
			false,
			false,
		)
		.unwrap_err()
		.to_string();
		assert!(err.contains("ghost") && err.contains("acme"), "got: {err}");
	}

	#[test]
	fn single_module_errors_when_several_are_selected() {
		let sel = resolve("[schema.a]\n[schema.b]\n", &[], &[], false, false);
		assert!(sel.single_module().is_err(), "commands that cannot fan out must refuse");
	}

	#[test]
	fn single_target_errors_when_several_are_selected() {
		let sel = resolve("[target.a]\n[target.b]\n", &[], &[], true, false);
		assert!(sel.single_target().is_err(), "rollout commands must not fan out across targets");
	}

	/// A subcommand arg whose clap id matches a global arg's id is silently
	/// swallowed: clap skips propagating the global into that subcommand, and the
	/// subcommand's value is hoisted back into the parent under the shared id.
	///
	/// That is how 1.0.0-beta.1 shipped with every rollout id being parsed as a
	/// `--target` name, which made `rollout start/complete/rollback/status/lint/
	/// repair` fail with `unknown target "<rollout-id>"` before ever connecting.
	///
	/// `Command::debug_assert()` does not catch this — its duplicate-id check is
	/// scoped within a single `Command`, and cross-level shadowing is exactly the
	/// case it skips. So walk the tree explicitly.
	#[test]
	fn no_subcommand_shadows_a_global_arg_id() {
		use clap::CommandFactory;

		let root = Cli::command();
		let globals: Vec<String> = root
			.get_arguments()
			.filter(|a| a.is_global_set())
			.map(|a| a.get_id().to_string())
			.collect();
		assert!(!globals.is_empty(), "expected at least one global arg to guard");

		fn walk(cmd: &clap::Command, globals: &[String], path: &str, bad: &mut Vec<String>) {
			for sub in cmd.get_subcommands() {
				let here = format!("{path} {}", sub.get_name());
				for arg in sub.get_arguments() {
					let id = arg.get_id().to_string();
					if globals.contains(&id) {
						bad.push(format!("`{}` redefines global arg id `{id}`", here.trim()));
					}
				}
				walk(sub, globals, &here, bad);
			}
		}

		let mut bad = Vec::new();
		walk(&root, &globals, "", &mut bad);
		assert!(bad.is_empty(), "argument id shadowing:\n  {}", bad.join("\n  "));

		// Cheap extra: catches same-level duplicates, which this walk does not.
		Cli::command().debug_assert();
	}
}
