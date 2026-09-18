// The binary is the one place that may write to the console directly: it owns the
// CLI's output format. Library modules go through `log` (see CliLogger below).
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
#[cfg(feature = "analyze")]
use std::io::IsTerminal;
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "analyze")]
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{DbCfg, DbOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::progress::CaptureLogger;
use surrealkit::project::ProjectConfig;
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts};
use surrealkit::selection::Selection;
use surrealkit::setup::run_setup;
use surrealkit::sync::{self, SyncOpts};
use surrealkit::templates::{self, InitOpts};
use surrealkit::tester::{TestOpts, run_test};
use surrealkit::typegen::{TypegenOpts, run_typegen};
use surrealkit::variables::{TemplateVars, build_vars, parse_var_flag};

#[derive(Parser, Debug)]
#[command(version, about = "SurrealKit CLI")]
pub struct Cli {
	/// Increase output
	#[arg(short, long, global = true)]
	verbose: bool,

	/// Schema module to operate on. Modules are tracked independently: a module
	/// only ever prunes its own database objects. Files live in
	/// `<folder>/modules/<name>/schema`. Omit for the default module
	/// (`<folder>/schema`), which is the pre-1.0 layout.
	#[arg(short = 's', long, global = true, value_name = "NAME")]
	schema: Vec<String>,

	/// Database target to operate on, from `[target.<name>]` in surrealkit.toml.
	/// Repeatable. Omit for the ambient connection (--host/--ns/--db and env).
	#[arg(short = 't', long, global = true, value_name = "NAME")]
	target: Vec<String>,

	/// Every declared schema module against every declared database target.
	#[arg(long, global = true)]
	all: bool,

	/// Continue to the next target after one fails, instead of stopping.
	#[arg(long, global = true)]
	keep_going: bool,

	/// Don't pull in the `depends_on` modules of the ones selected with --schema.
	#[arg(long, global = true)]
	no_deps: bool,

	/// Database host URL
	#[arg(long, global = true)]
	host: Option<String>,

	/// Database name
	#[arg(long, global = true)]
	db: Option<String>,

	/// Database namespace
	#[arg(long, global = true)]
	ns: Option<String>,

	/// Database user
	#[arg(long, global = true)]
	user: Option<String>,

	/// Database password
	#[arg(long, global = true)]
	pass: Option<String>,

	/// Authentication level: root (default), namespace/ns, or database/db
	#[arg(long, global = true)]
	auth_level: Option<String>,

	/// Root folder for the database directory (default: `./database`).
	#[arg(long, global = true)]
	folder: Option<String>,

	/// Seconds to wait for the database connection and sign-in (default: 30).
	/// `0` waits indefinitely, which was the behaviour through 1.0.0-beta.1.
	#[arg(long, global = true, value_name = "SECS")]
	connect_timeout_secs: Option<u64>,

	/// Seconds to wait for a single rollout step's SQL. Unset waits
	/// indefinitely, because a legitimate index build can take hours.
	#[arg(long, global = true, value_name = "SECS")]
	query_timeout_secs: Option<u64>,

	/// Set a template variable (repeatable): --var KEY=VALUE
	#[arg(long = "var", global = true, value_name = "KEY=VALUE")]
	var: Vec<String>,

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
	Setup,
	Sync {
		#[arg(long)]
		watch: bool,
		#[arg(long, default_value_t = 1000)]
		debounce_ms: u64,
		#[arg(long)]
		dry_run: bool,
		#[arg(long, default_value_t = true)]
		fail_fast: bool,
		#[arg(long)]
		no_prune: bool,
		#[arg(long)]
		allow_shared_prune: bool,
		/// Allow filesystem sync to connect with no schema files. Required before
		/// an intentional prune of every managed entity.
		#[arg(long)]
		allow_empty_prune: bool,
		/// Allow non-DEFINE statements in schema files (e.g. INSERT, UPDATE, CREATE).
		/// Disables catalog entity tracking; only file-level hashes are tracked.
		#[arg(long)]
		allow_all_statements: bool,
	},
	Rollout {
		#[command(subcommand)]
		command: RolloutCommands,
	},
	/// Run seed files. Each file runs only on first boot or when its content
	/// changes (tracked in the `__seed` table). Use `--force` to re-run all.
	Seed {
		/// Re-run every seed file, ignoring the `__seed` tracking table.
		#[arg(long)]
		force: bool,
	},
	Status,
	Apply {
		path: PathBuf,
	},
	Test {
		#[arg(long)]
		suite: Option<String>,
		#[arg(long)]
		case: Option<String>,
		#[arg(long)]
		tag: Vec<String>,
		#[arg(long)]
		fail_fast: bool,
		#[arg(long, default_value_t = 1)]
		parallel: usize,
		#[arg(long)]
		json_out: Option<PathBuf>,
		#[arg(long)]
		no_setup: bool,
		#[arg(long)]
		no_sync: bool,
		#[arg(long)]
		no_seed: bool,
		#[arg(long)]
		base_url: Option<String>,
		#[arg(long)]
		timeout_ms: Option<u64>,
		#[arg(long)]
		keep_db: bool,
	},
	// `check`, `generate` and `watch`: the commands that read only the
	// filesystem. Flattened, so they are spelled `surrealkit check` like every
	// other subcommand, while staying one value `main` can dispatch before it
	// assembles a database connection it will never open. A doc comment here
	// would become the whole CLI's `about` text, so this stays a plain comment
	// and the help lives on the variants of `AnalyzeCommands`.
	//
	// Without the `analyze` feature the variant is not built, so the three
	// subcommands do not exist and clap does not list them.
	#[cfg(feature = "analyze")]
	#[command(flatten)]
	Analyze(AnalyzeCommands),
	/// Serve SurrealKit's capabilities to an AI agent over the Model Context
	/// Protocol, on stdio. The MCP host launches this; you rarely run it by hand.
	#[cfg(feature = "mcp")]
	Mcp,
	/// Introspect the database and generate a typed schema document (JSON).
	Typegen {
		/// Output path (default: `{folder}/types/schema.json`).
		#[arg(long)]
		out: Option<PathBuf>,
		/// Print the JSON to stdout instead of writing a file.
		#[arg(long)]
		stdout: bool,
		/// Emit compact (single-line) JSON instead of pretty-printed.
		#[arg(long)]
		compact: bool,
	},
}

// The commands that contact no database.
//
// They are handled before the connection config is assembled, so a project
// whose `[target.*]` needs a secret -- or a machine carrying a stale
// `DATABASE_*` variable -- can still be analyzed. Keeping them in their own
// enum is what lets `main` dispatch them as a single value.
//
// A doc comment on this enum or on the variant that flattens it becomes the
// whole CLI's `about` text, so both stay plain comments.
#[cfg(feature = "analyze")]
#[derive(Subcommand, Debug)]
enum AnalyzeCommands {
	/// Statically check the project's SurrealQL — schema files, query files and
	/// the queries embedded in host code — against the schema. No database is
	/// contacted. Exits non-zero when any finding survives as an error.
	///
	/// Always covers the whole project: `-s/--schema` picks which directories
	/// are read as schema (and so are analyzed first), but every `.surql` and
	/// host file under the project root is read either way, and findings are
	/// reported wherever they land.
	Check {
		/// Emit one machine-readable JSON document (`{ summary, diagnostics[] }`)
		/// instead of rustc-style text.
		#[arg(long, conflicts_with = "watch")]
		json: bool,
		/// Re-check on every change to a `.surql` file, a host file, or
		/// `surrealkit.toml`. Runs once first, then blocks until interrupted.
		#[arg(long)]
		watch: bool,
	},
	/// Generate the typed TypeScript client for the queries embedded in host
	/// code (`db.query("SELECT …")`), from the schema alone. Refuses to
	/// overwrite a good registry when an embedded query has an error.
	Generate {
		/// Output path, relative to the working directory (default: `[analyze]
		/// out` in surrealkit.toml, which is relative to the project root;
		/// else `surrealql-analyzer.generated.ts` at the project root).
		#[arg(long)]
		out: Option<PathBuf>,
		/// Regenerate on every change. Runs once first, then blocks until
		/// interrupted. `[analyze] out` is read once, when the watch starts.
		#[arg(long)]
		watch: bool,
	},
	/// Check, then regenerate the typed client, on every change — the loop to
	/// run beside a dev server. A run that fails the check leaves the
	/// registry untouched. `surrealkit.toml` is re-read before every run, so
	/// an edit re-targets the analysis; `[analyze] out` is the exception, read
	/// once when the watch starts, because the path kept out of the watched
	/// set has to stay fixed for the life of the process.
	Watch {
		/// Output path for the generated module (see `generate --out`).
		#[arg(long)]
		out: Option<PathBuf>,
		/// Only check; write nothing. Rejects `--out`, which would name a file
		/// this run is never going to write.
		#[arg(long, conflicts_with = "out")]
		check_only: bool,
	},
}

#[cfg(feature = "analyze")]
impl AnalyzeCommands {
	/// The name the command was spelled with, for messages about flags that do
	/// not apply to it.
	fn name(&self) -> &'static str {
		match self {
			Self::Check {
				..
			} => "check",
			Self::Generate {
				..
			} => "generate",
			Self::Watch {
				..
			} => "watch",
		}
	}
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
	Baseline,
	Plan {
		#[arg(long)]
		name: Option<String>,
		#[arg(long)]
		dry_run: bool,
		/// Plan changes to entities that already exist (e.g. tightening an
		/// ASSERT), not just additions and removals. Rollback restores the
		/// previous definition, which reverses the schema but not any data
		/// effect the change had.
		#[arg(long)]
		allow_modified: bool,
	},
	Start {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	Complete {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	Rollback {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	Status {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: Option<String>,
	},
	Lint {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	/// Heal a rollout stuck in an intermediate state without re-running SQL
	/// steps. Useful when `complete` was killed mid-flight (issue #55) and
	/// `__rollout.status` is still `running_complete` / `running_rollback`.
	Repair {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
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

/// Print the per-pair summary shown after a fan-out run.
fn report_pairs(results: &[PairResult]) {
	let mw = results.iter().map(|r| r.module.len()).max().unwrap_or(6).max("schema".len());
	let tw = results.iter().map(|r| r.target.len()).max().unwrap_or(6).max("target".len());
	println!();
	println!("  {:<mw$}  {:<tw$}  status", "schema", "target", mw = mw, tw = tw);
	println!("  {}  {}  ------", "-".repeat(mw), "-".repeat(tw));
	for r in results {
		let status = if r.error.is_some() {
			"FAILED"
		} else {
			"ok"
		};
		println!("  {:<mw$}  {:<tw$}  {status}", r.module, r.target, mw = mw, tw = tw);
	}
	let failed = results.iter().filter(|r| r.error.is_some()).count();
	println!();
	if failed == 0 {
		println!("{} ok", results.len());
	} else {
		println!("{} ok, {failed} failed", results.len() - failed);
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
/// `env_logger` is deliberately not used: it writes everything to stderr with a
/// level prefix, which would be a visible CLI change.
struct CliLogger {
	level: log::LevelFilter,
}

impl log::Log for CliLogger {
	fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
		// Only SurrealKit's own output; dependencies stay quiet unless -v is given.
		metadata.level() <= self.level && metadata.target().starts_with("surrealkit")
	}

	fn log(&self, record: &log::Record<'_>) {
		if !self.enabled(record.metadata()) {
			return;
		}
		match record.level() {
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

/// Serve MCP over stdio.
///
/// Installs a logger that writes to **stderr**, never stdout: on this transport
/// stdout carries JSON-RPC framing and nothing else. Progress the CLI would have
/// printed is captured per tool call and returned in the result instead.
#[cfg(feature = "mcp")]
async fn run_mcp(args: &Cli) -> Result<()> {
	use surrealkit::mcp::{ServerConfig, SurrealKitMcp, serve_stdio};
	use surrealkit::progress::StderrLogger;

	let level = if args.verbose {
		log::LevelFilter::Debug
	} else {
		log::LevelFilter::Info
	};
	let _ = CaptureLogger::install(Box::new(StderrLogger), level, args.verbose);

	let overrides = DbOverrides {
		host: args.host.clone(),
		ns: args.ns.clone(),
		db: args.db.clone(),
		user: args.user.clone(),
		pass: args.pass.clone(),
		auth_level: args.auth_level.clone(),
		folder: args.folder.clone(),
		connect_timeout_secs: args.connect_timeout_secs,
		query_timeout_secs: args.query_timeout_secs,
	};
	let vars: Vec<(String, String)> =
		args.var.iter().map(|s| parse_var_flag(s)).collect::<Result<_>>()?;

	// The project root is captured once, here. Nothing afterwards reads or changes
	// the process working directory: `set_current_dir` is process-global, so under
	// concurrent tool calls it would be a data race.
	let root = std::env::current_dir().context("resolving the project root")?;
	let config = ServerConfig::new(root, overrides, vars)?;
	log::info!("surrealkit mcp: serving {} over stdio", config.root.display());

	serve_stdio(SurrealKitMcp::new(config)).await
}

/// Install the process logger. `-v/--verbose` raises the level to `debug`.
///
/// `CliLogger` is not installed directly: it becomes the *fallback* of
/// [`CaptureLogger`], which routes to a task-local buffer when one is active. No
/// CLI code path ever activates one, so console output is unchanged, but it means
/// the MCP server can collect the same progress as data without a second,
/// impossible, call to `log::set_logger`.
fn init_logging(verbose: bool) {
	let level = if verbose {
		log::LevelFilter::Debug
	} else {
		log::LevelFilter::Info
	};
	// Only fails if a logger is already installed, which cannot happen here.
	let _ = CaptureLogger::install(
		Box::new(CliLogger {
			level,
		}),
		level,
		verbose,
	);
}

/// Load `.env` / `.env.local` from the current working directory when present.
fn load_env() -> Option<DotEnv> {
	load_env_from(Path::new("."))
}

/// Load `.env` / `.env.local`, deciding on the one in `dir`.
///
/// The loader already walks up from the working directory to find the file; it
/// is only this existence check that decides whether to consult it at all.
/// Anchoring the check at the project root is what makes the analyze commands
/// read the same `.env` — and so resolve `SURREALDB_FOLDER` to the same
/// directory — from a subdirectory as from the root. The database commands
/// keep the working directory: that is where their `.env` has always been
/// looked for, and which database to connect to is not a project-wide fact the
/// way the folder to analyze is.
///
/// The two halves of the check are deliberately asymmetric, because the loader
/// is: it searches upwards for `.env`, but decides whether to prefer
/// `.env.local` by testing that name against the *working directory* only. So
/// `.env` is looked for where the caller asks and `.env.local` where the
/// loader will actually look for it. Testing both in `dir` would return `Some`
/// for a root-level `.env.local` that the loader then fails to find, and
/// rust_dotenv prints `Error: .env file not found` to stderr on an otherwise
/// clean, exit-0 run.
fn load_env_from(dir: &Path) -> Option<DotEnv> {
	let has_env = dir.join(".env").exists() || Path::new(".env.local").exists();
	if has_env {
		Some(DotEnv::new(""))
	} else {
		None
	}
}

#[tokio::main]
// anyhow::Result rather than Box<dyn Error>: output is identical (Rust prints a
// failed main's Err via Debug, and Box<dyn Error> already delegated to anyhow's),
// but it lets this function use bail!/context directly.
async fn main() -> Result<()> {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let args = Cli::parse();

	// The MCP server is dispatched before anything else, and deliberately so.
	// `init_logging` installs a logger that writes `log::info!` to **stdout**,
	// which on the stdio transport is the JSON-RPC channel: every progress line
	// would corrupt the protocol. The eager `Selection::resolve` below is just as
	// wrong for a server, since an unset `pass_env` on some unrelated target would
	// kill it at startup instead of being reported through a tool result.
	#[cfg(feature = "mcp")]
	if matches!(args.command, Commands::Mcp) {
		return run_mcp(&args).await;
	}

	init_logging(args.verbose);
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

	// Static analysis reads the filesystem and nothing else, so it runs before
	// the connection config exists. Assembling that config can fail for
	// reasons a check has no stake in -- a `[target.*]` whose `pass_env`
	// secret is not exported on this machine, a leftover `DATABASE_*`
	// variable -- and a CI job that only runs `surrealkit check --json` must
	// not be stopped by a password it never uses.
	#[cfg(feature = "analyze")]
	let command = match args.command {
		Commands::Analyze(command) => {
			warn_unused_target_selection(command.name(), !args.target.is_empty() || args.all);
			// The analysis is rooted at the directory holding surrealkit.toml,
			// so the `.env` that names the folder to analyze is the one beside
			// it -- not whichever directory the command happened to be run in.
			let env = surrealkit::analyze::project_root()
				.ok()
				.map_or_else(|| env.clone(), |root| load_env_from(&root));
			let folder = DbCfg::resolve_folder(env.as_ref(), &overrides);
			let project = ProjectConfig::load(None)?;
			run_analyze(command, &project, &folder, &args.schema, args.no_deps)?;
			let _ = std::io::stdout().flush();
			let _ = std::io::stderr().flush();
			std::process::exit(0);
		}
		other => other,
	};
	// Without the analyze feature there is no such command to dispatch early.
	#[cfg(not(feature = "analyze"))]
	let command = args.command;

	let cfg = DbCfg::from_env(env.as_ref(), &overrides)?;
	let folder = cfg.folder().to_owned();
	let project = ProjectConfig::load(None)?;
	let selection =
		Selection::resolve(&project, &cfg, &args.schema, &args.target, args.all, args.no_deps)?;
	// Read before `args.command` is moved. Offline commands use it to refuse a
	// target selection rather than accept one and silently ignore it.
	let target_selection_used = !args.target.is_empty() || args.all;

	match command {
		// Dispatched above, before the connection config was built. The arm
		// exists because `Commands` is one enum for clap's sake, not because
		// the value can reach here.
		#[cfg(feature = "analyze")]
		Commands::Analyze(_) => {}
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
			run_setup(&db, &folder).await?;
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
						println!("→ {} → {}", module.name(), target.name());
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
						typegen_ts_out: typegen_cfg.typescript.clone(),
						typegen_ts_format: typegen_cfg.format.clone(),
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
			if results.iter().any(|r| r.error.is_some()) {
				std::process::exit(1);
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
				rollout::run_lint(&folder, RolloutExecutionOpts::new(Some(rollout))).await?;
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
		} => {
			run_test(
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
				},
				template_vars,
				&overrides,
			)
			.await?;
		}
		#[cfg(feature = "mcp")]
		Commands::Mcp => unreachable!("dispatched before logging is installed"),
		Commands::Typegen {
			out,
			stdout,
			compact,
		} => {
			let db = connect(&cfg).await?;
			let typegen_cfg = surrealkit::variables::load_typegen_config(None)?;
			run_typegen(
				&db,
				&folder,
				cfg.ns(),
				cfg.db(),
				TypegenOpts {
					out,
					stdout,
					pretty: !compact,
					ts_out: typegen_cfg.typescript,
					ts_format: typegen_cfg.format,
				},
			)
			.await?;
		}
	}

	// Belt-and-braces (issue #55): bypass tokio runtime shutdown so the HTTP
	// client's background connection-pool tasks can't keep the process alive
	// after a successful command. Errors bubble up via `?` above and still
	// produce a non-zero exit code through the normal `Result` path.
	use std::io::Write;
	let _ = std::io::stdout().flush();
	let _ = std::io::stderr().flush();
	std::process::exit(0);
}

/// Colour on when stdout is a terminal that has not opted out (`NO_COLOR`).
#[cfg(feature = "analyze")]
fn styles() -> surrealql_analyzer::Styles {
	let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
	surrealql_analyzer::Styles::new(color)
}

/// `1 error` / `3 errors`, with the plural spelled out.
///
/// Taking the plural rather than appending an `s` because English does not:
/// `generate` counts queries, and `0 querys` is the kind of detail that makes
/// a tool look unfinished.
#[cfg(feature = "analyze")]
fn count(n: usize, singular: &str, plural: &str) -> String {
	if n == 1 {
		format!("{n} {singular}")
	} else {
		format!("{n} {plural}")
	}
}

/// Dispatch for the commands that contact no database.
///
/// `selected` is the global `--schema`, validated here rather than by
/// [`Selection::resolve`]: these commands never reach it, and an analysis of
/// the wrong modules is worth an error rather than a silent whole-project run.
#[cfg(feature = "analyze")]
fn run_analyze(
	command: AnalyzeCommands,
	project: &ProjectConfig,
	folder: &str,
	selected: &[String],
	no_deps: bool,
) -> Result<()> {
	// Built once and reused: discovery, the module resolution and the glob
	// construction are the same work for every phase of one invocation, and a
	// second build could disagree with the first if the config changed under it.
	let analyzer_project =
		surrealkit::analyze::analyzer_project(project, folder, selected, no_deps)?;
	let modules = surrealkit::analyze::selected_modules(project, selected, no_deps)?;
	let schema_dirs = surrealkit::analyze::schema_dirs(project, folder, &modules);
	// `--out` is typed at a shell, so it is relative to the working directory;
	// `[analyze] out` is written in the config file, so it is relative to the
	// project root and names the same file from every directory.
	let configured_out = || surrealkit::analyze::configured_out(project);
	let target = AnalysisTarget {
		folder,
		selected,
		no_deps,
	};
	match command {
		AnalyzeCommands::Check {
			json,
			watch,
		} => {
			if watch {
				run_watch(analyzer_project, target, &schema_dirs, None, true)?;
			} else if !run_check(&analyzer_project, json, &schema_dirs)? {
				bail!("check failed");
			}
		}
		AnalyzeCommands::Generate {
			out,
			watch,
		} => {
			let out = match out {
				Some(out) => Some(out),
				None => configured_out()?,
			};
			if watch {
				run_watch_generate_only(analyzer_project, target, out)?;
			} else {
				run_generate(&analyzer_project, out.as_deref())?;
			}
		}
		AnalyzeCommands::Watch {
			out,
			check_only,
		} => {
			let out = match out {
				Some(out) => Some(out),
				None => configured_out()?,
			};
			run_watch(analyzer_project, target, &schema_dirs, out, check_only)?;
		}
	}
	Ok(())
}

/// One `check` over the project. Prints every surviving finding and the
/// summary; returns whether the run is clean. `json` prints the analyzer's
/// stable document instead of text.
///
/// Findings and the summary go to **stderr**, as rustc's do: `check > out.txt`
/// is a reasonable thing to type, and it must not be what hides the errors.
/// `--json` is the exception — it is the machine-readable product of the run,
/// so it goes to stdout, alone.
#[cfg(feature = "analyze")]
fn run_check(
	analyzer_project: &surrealql_analyzer::Project,
	json: bool,
	schema_dirs: &[String],
) -> Result<bool> {
	let started = Instant::now();
	let report = surrealql_analyzer::check(analyzer_project)?;
	warn_if_no_sources(analyzer_project, report.summary.sources_checked, schema_dirs);
	if json {
		println!("{}", report.to_json()?);
		return Ok(report.passed());
	}
	for block in report.render(styles()) {
		eprintln!("{block}");
	}
	let summary = &report.summary;
	let warnings = summary.diagnostics.saturating_sub(summary.errors);
	eprintln!(
		"checked {} in {}ms: {}, {}",
		count(summary.sources_checked, "source", "sources"),
		started.elapsed().as_millis(),
		count(summary.errors, "error", "errors"),
		count(warnings, "warning", "warnings"),
	);
	Ok(report.passed())
}

/// A run that read nothing is almost always a misconfiguration — the wrong
/// `--folder`, or an `[analyze] ignore` broader than it looks — and it exits
/// 0, which in CI reads as "checked, all clear". Say so; do not fail, because
/// a project that genuinely has no SurrealQL yet is a legitimate state.
#[cfg(feature = "analyze")]
fn warn_if_no_sources(
	analyzer_project: &surrealql_analyzer::Project,
	sources_checked: usize,
	schema_dirs: &[String],
) {
	if sources_checked > 0 {
		return;
	}
	eprintln!(
		"warning: no SurrealQL sources found under {} (schema: {}); \
		 check --folder and [analyze] ignore",
		analyzer_project.root().display(),
		if schema_dirs.is_empty() {
			"none".to_string()
		} else {
			schema_dirs.join(", ")
		}
	);
}

/// One `generate` over the project. A blocked run prints the embedded
/// queries' errors and fails; a clean run prints where the module went and
/// any warnings.
#[cfg(feature = "analyze")]
fn run_generate(analyzer_project: &surrealql_analyzer::Project, out: Option<&Path>) -> Result<()> {
	use surrealql_analyzer::GenerateError;
	match surrealql_analyzer::generate(analyzer_project, out) {
		Ok(report) => {
			for block in report.render_warnings(styles()) {
				eprintln!("{block}");
			}
			if let Some(block) = report.render_missing_client(styles()) {
				eprintln!("{block}");
			}
			// The one line that reports a *product* rather than a finding, so
			// the one line that belongs on stdout.
			println!(
				"wrote {} ({})",
				analyzer_project.display_relative(&report.path),
				count(report.queries, "query", "queries")
			);
			Ok(())
		}
		Err(GenerateError::Blocked(blocked)) => {
			for block in blocked.render(styles()) {
				eprintln!("{block}");
			}
			bail!("{blocked}");
		}
		Err(GenerateError::Io(error)) => Err(error.into()),
	}
}

/// What a watch needs in order to rebuild its view of the project after every
/// change. The config is re-read each time; these are fixed for the process,
/// because they come from the command line.
#[derive(Clone, Copy)]
#[cfg(feature = "analyze")]
struct AnalysisTarget<'a> {
	folder: &'a str,
	selected: &'a [String],
	no_deps: bool,
}

/// `generate --watch`: regenerate on every change, without the full check.
#[cfg(feature = "analyze")]
fn run_watch_generate_only(
	analyzer_project: surrealql_analyzer::Project,
	target: AnalysisTarget<'_>,
	out: Option<PathBuf>,
) -> Result<()> {
	let exclude = out_for_exclusion(&analyzer_project, out.as_deref())?;
	watch(
		analyzer_project,
		target,
		move |analyzer_project| {
			if let Err(error) = run_generate(analyzer_project, out.as_deref()) {
				eprintln!("{error:#}");
			}
		},
		exclude,
	)
}

/// `watch` / `check --watch`: check on every change, and regenerate when the
/// check passes. `generate`'s own findings are the embedded-query subset of
/// what `check` just printed, so they are not printed twice.
#[cfg(feature = "analyze")]
fn run_watch(
	analyzer_project: surrealql_analyzer::Project,
	target: AnalysisTarget<'_>,
	schema_dirs: &[String],
	out: Option<PathBuf>,
	check_only: bool,
) -> Result<()> {
	let exclude = if check_only {
		None
	} else {
		out_for_exclusion(&analyzer_project, out.as_deref())?
	};
	let schema_dirs = schema_dirs.to_vec();
	watch(
		analyzer_project,
		target,
		move |analyzer_project| match run_check(analyzer_project, false, &schema_dirs) {
			Ok(true) if !check_only => {
				if let Err(error) = run_generate(analyzer_project, out.as_deref()) {
					eprintln!("{error:#}");
				}
			}
			Ok(_) => {}
			Err(error) => eprintln!("{error:#}"),
		},
		exclude,
	)
}

/// The registry path a watch must not treat as an input, or it would
/// re-trigger itself on every write.
///
/// The directory is created here, before the first run: the watcher resolves
/// this path through the filesystem to get the spelling it will report, and a
/// directory that does not exist yet cannot be resolved. `generate` would
/// create it a moment later anyway — doing it first is what makes the
/// exclusion and the write agree on the first run rather than the second.
#[cfg(feature = "analyze")]
fn out_for_exclusion(
	analyzer_project: &surrealql_analyzer::Project,
	out: Option<&Path>,
) -> Result<Option<PathBuf>> {
	let registry = analyzer_project.registry_path(out);
	if let Some(parent) = registry.parent().filter(|parent| !parent.as_os_str().is_empty()) {
		std::fs::create_dir_all(parent)
			.with_context(|| format!("creating the directory for {}", registry.display()))?;
	}
	Ok(Some(registry))
}

/// The watch loop: re-read `surrealkit.toml` before every run (an edit
/// re-targets the analysis on the next save; a half-typed file keeps the last
/// good configuration), and hand each run to `run` with a header line.
#[cfg(feature = "analyze")]
fn watch(
	initial: surrealql_analyzer::Project,
	target: AnalysisTarget<'_>,
	mut run: impl FnMut(&surrealql_analyzer::Project),
	exclude: Option<PathBuf>,
) -> Result<()> {
	let load = || -> Result<surrealql_analyzer::Project> {
		let project = ProjectConfig::load(None)?;
		surrealkit::analyze::analyzer_project(
			&project,
			target.folder,
			target.selected,
			target.no_deps,
		)
	};
	// The loader and the runner are two closures the loop calls in turn, and
	// both need the current project: the loader replaces it, the runner reads
	// it. A `RefCell` lets each hold a shared borrow of the cell.
	let current = std::cell::RefCell::new(initial);
	let extra_inputs: Vec<PathBuf> = surrealkit::analyze::config_path()?.into_iter().collect();

	println!("watching {} — Ctrl-C to stop", current.borrow().root().display());
	surrealql_analyzer::watch_loop(
		|| {
			match load() {
				Ok(project) => *current.borrow_mut() = project,
				// `{error:#}` -- the outer context alone is "parsing
				// surrealkit.toml", which says that the reload failed without
				// saying why, in the one mode where nothing else will.
				Err(error) => eprintln!("{CONFIG_RELOAD_FAILED}: {error:#}"),
			}
			current.borrow().clone()
		},
		&extra_inputs,
		exclude.as_deref(),
		|run_info| {
			println!();
			println!("run {} · {}", run_info.index, run_info.reason);
			run(&current.borrow());
			let _ = std::io::stdout().flush();
			let _ = std::io::stderr().flush();
		},
	)
	.map_err(|error| anyhow::anyhow!("watch: {error}"))
}

#[cfg(feature = "analyze")]
const CONFIG_RELOAD_FAILED: &str =
	"surrealkit.toml did not reload; keeping the previous configuration";

#[cfg(all(test, feature = "analyze"))]
mod output_tests {
	use super::*;

	#[test]
	fn counts_use_the_plural_they_are_given() {
		// `generate` counts queries, and appending an `s` produced `0 querys`.
		assert_eq!(count(0, "query", "queries"), "0 queries");
		assert_eq!(count(1, "query", "queries"), "1 query");
		assert_eq!(count(2, "query", "queries"), "2 queries");
		assert_eq!(count(0, "error", "errors"), "0 errors");
		assert_eq!(count(1, "source", "sources"), "1 source");
	}

	#[test]
	fn check_only_rejects_an_output_path_it_would_never_write() {
		use clap::Parser;
		let error = Cli::try_parse_from(["surrealkit", "watch", "--check-only", "--out", "x.ts"])
			.expect_err("--check-only writes nothing, so --out cannot mean anything");
		assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
		// Each alone is still fine.
		Cli::try_parse_from(["surrealkit", "watch", "--check-only"]).expect("check-only alone");
		Cli::try_parse_from(["surrealkit", "watch", "--out", "x.ts"]).expect("out alone");
	}
}

#[cfg(test)]
mod cli_tests {
	use super::*;

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
