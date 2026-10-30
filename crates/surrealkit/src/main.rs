// The binary is the one place that may write to the console directly: it owns the
// CLI's output format. Library modules go through `log` (see CliLogger below).
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{DbCfg, DbOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::module::Module;
use surrealkit::project::{ProjectConfig, Target};
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts, RolloutUpOpts};
use surrealkit::setup::force_setup;
use surrealkit::sync::{self, SyncOpts};
use surrealkit::tester::{TestOpts, run_test};
use surrealkit::typegen::{TypegenOpts, run_typegen};
use surrealkit::variables::{TemplateVars, build_project_vars, build_vars, parse_var_flag};

use crate::templates::InitOpts;

// `init` templates are a CLI-only concern, so the module lives in the binary
// rather than the public library surface.
mod templates;

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
		/// How each suite's database gets its schema: `sync` from the schema
		/// folder (the default), `rollouts` replayed in order from empty, or
		/// `both`, running every suite once on each.
		#[arg(long, value_enum, value_name = "SOURCE")]
		schema_from: Option<surrealkit::tester::SchemaSource>,
		/// Run a command against each suite's prepared database, after its cases and before
		/// the database is removed. The command is split on whitespace, with no quoting, so
		/// no argument can contain a space (wrap such a command in a script). It gets the
		/// endpoint, namespace, database, a token scoped to that database, the actor tokens
		/// and the schema digest as `SURREALKIT_EXEC_*` variables; its exit status is a case.
		/// Without suite files, one implicit suite `exec` runs.
		#[arg(long, value_name = "CMD")]
		exec: Option<String>,
		/// Also pass the root actor's token to `--exec` as `SURREALKIT_EXEC_TOKEN_ROOT`. It is
		/// valid for the whole server, not just the suite database.
		#[arg(long, requires = "exec")]
		exec_root_token: bool,
	},
	/// Introspect the database and generate a typed schema document (JSON).
	///
	/// Reads one database: `--target`, else the `primary` target, else the only
	/// declared target, else the ambient connection.
	Typegen {
		/// Output path (default: `[typegen] json`, else
		/// `{folder}/types/schema.json`).
		#[arg(long)]
		out: Option<PathBuf>,
		/// Print the JSON to stdout instead of writing the JSON file. Configured
		/// TypeScript output is still written.
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
		/// Write nothing; exit non-zero when any generated file is missing or
		/// differs from what would be generated.
		#[arg(long, conflicts_with = "stdout")]
		check: bool,
		/// Fail instead of warning when an output cannot be produced exactly as
		/// configured: the formatter fails, or a kind cannot be parsed. Same as
		/// `[typegen] strict`.
		#[arg(long)]
		strict: bool,
		/// Leave `generatedAt` out of the JSON so identical schemas produce
		/// identical bytes. Same as `[typegen] generated_at = false`.
		#[arg(long)]
		no_generated_at: bool,
		/// Read the schema document from this file (written by `typegen` or
		/// `compile`) instead of a database. Nothing connects.
		#[arg(long, value_name = "SCHEMA_JSON")]
		from: Option<PathBuf>,
	},
	/// Compile the schema modules to schema document v2 without a database.
	///
	/// Applies the selected modules, in dependency order, to a throwaway embedded
	/// engine with no network access, and writes what it stored as schema document
	/// v2 (and the TypeScript interfaces, when configured). Reads no connection
	/// settings. Strict: an error diagnostic fails the command.
	Compile {
		/// Output path (default: `[typegen] json`, else
		/// `{folder}/types/schema.json`).
		#[arg(long)]
		out: Option<PathBuf>,
		/// Print the JSON to stdout instead of writing the JSON file.
		#[arg(long)]
		stdout: bool,
		/// Emit compact (single-line) JSON instead of pretty-printed.
		#[arg(long)]
		compact: bool,
		/// Write nothing; exit non-zero when any output is missing or differs from
		/// what would be generated.
		#[arg(long, conflicts_with = "stdout")]
		check: bool,
		/// Log error diagnostics instead of failing on them. An external emitter that
		/// cannot run still fails the command.
		#[arg(long)]
		lenient: bool,
		/// Experimental capabilities the compile engine allows, adding to
		/// `[typegen] experimental`: files, surrealism, gql.
		#[arg(long = "allow-experimental", value_name = "FEATURE", value_delimiter = ',')]
		allow_experimental: Vec<String>,
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
	/// Check manifests without connecting. With an id, that manifest; without
	/// one, every manifest and how they chain, which suits a CI check.
	Lint {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: Option<String>,
	},
	/// Run every rollout this database has not run yet, in order. Each one is
	/// started and completed, except the newest, which is left ready to complete
	/// so the application can cut over first.
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
	/// Give a manifest planned before 1.0.0-beta.6 its own copy of the SQL it
	/// applies. Run it where the schema folder matches what it was planned from.
	Freeze {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
	},
	/// Drop the newest rollout from the project, as if it had never been
	/// planned: delete its manifest and directory, and put the snapshots back to
	/// where they were before it. Only for a rollout no database has completed.
	Discard {
		#[arg(value_name = "ROLLOUT_ID")]
		rollout: String,
		/// Leave the snapshots as they are, for when they have been put right by
		/// hand.
		#[arg(long)]
		keep_snapshots: bool,
	},
	/// Heal a rollout stuck in an intermediate state without re-running SQL
	/// steps. Useful when `complete` was killed mid-flight (issue #55) and
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
		let modules = select_modules(project, schemas, all, no_deps)?;

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

/// The schema modules one invocation operates on, in dependency order: explicit
/// `--schema`, else every declared module, else the default one.
fn select_modules(
	project: &ProjectConfig,
	schemas: &[String],
	all: bool,
	no_deps: bool,
) -> Result<Vec<Module>> {
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
	ordered
		.into_iter()
		.map(Module::new)
		.collect::<Result<Vec<_>>>()
		.context("resolving selected schema modules")
}

/// Warn that connection flags do nothing on a command that never connects.
fn warn_unused_connection_flags(command: &str, args: &Cli) {
	let used = !args.target.is_empty()
		|| args.all
		|| args.host.is_some()
		|| args.ns.is_some()
		|| args.db.is_some()
		|| args.user.is_some()
		|| args.pass.is_some()
		|| args.auth_level.is_some()
		|| args.connect_timeout_secs.is_some()
		|| args.query_timeout_secs.is_some();
	if used {
		log::warn!(
			"{command} never connects to a database, so the connection and target flags were \
			 ignored"
		);
	}
}

/// The commands that read only files: `compile`, and `typegen --from`. They run before
/// anything reads connection settings, so no `SURREALDB_*` variable or `.env` file can
/// reach them. Returns `false` for every other command.
async fn run_offline(args: &Cli) -> Result<bool> {
	let folder =
		args.folder.clone().unwrap_or_else(|| surrealkit::constants::DEFAULT_ROOT_DIR.to_string());
	match &args.command {
		Commands::Compile {
			out,
			stdout,
			compact,
			check,
			lenient,
			allow_experimental,
		} => {
			warn_unused_connection_flags("compile", args);
			let project = ProjectConfig::load(None)?;
			let modules = select_modules(&project, &args.schema, args.all, args.no_deps)?;
			let typegen_cfg = surrealkit::variables::load_typegen_config(None)?;
			// `sync` and `typegen` write the configured JSON in the configured version; compile
			// writing v2 there would make the commands overwrite one another.
			if out.is_none()
				&& !*stdout && typegen_cfg.json_path()?.is_some()
				&& typegen_cfg.schema_version()? != surrealkit::typegen::SchemaVersion::V2
			{
				anyhow::bail!(
					"compile writes schema document v2, but [typegen] json is the v1 document \
					 `sync` and `typegen` write (schema_version is 1 unless set); set [typegen] \
					 schema_version = 2 so that every command writes v2 there, or pass --out or \
					 --stdout"
				);
			}
			let raw_vars: Vec<(String, String)> =
				args.var.iter().map(|s| parse_var_flag(s)).collect::<anyhow::Result<_>>()?;
			let mut experimental = typegen_cfg.experimental.clone();
			experimental.extend(allow_experimental.iter().cloned());
			surrealkit::compile::run_compile(
				&project,
				&folder,
				&modules,
				&surrealkit::compile::CompileOpts {
					experimental,
					// Files and flags only: no SURREALKIT_VAR_* from the environment.
					vars: TemplateVars {
						vars: build_project_vars(&raw_vars, None)?,
					},
					expected_engine: typegen_cfg.engine.clone(),
				},
				&TypegenOpts {
					out: out.clone().or(typegen_cfg.json_path()?),
					stdout: *stdout,
					pretty: !compact,
					ts_out: typegen_cfg.typescript_path()?,
					ts_format: typegen_cfg.format.clone(),
					strict: !lenient,
					check: *check,
					omit_generated_at: true,
					schema_version: surrealkit::typegen::SchemaVersion::V2,
					emitters: typegen_cfg.emitters()?,
					acknowledge: typegen_cfg.acknowledgements()?,
					// --lenient tolerates diagnostics, never an emitter that cannot run.
					emitter_failures_fatal: true,
				},
			)
			.await?;
			Ok(true)
		}
		Commands::Typegen {
			out,
			stdout,
			compact,
			typescript,
			check,
			strict,
			no_generated_at,
			from: Some(from),
		} => {
			warn_unused_connection_flags("typegen --from", args);
			let mut typegen_cfg = surrealkit::variables::load_typegen_config(None)?;
			if let Some(typescript) = typescript {
				if surrealkit::variables::is_typescript_file(typescript) {
					typegen_cfg.filename = None;
				}
				typegen_cfg.typescript = Some(typescript.clone());
			}
			surrealkit::typegen::run_typegen_from(
				from,
				&folder,
				TypegenOpts {
					out: out.clone().or(typegen_cfg.json_path()?),
					stdout: *stdout,
					pretty: !compact,
					ts_out: typegen_cfg.typescript_path()?,
					ts_format: typegen_cfg.format.clone(),
					strict: *strict || typegen_cfg.strict,
					check: *check,
					omit_generated_at: *no_generated_at || !typegen_cfg.stamps_generated_at(),
					schema_version: typegen_cfg.schema_version()?,
					emitters: typegen_cfg.emitters()?,
					acknowledge: typegen_cfg.acknowledgements()?,
					emitter_failures_fatal: false,
				},
			)?;
			Ok(true)
		}
		_ => Ok(false),
	}
}

/// The database `typegen` introspects.
///
/// Typegen reads one database, so it resolves a target the way the other
/// single-target commands do: `--target`, else the `primary` target, else the
/// only declared one, else the ambient connection. Before this it always used the
/// ambient connection, so `--target` and `primary` were silently ignored and a
/// project with declared targets introspected whatever the environment pointed
/// at.
///
/// An explicit `--schema` must be one the target accepts: introspecting a
/// database the module is never applied to would describe the wrong schema.
fn typegen_target(selection: &Selection, schema_selection_used: bool) -> Result<&Target> {
	let target = selection.single_target()?;
	if schema_selection_used && selection.modules_for(target).is_empty() {
		bail!(
			"refusing typegen: target {:?} accepts none of the selected schema modules ({}); \
			 pick a target whose `schemas` list includes them",
			target.name(),
			selection.modules.iter().map(Module::name).collect::<Vec<_>>().join(", ")
		);
	}
	Ok(target)
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

/// Install the CLI logger. `-v/--verbose` raises the level to `debug`.
fn init_logging(verbose: bool) {
	let level = if verbose {
		log::LevelFilter::Debug
	} else {
		log::LevelFilter::Info
	};
	let logger = Box::leak(Box::new(CliLogger {
		level,
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

#[tokio::main]
// anyhow::Result rather than Box<dyn Error>: output is identical (Rust prints a
// failed main's Err via Debug, and Box<dyn Error> already delegated to anyhow's),
// but it lets this function use bail!/context directly.
async fn main() -> Result<()> {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let args = Cli::parse();
	init_logging(args.verbose);
	if run_offline(&args).await? {
		let _ = std::io::stdout().flush();
		let _ = std::io::stderr().flush();
		std::process::exit(0);
	}
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
	let schema_selection_used = !args.schema.is_empty();

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
			let typegen_schema_version = typegen_cfg.schema_version()?;
			let typegen_emitters = typegen_cfg.emitters()?;
			let typegen_acknowledge = typegen_cfg.acknowledgements()?;
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
						typegen_ts_out: typegen_ts_out.clone(),
						typegen_ts_format: typegen_cfg.format.clone(),
						typegen_json_out: typegen_json_out.clone(),
						typegen_strict: typegen_cfg.strict,
						typegen_omit_generated_at: !typegen_cfg.stamps_generated_at(),
						typegen_schema_version,
						typegen_emitters: typegen_emitters.clone(),
						typegen_acknowledge: typegen_acknowledge.clone(),
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
			exec,
			exec_root_token,
		} => {
			let exec: Vec<String> = match exec {
				Some(cmd) => {
					let argv: Vec<String> = cmd.split_whitespace().map(str::to_string).collect();
					if argv.is_empty() {
						bail!("--exec needs a command");
					}
					argv
				}
				None => Vec::new(),
			};
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
					schema_from,
					exec,
					exec_root_token,
				},
				template_vars,
				&overrides,
			)
			.await?;
		}
		Commands::Typegen {
			out,
			stdout,
			compact,
			typescript,
			check,
			strict,
			no_generated_at,
			from: _,
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
			let target = typegen_target(&selection, schema_selection_used)?;
			let db = connect(target.cfg()).await?;
			run_typegen(
				&db,
				&folder,
				TypegenOpts {
					out: out.or(json_out),
					stdout,
					pretty: !compact,
					ts_out,
					ts_format: typegen_cfg.format.clone(),
					strict: strict || typegen_cfg.strict,
					check,
					omit_generated_at: no_generated_at || !typegen_cfg.stamps_generated_at(),
					schema_version: typegen_cfg.schema_version()?,
					emitters: typegen_cfg.emitters()?,
					acknowledge: typegen_cfg.acknowledgements()?,
					emitter_failures_fatal: false,
				},
			)
			.await?;
		}
		// Handled by run_offline before anything reads connection settings.
		Commands::Compile {
			..
		} => bail!("compile runs before connection settings are read"),
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
	fn typegen_introspects_the_selected_target() {
		let raw =
			"[target.acme]\nns = \"acme\"\n[target.globex]\nns = \"globex\"\nprimary = true\n";
		let sel = resolve(raw, &[], &["acme"], false, false);
		assert_eq!(typegen_target(&sel, false).unwrap().cfg().ns(), "acme");
		// Without --target, the primary one; before, the ambient connection.
		let sel = resolve(raw, &[], &[], false, false);
		assert_eq!(typegen_target(&sel, false).unwrap().cfg().ns(), "globex");
		// The only declared target needs no `primary`.
		let sel = resolve("[target.acme]\nns = \"acme\"\n", &[], &[], false, false);
		assert_eq!(typegen_target(&sel, false).unwrap().cfg().ns(), "acme");
	}

	#[test]
	fn typegen_refuses_more_than_one_target() {
		let sel = resolve("[target.acme]\n[target.globex]\n", &[], &[], true, false);
		let err = typegen_target(&sel, false).unwrap_err().to_string();
		assert!(err.contains("one database target at a time"), "{err}");
	}

	#[test]
	fn typegen_refuses_a_target_that_never_receives_the_selected_schema() {
		let raw = "[schema.core]\n[schema.billing]\n[target.acme]\nschemas = [\"core\"]\n";
		let sel = resolve(raw, &["billing"], &["acme"], false, true);
		let err = typegen_target(&sel, true).unwrap_err().to_string();
		assert!(err.contains("accepts none of the selected schema modules (billing)"), "{err}");
		// A module the target does accept is fine.
		let sel = resolve(raw, &["core"], &["acme"], false, true);
		assert_eq!(typegen_target(&sel, true).unwrap().name(), "acme");
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
