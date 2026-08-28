use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{DbCfg, DbOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::module::Module;
use surrealkit::project::{ProjectConfig, Target};
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts};
use surrealkit::setup::run_setup;
use surrealkit::sync::{self, SyncOpts};
use surrealkit::tester::{TestOpts, run_test};
use surrealkit::typegen::{TypegenOpts, run_typegen};
use surrealkit::variables::{TemplateVars, build_vars, parse_var_flag};

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
		/// Allow a prune that removes every managed entity because no schema files
		/// were found (normally refused: it usually means the folder is wrong).
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

#[derive(Subcommand, Debug)]
enum RolloutCommands {
	Baseline,
	Plan {
		#[arg(long)]
		name: Option<String>,
		#[arg(long)]
		dry_run: bool,
	},
	Start {
		target: String,
	},
	Complete {
		target: String,
	},
	Rollback {
		target: String,
	},
	Status {
		target: Option<String>,
	},
	Lint {
		target: String,
	},
	/// Heal a rollout stuck in an intermediate state without re-running SQL
	/// steps. Useful when `complete` was killed mid-flight (issue #55) and
	/// `__rollout.status` is still `running_complete` / `running_rollback`.
	Repair {
		target: String,
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
	let env = load_env();
	let overrides = DbOverrides {
		host: args.host,
		ns: args.ns,
		db: args.db,
		user: args.user,
		pass: args.pass,
		auth_level: args.auth_level,
		folder: args.folder,
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
			if watch && selection.pairs() > 1 {
				bail!(
					"--watch needs a single schema module and target ({} selected); \
					 watching a whole matrix on a timer is rarely what you want",
					selection.pairs()
				);
			}

			let mut results: Vec<PairResult> = Vec::new();
			'targets: for target in selection.targets() {
				let db = connect(target.cfg()).await?;
				for module in selection.modules_for(target) {
					if selection.is_fan_out() {
						println!("→ {} → {}", module.name(), target.name());
					}
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
					let outcome = sync::run_sync(&db, opts).await;
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
				let db = connect(&cfg).await?;
				rollout::run_baseline(&db, &folder, selection.single_module()?).await?;
			}
			RolloutCommands::Plan {
				name,
				dry_run,
			} => {
				rollout::run_plan(
					&folder,
					RolloutPlanOpts {
						name,
						dry_run,
					},
				)
				.await?;
			}
			RolloutCommands::Start {
				target,
			} => {
				let db = connect(&cfg).await?;
				rollout::run_start(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(target),
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Complete {
				target,
			} => {
				let db = connect(&cfg).await?;
				rollout::run_complete(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(target),
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Rollback {
				target,
			} => {
				let db = connect(&cfg).await?;
				rollout::run_rollback(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(target),
					},
					&template_vars,
				)
				.await?;
			}
			RolloutCommands::Status {
				target,
			} => {
				let db = connect(&cfg).await?;
				rollout::run_status(&db, &folder, target).await?;
			}
			RolloutCommands::Lint {
				target,
			} => {
				rollout::run_lint(
					&folder,
					RolloutExecutionOpts {
						selector: Some(target),
					},
				)
				.await?;
			}
			RolloutCommands::Repair {
				target,
			} => {
				let db = connect(&cfg).await?;
				rollout::run_repair(
					&db,
					&folder,
					RolloutExecutionOpts {
						selector: Some(target),
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
}
