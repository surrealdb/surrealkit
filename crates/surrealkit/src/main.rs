use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{Cfg, ConfigOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts};
use surrealkit::schema::{SchemaCatalog, SchemaTarget, load_schema_catalog};
use surrealkit::setup::run_setup;
use surrealkit::sync::{self, SyncOpts};
use surrealkit::tester::{TestOpts, run_test};
use surrealkit::typegen::{TypegenOpts, run_typegen};
use surrealkit::variables::{TemplateVars, build_vars, parse_var_flag};
use surrealkit::{scaffold, seed};

#[derive(Parser, Debug)]
#[command(version, about = "SurrealKit CLI")]
pub struct Cli {
	/// Increase output
	#[arg(short, long, global = true)]
	verbose: bool,

	/// Database host URL
	#[arg(long, global = true)]
	host: Option<String>,

	/// Deprecated: database namespace for legacy flat mode. Use [schema.*].ns in surrealkit.toml.
	#[arg(long, global = true)]
	ns: Option<String>,

	/// Deprecated: database name for legacy flat mode. Use [schema.*].db in surrealkit.toml.
	#[arg(long, global = true)]
	db: Option<String>,

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
	Init,
	Setup {
		#[arg(long)]
		schema: Option<String>,
		#[arg(long, conflicts_with = "schema")]
		skip_template_schemas: bool,
	},
	Sync {
		#[arg(long)]
		schema: Option<String>,
		/// Skip schemas whose required template variables are not supplied
		/// instead of erroring. Only applies when syncing all schemas.
		#[arg(long, conflicts_with = "schema")]
		skip_template_schemas: bool,
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
		/// Allow non-DEFINE statements in schema files (e.g. INSERT, UPDATE, CREATE).
		/// Disables catalog entity tracking; only file-level hashes are tracked.
		#[arg(long)]
		allow_all_statements: bool,
	},
	Rollout {
		#[arg(long)]
		schema: Option<String>,
		/// Skip schemas whose required template variables are not supplied
		/// instead of erroring. Only applies when running rollout for all schemas.
		#[arg(long, conflicts_with = "schema")]
		skip_template_schemas: bool,
		#[command(subcommand)]
		command: RolloutCommands,
	},
	Seed {
		#[arg(long)]
		schema: Option<String>,
		#[arg(long, conflicts_with = "schema")]
		skip_template_schemas: bool,
	},
	Status {
		#[arg(long)]
		schema: Option<String>,
	},
	Apply {
		#[arg(long)]
		schema: Option<String>,
		/// Skip schemas whose required template variables are not supplied
		/// instead of erroring. Only applies when applying to all schemas.
		#[arg(long, conflicts_with = "schema")]
		skip_template_schemas: bool,
		path: PathBuf,
	},
	Test {
		#[arg(long)]
		schema: Option<String>,
		#[arg(long, conflicts_with = "schema")]
		skip_template_schemas: bool,
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

async fn connect_target(
	cfg: &Cfg,
	target: &SchemaTarget,
) -> anyhow::Result<surrealkit::Surreal<surrealkit::engine::any::Any>> {
	let target_cfg = target.connect_config(cfg);
	connect(&target_cfg).await
}

#[derive(Debug, Clone)]
struct SyncFlags {
	watch: bool,
	debounce_ms: u64,
	dry_run: bool,
	fail_fast: bool,
	no_prune: bool,
	allow_shared_prune: bool,
	allow_all_statements: bool,
}

fn sync_opts(folder: &str, flags: &SyncFlags, vars: TemplateVars) -> SyncOpts {
	SyncOpts {
		watch: flags.watch,
		debounce_ms: flags.debounce_ms,
		dry_run: flags.dry_run,
		fail_fast: flags.fail_fast,
		prune: !flags.no_prune,
		allow_shared_prune: flags.allow_shared_prune,
		allow_all_statements: flags.allow_all_statements,
		vars,
		folder: folder.to_string(),
	}
}

fn warn_deprecated_target_flags(overrides: &ConfigOverrides) {
	if overrides.ns.is_some() || overrides.db.is_some() {
		eprintln!(
			"warning: --ns and --db are deprecated and only kept for legacy flat-schema \
			 compatibility. Prefer defining namespace/database targets in surrealkit.toml:\n\
			 \n\
			 \t[schema.main]\n\
			 \tns = \"{}\"\n\
			 \tdb = \"{}\"\n\
			 \n\
			 Named-schema commands ignore --ns/--db and use the selected schema target.",
			overrides.ns.as_deref().unwrap_or("<namespace>"),
			overrides.db.as_deref().unwrap_or("<database>")
		);
	}
}

fn warn_legacy_target(command: &str, target: &SchemaTarget, folder: &str) {
	if !target.is_legacy() {
		return;
	}
	eprintln!(
		"warning: no named schemas found in surrealkit.toml; `{command}` is using the \
		 deprecated legacy flat layout (ns={} db={}).\n\
		 \n\
		 Define named schemas in surrealkit.toml and move legacy files manually:\n\
		 \n\
		 \t{folder}/schema/                 -> {folder}/schemas/<name>/\n\
		 \t{folder}/seed/*.surql            -> {folder}/seed/<name>/\n\
		 \t{folder}/rollouts/*.toml         -> {folder}/rollouts/<name>/\n\
		 \t{folder}/snapshots/*_snapshot.json -> {folder}/snapshots/<name>/\n\
		 \n\
		 Example:\n\
		 \t[schema.main]\n\
		 \tns = \"{}\"\n\
		 \tdb = \"{}\"\n\
		 \n\
		 The legacy flat layout will be removed in a future release.",
		target.ns(),
		target.db(),
		target.ns(),
		target.db()
	);
}

fn log_named_target(action: &str, target: &SchemaTarget) {
	if target.schema_name().is_none() {
		return;
	}
	if target.is_merged() {
		println!(
			"{action} merged schemas [{}] into ns={} db={}",
			target.source_schemas().join(", "),
			target.ns(),
			target.db()
		);
		return;
	}
	println!(
		"{action} schema '{}' into ns={} db={}",
		target.label(),
		target.ns(),
		target.db()
	);
}

fn resolve_targets(
	catalog: &SchemaCatalog,
	schema: Option<&str>,
	skip_template_schemas: bool,
	folder: &str,
	vars: &TemplateVars,
	cfg: &Cfg,
) -> anyhow::Result<Vec<SchemaTarget>> {
	let targets =
		catalog.resolve_targets(schema, skip_template_schemas, folder, vars, cfg.ns(), cfg.db())?;
	if targets.is_empty() {
		anyhow::bail!("No resolved schemas found.");
	}
	Ok(targets)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let args = Cli::parse();
	let env = load_env();
	let overrides = ConfigOverrides {
		host: args.host,
		ns: args.ns,
		db: args.db,
		user: args.user,
		pass: args.pass,
		auth_level: args.auth_level,
		folder: args.folder,
	};
	warn_deprecated_target_flags(&overrides);

	let raw_vars: Vec<(String, String)> =
		args.var.iter().map(|s| parse_var_flag(s)).collect::<anyhow::Result<_>>()?;
	let template_vars = TemplateVars {
		vars: build_vars(&raw_vars, None, env.as_ref())?,
	};

	let cfg = Cfg::from_env(env.as_ref(), &overrides)?;
	let folder = cfg.folder().to_owned();
	let schema_catalog = load_schema_catalog(None)?;

	match args.command {
		Commands::Init => scaffold::scaffold(&folder)?,
		Commands::Setup {
			schema,
			skip_template_schemas,
		} => {
			let targets = resolve_targets(
				&schema_catalog,
				schema.as_deref(),
				skip_template_schemas,
				&folder,
				&template_vars,
				&cfg,
			)?;
			for target in targets {
				warn_legacy_target("setup", &target, &folder);
				log_named_target("Setting up", &target);
				let db = connect_target(&cfg, &target).await?;
				run_setup(&db, &folder).await?;
			}
		}
		Commands::Sync {
			schema,
			skip_template_schemas,
			watch,
			debounce_ms,
			dry_run,
			fail_fast,
			no_prune,
			allow_shared_prune,
			allow_all_statements,
		} => {
			let flags = SyncFlags {
				watch,
				debounce_ms,
				dry_run,
				fail_fast,
				no_prune,
				allow_shared_prune,
				allow_all_statements,
			};
			let targets = resolve_targets(
				&schema_catalog,
				schema.as_deref(),
				skip_template_schemas,
				&folder,
				&template_vars,
				&cfg,
			)?;
			for target in targets {
				warn_legacy_target("sync", &target, &folder);
				log_named_target("Syncing", &target);
				let db = connect_target(&cfg, &target).await?;
				sync::run_sync_with_workspace(
					&db,
					target.workspace(),
					sync_opts(&folder, &flags, template_vars.clone()),
				)
				.await?;
			}
		}
		Commands::Rollout {
			schema,
			skip_template_schemas,
			command,
		} => {
			let targets = resolve_targets(
				&schema_catalog,
				schema.as_deref(),
				skip_template_schemas,
				&folder,
				&template_vars,
				&cfg,
			)?;
			for target in targets {
				warn_legacy_target("rollout", &target, &folder);
				log_named_target("Running rollout for", &target);
				match &command {
					RolloutCommands::Baseline => {
						let db = connect_target(&cfg, &target).await?;
						for workspace in target.rollout_workspaces() {
							rollout::run_baseline_with_workspace(&db, workspace).await?;
						}
					}
					RolloutCommands::Plan {
						name,
						dry_run,
					} => {
						let opts = RolloutPlanOpts {
							name: name.clone(),
							dry_run: *dry_run,
						};
						for workspace in target.rollout_workspaces() {
							rollout::run_plan_with_workspace(workspace, opts.clone()).await?;
						}
					}
					RolloutCommands::Start {
						target: selector,
					} => {
						let opts = RolloutExecutionOpts {
							selector: Some(selector.clone()),
						};
						let db = connect_target(&cfg, &target).await?;
						for workspace in target.rollout_workspaces() {
							rollout::run_start_with_workspace(
								&db,
								workspace,
								opts.clone(),
								&template_vars,
							)
							.await?;
						}
					}
					RolloutCommands::Complete {
						target: selector,
					} => {
						let opts = RolloutExecutionOpts {
							selector: Some(selector.clone()),
						};
						let db = connect_target(&cfg, &target).await?;
						for workspace in target.rollout_workspaces() {
							rollout::run_complete_with_workspace(
								&db,
								workspace,
								opts.clone(),
								&template_vars,
							)
							.await?;
						}
					}
					RolloutCommands::Rollback {
						target: selector,
					} => {
						let opts = RolloutExecutionOpts {
							selector: Some(selector.clone()),
						};
						let db = connect_target(&cfg, &target).await?;
						for workspace in target.rollout_workspaces() {
							rollout::run_rollback_with_workspace(
								&db,
								workspace,
								opts.clone(),
								&template_vars,
							)
							.await?;
						}
					}
					RolloutCommands::Status {
						target: selector,
					} => {
						let db = connect_target(&cfg, &target).await?;
						rollout::run_status(&db, &folder, selector.clone()).await?;
					}
					RolloutCommands::Lint {
						target: selector,
					} => {
						let opts = RolloutExecutionOpts {
							selector: Some(selector.clone()),
						};
						for workspace in target.rollout_workspaces() {
							rollout::run_lint_with_workspace(workspace, opts.clone()).await?;
						}
					}
					RolloutCommands::Repair {
						target: selector,
					} => {
						let opts = RolloutExecutionOpts {
							selector: Some(selector.clone()),
						};
						let db = connect_target(&cfg, &target).await?;
						for workspace in target.rollout_workspaces() {
							rollout::run_repair_with_workspace(&db, workspace, opts.clone()).await?;
						}
					}
				}
			}
		}
		Commands::Seed {
			schema,
			skip_template_schemas,
		} => {
			let targets = resolve_targets(
				&schema_catalog,
				schema.as_deref(),
				skip_template_schemas,
				&folder,
				&template_vars,
				&cfg,
			)?;
			for target in targets {
				warn_legacy_target("seed", &target, &folder);
				log_named_target("Seeding", &target);
				let db = connect_target(&cfg, &target).await?;
				if target.is_legacy() {
					seed::seed(&db, &folder, &template_vars).await?;
				} else {
					seed::seed_from_dirs(&db, target.seed_dirs(), &template_vars).await?;
				}
			}
		}
		Commands::Status {
			schema,
		} => {
			if let Some(schema_name) = schema {
				let target = schema_catalog.resolve_required_target(
					Some(&schema_name),
					&folder,
					&template_vars,
					cfg.ns(),
					cfg.db(),
				)?;
				println!("--- schema '{}' (ns={} db={})", target.label(), target.ns(), target.db());
				let db = connect_target(&cfg, &target).await?;
				rollout::run_status(&db, &folder, None).await?;
			} else if !schema_catalog.is_empty() {
				for name in schema_catalog.names() {
					match schema_catalog.resolve_typed(&name, &folder, &template_vars) {
						Ok(schema) => {
							let target = SchemaTarget::Named {
								schema,
							};
							println!(
								"--- schema '{}' (ns={} db={})",
								target.label(),
								target.ns(),
								target.db()
							);
							let db = connect_target(&cfg, &target).await?;
							rollout::run_status(&db, &folder, None).await?;
						}
						Err(err) => {
							println!("--- schema '{}' (skipped: {})", name, err);
						}
					}
				}
			} else {
				let target = schema_catalog.resolve_required_target(
					None,
					&folder,
					&template_vars,
					cfg.ns(),
					cfg.db(),
				)?;
				warn_legacy_target("status", &target, &folder);
				let db = connect_target(&cfg, &target).await?;
				rollout::run_status(&db, &folder, None).await?;
			}
		}
		Commands::Apply {
			schema,
			skip_template_schemas,
			path,
		} => {
			let sql = std::fs::read_to_string(&path)?;
			let sql = template_vars.apply(&sql)?;
			let targets = resolve_targets(
				&schema_catalog,
				schema.as_deref(),
				skip_template_schemas,
				&folder,
				&template_vars,
				&cfg,
			)?;
			for target in targets {
				warn_legacy_target("apply", &target, &folder);
				if target.schema_name().is_some() {
					if target.is_merged() {
						println!(
							"Applying {} to merged schemas [{}] (ns={} db={})",
							path.display(),
							target.source_schemas().join(", "),
							target.ns(),
							target.db()
						);
					} else {
						println!(
							"Applying {} to schema '{}' (ns={} db={})",
							path.display(),
							target.label(),
							target.ns(),
							target.db()
						);
					}
				}
				let db = connect_target(&cfg, &target).await?;
				exec_surql(&db, &sql).await?;
			}
		}
		Commands::Test {
			schema,
			skip_template_schemas,
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
					schema,
					skip_template_schemas,
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
			run_typegen(
				&db,
				&folder,
				cfg.ns(),
				cfg.db(),
				TypegenOpts {
					out,
					stdout,
					pretty: !compact,
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
mod tests {
	use super::*;

	#[test]
	fn global_ns_and_db_flags_parse_for_legacy_compatibility() {
		assert!(Cli::try_parse_from(["surrealkit", "--ns", "app", "--db", "main", "sync"]).is_ok());
	}

	#[test]
	fn migrate_command_is_not_exposed() {
		assert!(Cli::try_parse_from(["surrealkit", "migrate", "main"]).is_err());
	}

	#[test]
	fn sync_schema_flag_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "sync", "--schema", "admin"]).is_ok());
	}

	#[test]
	fn sync_skip_template_schemas_flag_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "sync", "--skip-template-schemas"]).is_ok());
	}

	#[test]
	fn sync_skip_template_schemas_conflicts_with_schema() {
		assert!(
			Cli::try_parse_from([
				"surrealkit",
				"sync",
				"--schema",
				"admin",
				"--skip-template-schemas",
			])
			.is_err()
		);
	}

	#[test]
	fn sync_all_schemas_flag_is_removed() {
		assert!(Cli::try_parse_from(["surrealkit", "sync", "--all-schemas"]).is_err());
	}

	#[test]
	fn seed_schema_flag_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "seed", "--schema", "admin"]).is_ok());
	}

	#[test]
	fn seed_skip_template_schemas_flag_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "seed", "--skip-template-schemas"]).is_ok());
	}

	#[test]
	fn seed_skip_template_schemas_conflicts_with_schema() {
		assert!(
			Cli::try_parse_from([
				"surrealkit",
				"seed",
				"--schema",
				"admin",
				"--skip-template-schemas",
			])
			.is_err()
		);
	}

	#[test]
	fn seed_all_schemas_flag_is_removed() {
		assert!(Cli::try_parse_from(["surrealkit", "seed", "--all-schemas"]).is_err());
	}

	#[test]
	fn setup_schema_flag_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "setup", "--schema", "admin"]).is_ok());
	}

	#[test]
	fn status_schema_flag_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "status", "--schema", "admin"]).is_ok());
	}

	#[test]
	fn apply_schema_flag_parses() {
		assert!(
			Cli::try_parse_from(["surrealkit", "apply", "--schema", "admin", "file.surql"]).is_ok()
		);
	}

	#[test]
	fn rollout_schema_flag_parses() {
		assert!(
			Cli::try_parse_from(["surrealkit", "rollout", "--schema", "admin", "baseline"]).is_ok()
		);
	}

	#[test]
	fn rollout_baseline_without_schema_parses() {
		assert!(Cli::try_parse_from(["surrealkit", "rollout", "baseline"]).is_ok());
	}

	#[test]
	fn rollout_skip_template_schemas_flag_parses() {
		assert!(
			Cli::try_parse_from(["surrealkit", "rollout", "--skip-template-schemas", "baseline"])
				.is_ok()
		);
	}

	#[test]
	fn rollout_skip_template_schemas_conflicts_with_schema() {
		assert!(
			Cli::try_parse_from([
				"surrealkit",
				"rollout",
				"--schema",
				"admin",
				"--skip-template-schemas",
				"baseline",
			])
			.is_err()
		);
	}
}
