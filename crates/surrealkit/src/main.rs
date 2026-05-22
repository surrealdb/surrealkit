use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{Cfg, ConfigOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts};
use surrealkit::schema::{ResolvedSchema, load_schema_catalog};
use surrealkit::setup::run_setup;
use surrealkit::sync::{self, SyncOpts};
use surrealkit::tester::{TestOpts, run_test};
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
	Setup,
	Sync {
		#[arg(long, conflicts_with = "all_schemas")]
		schema: Option<String>,
		#[arg(long)]
		all_schemas: bool,
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
		#[command(subcommand)]
		command: RolloutCommands,
	},
	Seed {
		#[arg(long, conflicts_with = "all_schemas")]
		schema: Option<String>,
		#[arg(long)]
		all_schemas: bool,
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

async fn connect_schema(
	cfg: &Cfg,
	schema: &ResolvedSchema,
) -> anyhow::Result<surrealkit::Surreal<surrealkit::engine::any::Any>> {
	let target_cfg = cfg.with_target(schema.ns.clone(), schema.db.clone());
	connect(&target_cfg).await
}

fn sync_opts(
	folder: &str,
	watch: bool,
	debounce_ms: u64,
	dry_run: bool,
	fail_fast: bool,
	no_prune: bool,
	allow_shared_prune: bool,
	allow_all_statements: bool,
	vars: TemplateVars,
) -> SyncOpts {
	SyncOpts {
		watch,
		debounce_ms,
		dry_run,
		fail_fast,
		prune: !no_prune,
		allow_shared_prune,
		allow_all_statements,
		vars,
		folder: folder.to_string(),
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

	let args = Cli::parse();
	let env = load_env();
	let overrides = ConfigOverrides {
		host: args.host,
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

	let cfg = Cfg::from_env(env.as_ref(), &overrides)?;
	let folder = cfg.folder().to_owned();
	let schema_catalog = load_schema_catalog(None)?;

	match args.command {
		Commands::Init => scaffold::scaffold(&folder)?,
		Commands::Setup => {
			let db = connect(&cfg).await?;
			run_setup(&db, &folder).await?;
		}
		Commands::Sync {
			schema,
			all_schemas,
			watch,
			debounce_ms,
			dry_run,
			fail_fast,
			no_prune,
			allow_shared_prune,
			allow_all_statements,
		} => {
			if let Some(schema_name) = schema {
				let schema =
					schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
				let db = connect_schema(&cfg, &schema).await?;
				sync::run_sync_with_workspace(
					&db,
					&schema.workspace,
					sync_opts(
						&folder,
						watch,
						debounce_ms,
						dry_run,
						fail_fast,
						no_prune,
						allow_shared_prune,
						allow_all_statements,
						template_vars,
					),
				)
				.await?;
			} else if all_schemas {
				let schemas = schema_catalog.resolve_all_concrete(&folder, &template_vars)?;
				if schemas.is_empty() {
					println!("No concrete schemas found.");
				}
				for schema in schemas {
					println!(
						"Syncing schema '{}' into ns={} db={}",
						schema.name, schema.ns, schema.db
					);
					let db = connect_schema(&cfg, &schema).await?;
					sync::run_sync_with_workspace(
						&db,
						&schema.workspace,
						sync_opts(
							&folder,
							watch,
							debounce_ms,
							dry_run,
							fail_fast,
							no_prune,
							allow_shared_prune,
							allow_all_statements,
							template_vars.clone(),
						),
					)
					.await?;
				}
			} else {
				let db = connect(&cfg).await?;
				sync::run_sync(
					&db,
					&folder,
					sync_opts(
						&folder,
						watch,
						debounce_ms,
						dry_run,
						fail_fast,
						no_prune,
						allow_shared_prune,
						allow_all_statements,
						template_vars,
					),
				)
				.await?;
			}
		}
		Commands::Rollout {
			schema,
			command,
		} => match command {
			RolloutCommands::Baseline => {
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					let db = connect_schema(&cfg, &schema).await?;
					rollout::run_baseline_with_workspace(&db, &schema.workspace).await?;
				} else {
					let db = connect(&cfg).await?;
					rollout::run_baseline(&db, &folder).await?;
				}
			}
			RolloutCommands::Plan {
				name,
				dry_run,
			} => {
				let opts = RolloutPlanOpts {
					name,
					dry_run,
				};
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					rollout::run_plan_with_workspace(&schema.workspace, opts).await?;
				} else {
					rollout::run_plan(&folder, opts).await?;
				}
			}
			RolloutCommands::Start {
				target,
			} => {
				let opts = RolloutExecutionOpts {
					selector: Some(target),
				};
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					let db = connect_schema(&cfg, &schema).await?;
					rollout::run_start_with_workspace(&db, &schema.workspace, opts, &template_vars)
						.await?;
				} else {
					let db = connect(&cfg).await?;
					rollout::run_start(&db, &folder, opts, &template_vars).await?;
				}
			}
			RolloutCommands::Complete {
				target,
			} => {
				let opts = RolloutExecutionOpts {
					selector: Some(target),
				};
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					let db = connect_schema(&cfg, &schema).await?;
					rollout::run_complete_with_workspace(
						&db,
						&schema.workspace,
						opts,
						&template_vars,
					)
					.await?;
				} else {
					let db = connect(&cfg).await?;
					rollout::run_complete(&db, &folder, opts, &template_vars).await?;
				}
			}
			RolloutCommands::Rollback {
				target,
			} => {
				let opts = RolloutExecutionOpts {
					selector: Some(target),
				};
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					let db = connect_schema(&cfg, &schema).await?;
					rollout::run_rollback_with_workspace(
						&db,
						&schema.workspace,
						opts,
						&template_vars,
					)
					.await?;
				} else {
					let db = connect(&cfg).await?;
					rollout::run_rollback(&db, &folder, opts, &template_vars).await?;
				}
			}
			RolloutCommands::Status {
				target,
			} => {
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					let db = connect_schema(&cfg, &schema).await?;
					rollout::run_status(&db, &folder, target).await?;
				} else {
					let db = connect(&cfg).await?;
					rollout::run_status(&db, &folder, target).await?;
				}
			}
			RolloutCommands::Lint {
				target,
			} => {
				let opts = RolloutExecutionOpts {
					selector: Some(target),
				};
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					rollout::run_lint_with_workspace(&schema.workspace, opts).await?;
				} else {
					rollout::run_lint(&folder, opts).await?;
				}
			}
			RolloutCommands::Repair {
				target,
			} => {
				let opts = RolloutExecutionOpts {
					selector: Some(target),
				};
				if let Some(schema_name) = schema {
					let schema =
						schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
					let db = connect_schema(&cfg, &schema).await?;
					rollout::run_repair_with_workspace(&db, &schema.workspace, opts).await?;
				} else {
					let db = connect(&cfg).await?;
					rollout::run_repair(&db, &folder, opts).await?;
				}
			}
		},
		Commands::Seed {
			schema,
			all_schemas,
		} => {
			if let Some(schema_name) = schema {
				let schema =
					schema_catalog.resolve_concrete(&schema_name, &folder, &template_vars)?;
				let db = connect_schema(&cfg, &schema).await?;
				seed::seed_from_dirs(&db, &schema.seed_dirs, &template_vars).await?;
			} else if all_schemas {
				let schemas = schema_catalog.resolve_all_concrete(&folder, &template_vars)?;
				if schemas.is_empty() {
					println!("No concrete schemas found.");
				}
				for schema in schemas {
					println!(
						"Seeding schema '{}' into ns={} db={}",
						schema.name, schema.ns, schema.db
					);
					let db = connect_schema(&cfg, &schema).await?;
					seed::seed_from_dirs(&db, &schema.seed_dirs, &template_vars).await?;
				}
			} else {
				let db = connect(&cfg).await?;
				seed::seed(&db, &folder, &template_vars).await?;
			}
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
	fn global_ns_and_db_flags_are_removed() {
		assert!(Cli::try_parse_from(["surrealkit", "--ns", "app", "sync"]).is_err());
		assert!(Cli::try_parse_from(["surrealkit", "--db", "main", "sync"]).is_err());
	}

	#[test]
	fn sync_schema_and_all_schemas_are_mutually_exclusive() {
		assert!(
			Cli::try_parse_from(["surrealkit", "sync", "--schema", "admin", "--all-schemas",])
				.is_err()
		);
	}

	#[test]
	fn seed_schema_and_all_schemas_are_mutually_exclusive() {
		assert!(
			Cli::try_parse_from(["surrealkit", "seed", "--schema", "admin", "--all-schemas",])
				.is_err()
		);
	}
}
