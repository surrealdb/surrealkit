use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealkit::config::{DbCfg, DbOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts};
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
		#[command(subcommand)]
		command: RolloutCommands,
	},
	Seed,
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
	};

	let raw_vars: Vec<(String, String)> =
		args.var.iter().map(|s| parse_var_flag(s)).collect::<anyhow::Result<_>>()?;
	let template_vars = TemplateVars {
		vars: build_vars(&raw_vars, None)?,
	};

	match args.command {
		Commands::Init => scaffold::scaffold()?,
		Commands::Setup => {
			let db = connect_from_env(env.as_ref(), &overrides).await?;
			run_setup(&db).await?;
		}
		Commands::Sync {
			watch,
			debounce_ms,
			dry_run,
			fail_fast,
			no_prune,
			allow_shared_prune,
			allow_all_statements,
		} => {
			let db = connect_from_env(env.as_ref(), &overrides).await?;
			sync::run_sync(
				&db,
				SyncOpts {
					watch,
					debounce_ms,
					dry_run,
					fail_fast,
					prune: !no_prune,
					allow_shared_prune,
					allow_all_statements,
					vars: template_vars,
				},
			)
			.await?;
		}
		Commands::Rollout {
			command,
		} => match command {
			RolloutCommands::Baseline => {
				let db = connect_from_env(env.as_ref(), &overrides).await?;
				rollout::run_baseline(&db).await?;
			}
			RolloutCommands::Plan {
				name,
				dry_run,
			} => {
				rollout::run_plan(RolloutPlanOpts {
					name,
					dry_run,
				})
				.await?;
			}
			RolloutCommands::Start {
				target,
			} => {
				let db = connect_from_env(env.as_ref(), &overrides).await?;
				rollout::run_start(
					&db,
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
				let db = connect_from_env(env.as_ref(), &overrides).await?;
				rollout::run_complete(
					&db,
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
				let db = connect_from_env(env.as_ref(), &overrides).await?;
				rollout::run_rollback(
					&db,
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
				let db = connect_from_env(env.as_ref(), &overrides).await?;
				rollout::run_status(&db, target).await?;
			}
			RolloutCommands::Lint {
				target,
			} => {
				rollout::run_lint(RolloutExecutionOpts {
					selector: Some(target),
				})
				.await?;
			}
		},
		Commands::Seed => {
			let db = connect_from_env(env.as_ref(), &overrides).await?;
			seed::seed(&db, &template_vars).await?;
		}
		Commands::Status => {
			let db = connect_from_env(env.as_ref(), &overrides).await?;
			rollout::run_status(&db, None).await?;
		}
		Commands::Apply {
			path,
		} => {
			let db = connect_from_env(env.as_ref(), &overrides).await?;
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

	Ok(())
}

async fn connect_from_env(
	env: Option<&DotEnv>,
	overrides: &DbOverrides,
) -> anyhow::Result<Surreal<Any>> {
	let cfg = DbCfg::from_env(env, overrides)?;
	connect(&cfg).await
}
