use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

mod config;
mod core;
mod rollout;
mod scaffold;
mod schema_state;
mod seed;
mod setup;
mod sync;
mod tester;

use core::exec_surql;

use config::{DbCfg, connect};
use rollout::{RolloutExecutionOpts, RolloutPlanOpts};
use setup::run_setup;
use sync::SyncOpts;
use tester::{TestOpts, run_test};

#[derive(Parser, Debug)]
#[command(version, about = "SurrealKit CLI")]
pub struct Cli {
	/// Increase output
	#[arg(short, long, global = true)]
	verbose: bool,

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

fn load_env() -> DotEnv {
	// Load .env in CWD if present, ignore missing
	let env = DotEnv::new("");
	env
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let args = Cli::parse();
	let env = load_env();

	match args.command {
		Commands::Init => scaffold::scaffold()?,
		Commands::Setup => {
			let db = connect_from_env(&env).await?;
			run_setup(&db).await?;
		}
		Commands::Sync {
			watch,
			debounce_ms,
			dry_run,
			fail_fast,
			no_prune,
			allow_shared_prune,
		} => {
			let db = connect_from_env(&env).await?;
			sync::run_sync(
				&db,
				SyncOpts {
					watch,
					debounce_ms,
					dry_run,
					fail_fast,
					prune: !no_prune,
					allow_shared_prune,
				},
			)
			.await?;
		}
		Commands::Rollout {
			command,
		} => match command {
			RolloutCommands::Baseline => {
				let db = connect_from_env(&env).await?;
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
				let db = connect_from_env(&env).await?;
				rollout::run_start(
					&db,
					RolloutExecutionOpts {
						selector: Some(target),
					},
				)
				.await?;
			}
			RolloutCommands::Complete {
				target,
			} => {
				let db = connect_from_env(&env).await?;
				rollout::run_complete(
					&db,
					RolloutExecutionOpts {
						selector: Some(target),
					},
				)
				.await?;
			}
			RolloutCommands::Rollback {
				target,
			} => {
				let db = connect_from_env(&env).await?;
				rollout::run_rollback(
					&db,
					RolloutExecutionOpts {
						selector: Some(target),
					},
				)
				.await?;
			}
			RolloutCommands::Status {
				target,
			} => {
				let db = connect_from_env(&env).await?;
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
			let db = connect_from_env(&env).await?;
			seed::seed(&db).await?;
		}
		Commands::Status => {
			let db = connect_from_env(&env).await?;
			rollout::run_status(&db, None).await?;
		}
		Commands::Apply {
			path,
		} => {
			let db = connect_from_env(&env).await?;
			let sql = std::fs::read_to_string(&path)?;
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
			run_test(TestOpts {
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
			})
			.await?;
		}
	}

	Ok(())
}

async fn connect_from_env(env: &DotEnv) -> anyhow::Result<Surreal<Any>> {
	let cfg = DbCfg::from_env(env)?;
	connect(&cfg).await
}
