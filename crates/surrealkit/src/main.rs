use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rust_dotenv::dotenv::DotEnv;
use surrealkit::config::{Cfg, ConfigOverrides, connect};
use surrealkit::core::exec_surql;
use surrealkit::rollout::{self, RolloutExecutionOpts, RolloutPlanOpts};
use surrealkit::seed;
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

	let raw_vars: Vec<(String, String)> =
		args.var.iter().map(|s| parse_var_flag(s)).collect::<anyhow::Result<_>>()?;
	let template_vars = TemplateVars {
		vars: build_vars(&raw_vars, None)?,
	};

	let cfg = Cfg::from_env(env.as_ref(), &overrides)?;
	let folder = cfg.folder().to_owned();

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
			allow_all_statements,
		} => {
			let db = connect(&cfg).await?;
			let typegen_cfg = surrealkit::variables::load_typegen_config(None)?;
			sync::run_sync(
				&db,
				&folder,
				SyncOpts {
					watch,
					debounce_ms,
					dry_run,
					fail_fast,
					prune: !no_prune,
					allow_shared_prune,
					allow_all_statements,
					vars: template_vars,
					folder: folder.clone(),
					typegen_ts_out: typegen_cfg.typescript,
					typegen_ts_format: typegen_cfg.format,
				},
			)
			.await?;
		}
		Commands::Rollout {
			command,
		} => match command {
			RolloutCommands::Baseline => {
				let db = connect(&cfg).await?;
				rollout::run_baseline(&db, &folder).await?;
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
		Commands::Seed => {
			let db = connect(&cfg).await?;
			seed::seed(&db, &folder, &template_vars).await?;
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
