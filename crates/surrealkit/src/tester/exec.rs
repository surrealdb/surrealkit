//! `surrealkit test --exec <cmd>`: run an external command against a suite's prepared,
//! disposable database, before the runner removes it.
//!
//! The command gets the database's endpoint, namespace and database, a token scoped to that
//! database, the suite's actor tokens and the `semanticDigest` of the database's schema (see
//! [`TestOpts::exec`]), so a test suite in any language (the Effect round-trip properties of
//! `effect-surrealkit`, say) can run against exactly the schema SurrealKit prepared, without
//! credentials of its own.
//!
//! The database token belongs to a user SurrealKit defines on the suite database
//! (`__surrealkit_exec`, owner of that database only, token valid for an hour), so a command
//! pointed at a shared server cannot reach anything else. The server-wide root token is
//! passed only with `--exec-root-token`.
//!
//! The command's exit status is the case's result. Its stdin, stdout and stderr are the
//! runner's. When the runner is interrupted (Ctrl-C) while the command runs, the command is
//! killed; either way the suite's database is removed afterwards unless `--keep-db` is set.
//!
//! [`TestOpts::exec`]: super::TestOpts::exec

use std::collections::{BTreeMap, HashMap};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use surrealdb::opt::auth::Database;

use super::actors::ActorSession;
use super::types::{CaseReport, SchemaSource};
use crate::typegen::document::EngineMode;

/// How often a running command is polled for exit or interruption.
const POLL: Duration = Duration::from_millis(25);

/// The user `--exec` signs in as: an owner of the suite's database, and of nothing else.
pub(super) const EXEC_USER: &str = "__surrealkit_exec";

/// The runner's own credentials, which the command does not inherit: it gets the suite
/// database's token instead, so it cannot reach past that database with them.
const WITHHELD_CREDENTIALS: &[&str] =
	&["SURREALDB_USER", "SURREALDB_PASSWORD", "DATABASE_USER", "DATABASE_PASSWORD"];

/// Where the command runs.
pub(super) struct ExecTarget<'a> {
	pub host: &'a str,
	pub namespace: &'a str,
	pub database: &'a str,
	pub suite: &'a str,
	pub source: SchemaSource,
	pub api_url: Option<&'a str>,
	/// Also pass the root actor's (server-wide) token: `--exec-root-token`.
	pub root_token: bool,
}

/// `SURREALKIT_EXEC_TOKEN_<ACTOR>`: the actor name upper-cased, every character but A–Z and
/// 0–9 replaced by `_`.
pub(super) fn token_variable(actor: &str) -> String {
	let suffix: String = actor
		.chars()
		.map(|c| {
			let c = c.to_ascii_uppercase();
			if c.is_ascii_uppercase() || c.is_ascii_digit() {
				c
			} else {
				'_'
			}
		})
		.collect();
	format!("SURREALKIT_EXEC_TOKEN_{suffix}")
}

/// The variables the command receives, besides the parent's environment. `database_token`
/// is the [`EXEC_USER`] token; the root actor's token is passed only when
/// [`ExecTarget::root_token`] is set.
pub(super) fn environment(
	target: &ExecTarget<'_>,
	actors: &HashMap<String, ActorSession>,
	digest: &str,
	database_token: &str,
) -> Result<BTreeMap<String, String>> {
	let mut env = BTreeMap::new();
	env.insert("SURREALKIT_EXEC_TOKEN".to_string(), database_token.to_string());
	env.insert("SURREALKIT_EXEC_ENDPOINT".to_string(), target.host.to_string());
	env.insert("SURREALKIT_EXEC_NAMESPACE".to_string(), target.namespace.to_string());
	env.insert("SURREALKIT_EXEC_DATABASE".to_string(), target.database.to_string());
	env.insert("SURREALKIT_EXEC_SUITE".to_string(), target.suite.to_string());
	env.insert("SURREALKIT_EXEC_SCHEMA_FROM".to_string(), target.source.label().to_string());
	env.insert("SURREALKIT_EXEC_SCHEMA_DIGEST".to_string(), digest.to_string());
	if let Some(api) = target.api_url {
		env.insert("SURREALKIT_EXEC_API_URL".to_string(), api.to_string());
	}
	let mut names: Vec<&String> =
		actors.keys().filter(|name| target.root_token || name.as_str() != "root").collect();
	names.sort();
	let mut owners: BTreeMap<String, &str> = BTreeMap::new();
	for name in &names {
		let variable = token_variable(name);
		if let Some(other) = owners.insert(variable.clone(), name) {
			bail!(
				"actors `{other}` and `{name}` would both pass their token as {variable}; rename \
				 one of them to use --exec"
			);
		}
		if let Some(token) = actors.get(*name).and_then(|a| a.token.as_ref()) {
			env.insert(variable, token.clone());
		}
	}
	env.insert(
		"SURREALKIT_EXEC_ACTORS".to_string(),
		names.iter().map(|n| n.as_str()).collect::<Vec<_>>().join(","),
	);
	Ok(env)
}

/// Run `argv` against the prepared database and report it as one case. Fails (rather than
/// reporting a failed case) only when the runner was interrupted.
pub(super) async fn run_exec(
	argv: &[String],
	target: &ExecTarget<'_>,
	actors: &HashMap<String, ActorSession>,
	interrupted: &Arc<AtomicBool>,
) -> Result<CaseReport> {
	let started = Instant::now();
	let name = format!("exec: {}", argv.join(" "));
	let outcome = async {
		let root = actors.get("root").context("the suite has no root session")?;
		// Before the exec user exists: users are part of the schema document.
		let document = crate::typegen::introspect_v2::introspect(&root.db, EngineMode::Remote)
			.await
			.context("reading the prepared database's schema for SURREALKIT_EXEC_SCHEMA_DIGEST")?;
		let database_token = database_token(target, root).await?;
		let env = environment(target, actors, &document.semantic_digest, &database_token)?;
		wait(argv, &env, interrupted).await
	}
	.await;
	let (passed, message) = match outcome {
		Ok(Exit::Success) => (true, None),
		Ok(Exit::Failed(status)) => (false, Some(format!("`{}` {status}", argv.join(" ")))),
		Ok(Exit::Interrupted) => {
			bail!("interrupted while `{}` ran; it was stopped", argv.join(" "))
		}
		Err(err) => (false, Some(format!("{err:#}"))),
	};
	Ok(CaseReport {
		name,
		kind: "exec".to_string(),
		duration_ms: started.elapsed().as_millis(),
		passed,
		message,
		assertions: Vec::new(),
	})
}

/// Define [`EXEC_USER`] on the suite database, with a random password, and sign in as it.
async fn database_token(target: &ExecTarget<'_>, root: &ActorSession) -> Result<String> {
	let password = surrealdb_types::uuid::Uuid::new_v4().simple().to_string();
	root.db
		.query(format!(
			"DEFINE USER OVERWRITE {EXEC_USER} ON DATABASE PASSWORD '{password}' ROLES OWNER \
			 DURATION FOR TOKEN 1h, FOR SESSION 1h"
		))
		.await
		.and_then(|response| response.check())
		.map_err(|err| anyhow!("{err}"))
		.context("defining the --exec user on the suite database")?;
	let db = crate::core::create_surreal_client(&target.host.to_string())
		.await
		.with_context(|| format!("connecting to {} as the --exec user", target.host))?;
	let token = db
		.signin(Database {
			namespace: target.namespace.to_string(),
			database: target.database.to_string(),
			username: EXEC_USER.to_string(),
			password,
		})
		.await
		.context("signing in as the --exec user")?;
	Ok(token.access.as_insecure_token().to_string())
}

enum Exit {
	Success,
	Failed(String),
	Interrupted,
}

/// The command for `argv`, with `env` and without [`WITHHELD_CREDENTIALS`].
fn command(argv: &[String], env: &BTreeMap<String, String>) -> Result<Command> {
	let (program, args) = argv.split_first().context("--exec needs a command")?;
	let mut command = Command::new(program);
	for name in WITHHELD_CREDENTIALS {
		command.env_remove(name);
	}
	command.args(args).envs(env);
	Ok(command)
}

/// Start the command and wait for it, killing it if the runner is interrupted.
async fn wait(
	argv: &[String],
	env: &BTreeMap<String, String>,
	interrupted: &Arc<AtomicBool>,
) -> Result<Exit> {
	let mut child = command(argv, env)?
		.stdin(Stdio::inherit())
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.spawn()
		.with_context(|| format!("starting `{}`", argv.join(" ")))?;
	loop {
		if let Some(status) = child.try_wait().context("waiting for the --exec command")? {
			return Ok(if status.success() {
				Exit::Success
			} else {
				Exit::Failed(match status.code() {
					Some(code) => format!("exited with status {code}"),
					None => format!("was terminated ({status})"),
				})
			});
		}
		if interrupted.load(Ordering::SeqCst) {
			// Already gone is fine: either way it must not outlive the database.
			let _ = child.kill();
			let _ = child.wait();
			return Ok(Exit::Interrupted);
		}
		tokio::time::sleep(POLL).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn token_variables_are_upper_snake_case() {
		assert_eq!(token_variable("root"), "SURREALKIT_EXEC_TOKEN_ROOT");
		assert_eq!(token_variable("record-user.2"), "SURREALKIT_EXEC_TOKEN_RECORD_USER_2");
		assert_eq!(token_variable("ünï"), "SURREALKIT_EXEC_TOKEN__N_");
	}

	#[test]
	fn the_runner_credentials_are_not_inherited() {
		let env = BTreeMap::from([("SURREALKIT_EXEC_TOKEN".to_string(), "t".to_string())]);
		let command = command(&["true".into()], &env).expect("command");
		let envs: BTreeMap<_, _> = command
			.get_envs()
			.map(|(k, v)| {
				(k.to_string_lossy().into_owned(), v.map(|v| v.to_string_lossy().into_owned()))
			})
			.collect();
		for name in WITHHELD_CREDENTIALS {
			assert_eq!(envs.get(*name), Some(&None), "{name} is removed");
		}
		assert_eq!(envs.get("SURREALKIT_EXEC_TOKEN"), Some(&Some("t".to_string())));
		assert!(WITHHELD_CREDENTIALS.contains(&"SURREALDB_PASSWORD"));
	}

	#[tokio::test]
	async fn a_failing_command_is_a_failed_case_and_an_interrupt_stops_it() {
		let flag = Arc::new(AtomicBool::new(false));
		let env = BTreeMap::new();
		#[cfg(unix)]
		{
			let ok = wait(&["true".into()], &env, &flag).await.expect("true");
			assert!(matches!(ok, Exit::Success));
			let failed = wait(&["false".into()], &env, &flag).await.expect("false");
			assert!(matches!(failed, Exit::Failed(ref s) if s == "exited with status 1"));
			let started = Instant::now();
			let interrupter = {
				let flag = flag.clone();
				tokio::spawn(async move {
					tokio::time::sleep(Duration::from_millis(100)).await;
					flag.store(true, Ordering::SeqCst);
				})
			};
			let stopped = wait(&["sleep".into(), "30".into()], &env, &flag).await.expect("sleep");
			interrupter.await.expect("interrupter");
			assert!(matches!(stopped, Exit::Interrupted));
			assert!(started.elapsed() < Duration::from_secs(10), "the command was killed");
		}
		let missing = wait(&["surrealkit-no-such-command".into()], &env, &flag).await;
		assert!(format!("{:#}", missing.err().expect("not found")).contains("starting"));
	}
}
