//! Prompts: the workflows where knowing SurrealKit's shape matters more than
//! knowing its API.
//!
//! Each of these encodes something a tool description cannot. A prompt that just
//! wraps one tool call would be `prompts/list` noise, so there are four, not ten.

use rmcp::handler::server::router::prompt::PromptRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{GetPromptResult, PromptMessage, PromptMessageRole};
use rmcp::{prompt, prompt_router, schemars};
use serde::Deserialize;

use crate::mcp::SurrealKitMcp;

/// Build the prompt router.
pub(super) fn router() -> PromptRouter<SurrealKitMcp> {
	SurrealKitMcp::prompt_router()
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct AuthorRolloutArgs {
	/// The schema change in plain language, e.g. "split `name` into
	/// `given_name` and `family_name` on the person table".
	pub change: String,
	/// The schema module it belongs to. Omit for the default module.
	#[serde(default)]
	pub module: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DiagnoseRolloutArgs {
	/// The rollout id that looks stuck. Omit to triage all of them.
	#[serde(default)]
	pub rollout_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReviewSyncArgs {
	/// The target to review against. Omit for the primary target.
	#[serde(default)]
	pub target: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WriteTestsArgs {
	/// The table to write a suite for.
	pub table: String,
}

fn user(text: String) -> GetPromptResult {
	let mut result = GetPromptResult::default();
	result.messages = vec![PromptMessage::new_text(PromptMessageRole::User, text)];
	result
}

#[prompt_router]
impl SurrealKitMcp {
	/// Turn a described schema change into a phased rollout manifest.
	#[prompt(
		name = "author_rollout",
		description = "Write a phased rollout manifest for a schema change, for the cases \
		               `rollout_plan` refuses to plan automatically."
	)]
	pub async fn author_rollout(
		&self,
		Parameters(args): Parameters<AuthorRolloutArgs>,
	) -> GetPromptResult {
		let module = args
			.module
			.as_deref()
			.map(|m| format!(" in the `{m}` schema module"))
			.unwrap_or_default();
		user(format!(
			indoc::indoc! {"
                I want to make this schema change{}: {}

                Write it as a SurrealKit rollout manifest. Work in this order:

                1. Call `project_info` to see the modules and targets, then read the
                   relevant schema files (the `surrealkit://schema/...` resources).
                2. Try `rollout_plan` with dry_run: true first. If it produces the plan I
                   want, say so and stop -- a hand-written manifest is only worth it when
                   automatic planning refuses.
                3. Automatic planning refuses on *modified* entities and on add-and-remove
                   in the same scope, which is exactly the rename-a-field case. If that is
                   what we have, hand-write the manifest.

                Split the work across the three phases and respect what each is for:

                  * `start` is additive and must leave the old shape working. Add the new
                    column, backfill it, add the new index. Code still reading the old
                    shape must not break, because rollback has to stay possible.
                  * `complete` is the destructive half: drop what start superseded. This
                    runs only once the new shape is actually being read.
                  * `rollback` undoes `start`, and has to be written by hand -- it is not
                    inferred.

                Show me the manifest TOML and say which file it should go in. Do not run
                `rollout_start`: I want to read it first.
            "},
			module, args.change
		))
	}

	/// Work out what to do with a rollout that is stuck.
	#[prompt(
		name = "diagnose_rollout",
		description = "Triage a stuck or failed rollout and decide between repair, rollback \
		               and abandon."
	)]
	pub async fn diagnose_rollout(
		&self,
		Parameters(args): Parameters<DiagnoseRolloutArgs>,
	) -> GetPromptResult {
		let which = args
			.rollout_id
			.as_deref()
			.map(|id| format!("rollout `{id}`"))
			.unwrap_or_else(|| "whichever rollouts are not in a terminal state".to_string());
		user(format!(
			indoc::indoc! {"
                Something is wrong with {}. Diagnose it and tell me what to do.

                Start with `rollout_status`. The status tells you most of it:

                  * `running_start` / `running_complete` / `running_rollback` mean a run was
                    interrupted -- killed mid-flight, or the connection dropped. The SQL
                    steps may have partly applied. `rollout_repair` reconciles the recorded
                    metadata without re-running any SQL, so it is the safe first move.
                  * `failed` means a step returned an error. Read `last_error` and the
                    per-step statuses to find which one, and fix the cause before retrying.
                  * `ready_to_complete` is not stuck -- start succeeded and it is waiting
                    for you to decide.

                Rules that matter:

                  * Try `rollout_repair` before anything else. It cannot make a partial
                    schema change worse.
                  * `rollout_rollback` only works before complete has run.
                  * Abandoning a rollout marks it terminal WITHOUT reverting any schema
                    change. It is a last resort, and it leaves the database in whatever
                    half-applied shape it is in.
                  * Rollouts hold an advisory lock with a 900-second TTL. If the error is
                    about a held lock and no run is actually in flight, waiting it out is
                    usually correct.

                Tell me what happened, what state the schema is actually in, and the one
                command you recommend -- do not run a destructive one without asking.
            "},
			which
		))
	}

	/// Read a sync plan before letting it run.
	#[prompt(
		name = "review_sync",
		description = "Review what a sync would change, and judge whether the prune is safe."
	)]
	pub async fn review_sync(
		&self,
		Parameters(args): Parameters<ReviewSyncArgs>,
	) -> GetPromptResult {
		let target =
			args.target.as_deref().map(|t| format!(" against target `{t}`")).unwrap_or_default();
		user(format!(
			indoc::indoc! {"
                Review what `sync`{} would do, before I run it.

                Call `sync` with dry_run: true and read the plan. Then tell me:

                  1. Which files would be applied, and whether any of them changes an
                     existing table's shape rather than only adding to it.
                  2. Every object that would be REMOVED, by name and kind. For each one,
                     say whether it holds data that would go with it.
                  3. Whether anything in that removal list looks like an accident -- a
                     file someone deleted by mistake, or a whole module missing because
                     the folder is wrong. A plan that removes *everything* almost always
                     means the wrong folder, not an intentional teardown.
                  4. Your recommendation: run it, run it with no_prune, or stop and fix
                     something first. If any removal is destructive and intended, say so
                     explicitly rather than burying it.

                If this is a database I cannot afford to lose, say whether a rollout would
                be the better route.
            "},
			target
		))
	}

	/// Write a test suite for a table.
	#[prompt(
		name = "write_test_suite",
		description = "Write a SurrealKit test suite covering a table's shape and permissions."
	)]
	pub async fn write_test_suite(
		&self,
		Parameters(args): Parameters<WriteTestsArgs>,
	) -> GetPromptResult {
		user(format!(
			indoc::indoc! {"
                Write a SurrealKit test suite for the `{}` table.

                First read the table's schema file and an existing suite under
                `<folder>/tests/suites` to match the house style, plus
                `<folder>/tests/config.toml` for the actors already defined.

                A suite is TOML with `[[cases]]`, each with a `kind`:

                  * `sql_expect` runs SurrealQL and asserts on the result, or asserts it
                    is denied (`allow = false`, optionally `error_contains`).
                  * `permissions_matrix` checks select/create/update/delete for an actor
                    against a table in one case -- the most economical way to cover a
                    permissions rule.
                  * `schema_metadata` asserts on `INFO FOR DB`/`INFO FOR TABLE` output.
                  * `schema_behavior` asserts the schema actually enforces something.
                  * `api_request` issues an HTTP request and asserts on status, headers
                    and body.

                Actors come in six kinds: `root`, `namespace`, `database`, `record`,
                `token` and `headers`.

                Cover the shape (required fields, types, defaults), then the permissions
                from at least one allowed and one denied angle -- a permissions test that
                only checks the allowed path proves nothing.

                Credentials go in `*_env` fields naming an environment variable, never
                inline. Show me the suite and which file to put it in.
            "},
			args.table
		))
	}
}
