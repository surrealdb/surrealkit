//! The rollout side of `MIGRATE EXISTING ROWS`: which migrations a rollout's
//! start phase runs, what undoes them, and the steps that revert one or wait for
//! SurrealDB to finish rewriting a table.
//!
//! A migration cannot be undone by restoring the old definition. At
//! compatibility level 4 the server refuses a definition the records no longer
//! fit, and at level 3 it accepts one that leaves them wrong. So for a field the
//! start phase migrates in place, `rollout plan` writes a `revert_migration`
//! step: a second migration, written by the author, that turns the values back.
//! A rollout whose in-place migrations are not all reverted is irreversible, and
//! `rollout rollback` refuses it once the migrating step has run.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use super::{FileRef, LoadedRolloutSpec, RolloutAction, RolloutPhase, RolloutStep, frozen};
use crate::migrate::Migration;
use crate::schema_state::{EntityKey, EntityKind};

/// How long `await_migrations` waits when its step gives no `timeout_secs`.
pub(crate) const AWAIT_DEFAULT_TIMEOUT: Duration = Duration::from_secs(3600);

/// How often `await_migrations` reads the tables' progress.
const AWAIT_POLL: Duration = Duration::from_secs(2);

/// A migration a start-phase step runs, and the step that runs it.
#[derive(Debug, Clone)]
pub(crate) struct StartMigration {
	pub step_id: String,
	pub migration: Migration,
}

/// The migrations the start phase's `apply_schema` and frozen `apply_files`
/// steps carry. A legacy manifest's paths are read from the schema folder when
/// the step runs, so they are not known here.
pub(crate) fn start_migrations(rollout: &LoadedRolloutSpec) -> Result<Vec<StartMigration>> {
	let mut out = Vec::new();
	for step in rollout.spec.steps.iter().filter(|step| step.phase == RolloutPhase::Start) {
		let texts: Vec<String> = match &step.action {
			RolloutAction::ApplySchema {
				sql,
			} => vec![sql.clone()],
			RolloutAction::ApplyFiles {
				files,
			} => files
				.iter()
				.filter_map(|file| match file {
					FileRef::Frozen(frozen) => Some(frozen::read_for_step(rollout, frozen)),
					FileRef::Path(_) => None,
				})
				.collect::<Result<_>>()?,
			_ => continue,
		};
		for text in texts {
			for (_, migration) in crate::migrate::find_all(&text)? {
				out.push(StartMigration {
					step_id: step.id.clone(),
					migration,
				});
			}
		}
	}
	Ok(out)
}

/// The start-phase field migrations nothing in the rollback phase undoes: not
/// reverted by a `revert_migration` step, and not on a field the rollback
/// removes anyway.
pub(crate) fn unreverted(rollout: &LoadedRolloutSpec) -> Result<Vec<StartMigration>> {
	let rollback = || rollout.spec.steps.iter().filter(|step| step.phase == RolloutPhase::Rollback);
	let reverted = |table: &str, field: &str| {
		rollback().any(|step| match &step.action {
			RolloutAction::RevertMigration {
				table: t,
				field: f,
				..
			} => crate::migrate::unquote(t) == table && crate::migrate::unquote(f) == field,
			RolloutAction::RemoveEntities {
				entities,
			} => entities.iter().any(|entity| {
				(entity.kind == EntityKind::Field
					&& entity.scope.as_deref().map(crate::migrate::unquote).as_deref()
						== Some(table) && crate::migrate::unquote(&entity.name) == field)
					|| (entity.kind == EntityKind::Table
						&& crate::migrate::unquote(&entity.name) == table)
			}),
			_ => false,
		})
	};
	Ok(start_migrations(rollout)?
		.into_iter()
		.filter(|item| {
			item.migration.field().is_some_and(|field| {
				!reverted(&item.migration.table, &crate::migrate::unquote(field))
			})
		})
		.collect())
}

/// Refuse a rollout whose migration steps cannot run as written: a
/// `revert_migration` the author has not filled in, or one that reuses the name
/// of the migration it reverts.
pub(crate) fn check_steps(rollout: &LoadedRolloutSpec) -> Result<()> {
	let forward = start_migrations(rollout)?;
	for step in &rollout.spec.steps {
		let RolloutAction::RevertMigration {
			table,
			field,
			name,
			using,
			..
		} = &step.action
		else {
			continue;
		};
		if using.trim().is_empty() {
			bail!(
				"revert_migration step '{}' in rollout '{}' has an empty `using`. Write what a \
				 migrated value of `{table}.{field}` becomes again, over `$value`, or delete the \
				 step to make the rollout irreversible",
				step.id,
				rollout.spec.id
			);
		}
		if forward.iter().any(|f| {
			f.migration.table == crate::migrate::unquote(table)
				&& f.migration.name == crate::migrate::unquote(name)
		}) {
			bail!(
				"revert_migration step '{}' in rollout '{}' is named `{name}`, the name of the \
				 migration it reverts. A table runs a name once, so give the revert a name of its \
				 own",
				step.id,
				rollout.spec.id
			);
		}
	}
	Ok(())
}

/// Refuse to roll back a rollout whose start phase ran a migration nothing in
/// its rollback undoes. `completed` says whether a start step finished.
pub(crate) fn refuse_unrevertable_rollback(
	rollout: &LoadedRolloutSpec,
	completed: impl Fn(&str) -> bool,
) -> Result<()> {
	let ran: Vec<StartMigration> =
		unreverted(rollout)?.into_iter().filter(|item| completed(&item.step_id)).collect();
	if ran.is_empty() {
		return Ok(());
	}
	let names = ran
		.iter()
		.map(|item| {
			format!(
				"`{}` on `{}.{}` (step '{}')",
				item.migration.name,
				item.migration.table,
				item.migration.field().unwrap_or_default(),
				item.step_id
			)
		})
		.collect::<Vec<_>>()
		.join(", ");
	bail!(
		"rollout '{}' cannot be rolled back: its start phase migrated stored values with {names}, \
		 and its rollback has no revert_migration step to turn them back. Restoring the old \
		 definitions alone would leave records that do not fit them. Fix forward with a new \
		 rollout, or add a revert_migration step and plan again",
		rollout.spec.id
	)
}

/// A rollback step that reverts the in-place migration of `table.field`, with
/// a `using` for the author to fill in.
pub(crate) fn revert_step(table: &str, field: &str, forward_name: &str) -> RolloutStep {
	RolloutStep {
		id: format!("revert_{}", step_slug(&format!("{table}_{field}"))),
		phase: RolloutPhase::Rollback,
		action: RolloutAction::RevertMigration {
			table: table.to_string(),
			field: field.to_string(),
			name: format!("{forward_name}_revert"),
			using: String::new(),
			// Written out so the author sees it: SurrealDB refuses a migration
			// without one unless it can prove the transform never fails.
			fallback: Some(String::new()),
		},
	}
}

fn step_slug(text: &str) -> String {
	text.chars()
		.map(|c| {
			if c.is_ascii_alphanumeric() {
				c.to_ascii_lowercase()
			} else {
				'_'
			}
		})
		.collect()
}

/// Splice a `MIGRATE EXISTING ROWS` clause into a field definition, where the
/// grammar puts it: before any `GRAPHQL_ALIAS` or `GRAPHQL_DEPRECATED`.
pub(crate) fn splice_clause(definition: &str, clause: &str) -> Result<String> {
	let statements = crate::surql_scan::scan(definition).map_err(|err| anyhow::anyhow!("{err}"))?;
	let Some(stmt) = statements.first() else {
		bail!("the captured definition is empty");
	};
	let graphql = (0..stmt.tokens.len()).find(|&i| {
		stmt.tokens[i].depth == 0
			&& (stmt.is_word(definition, i, "GRAPHQL_ALIAS")
				|| stmt.is_word(definition, i, "GRAPHQL_DEPRECATED"))
	});
	let at = graphql.map_or(stmt.span.end, |i| stmt.tokens[i - 1].span.end);
	Ok(format!("{} {clause}{}", &definition[stmt.span.start..at], &definition[at..stmt.span.end]))
}

/// The statement a `revert_migration` step runs: the definition `start`
/// captured, with the reverse migration in it.
pub(crate) fn revert_statement(
	captured: &str,
	name: &str,
	using: &str,
	fallback: Option<&str>,
) -> Result<String> {
	let mut clause = format!("MIGRATE EXISTING ROWS AS {} USING {using}", quote_name(name));
	if let Some(fallback) = fallback {
		clause.push_str(&format!(" FALLBACK {fallback}"));
	}
	splice_clause(captured, &clause)
}

fn quote_name(name: &str) -> String {
	let bare = crate::migrate::unquote(name);
	if !bare.is_empty() && bare.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
		bare
	} else {
		crate::rollout::quote_ident(&bare)
	}
}

/// The field a `revert_migration` step needs captured before start.
pub(crate) fn revert_capture(step: &RolloutStep) -> Option<EntityKey> {
	match &step.action {
		RolloutAction::RevertMigration {
			table,
			field,
			..
		} => Some(EntityKey {
			kind: EntityKind::Field,
			scope: Some(table.clone()),
			name: field.clone(),
		}),
		_ => None,
	}
}

/// What one table's `migrations` section says is still under way.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Progress {
	/// Lines saying what is still running.
	pub running: Vec<String>,
	/// Lines saying what finished with records that do not fit.
	pub failed: Vec<String>,
}

/// Read what SurrealDB is still doing to `table` after a migration.
pub(crate) fn progress(table: &str, info: &Value) -> Progress {
	let mut out = Progress::default();
	let Some(section) = info.get("migrations") else {
		return out;
	};
	if let Some(progress) = section.get("progress") {
		let count = |key: &str| progress.get(key).and_then(Value::as_i64).unwrap_or(0);
		out.running.push(format!(
			"`{table}`: rewriting records (lap {}, {} scanned, {} rewritten, {} took the FALLBACK)",
			count("lap"),
			count("scanned"),
			count("rewritten"),
			count("fallbacks")
		));
	}
	for entry in section.get("verification").and_then(Value::as_array).into_iter().flatten() {
		let field = entry.get("field").and_then(Value::as_str).unwrap_or("?");
		let failures = entry.get("failures").and_then(Value::as_i64).unwrap_or(0);
		let finished = entry.get("finished").and_then(Value::as_bool).unwrap_or(false);
		if !finished {
			out.running.push(format!("`{table}.{field}`: checking that every record fits"));
		} else if failures > 0 {
			out.failed
				.push(format!("`{table}.{field}`: {failures} record(s) do not fit its definition"));
		}
	}
	if let Some(undeclared) = section.get("undeclared") {
		let failures = undeclared.get("failures").and_then(Value::as_i64).unwrap_or(0);
		let finished = undeclared.get("finished").and_then(Value::as_bool).unwrap_or(false);
		if !finished {
			out.running.push(format!("`{table}`: checking for fields it does not declare"));
		} else if failures > 0 {
			out.failed.push(format!(
				"`{table}`: {failures} record(s) hold fields the table does not declare; clear them \
				 with ALTER TABLE {table} MIGRATE EXISTING ROWS AS <name> REMOVE UNDECLARED FIELDS"
			));
		}
	}
	out
}

/// Wait until SurrealDB has finished rewriting and checking `tables`.
pub(crate) async fn await_tables(
	db: &Surreal<Any>,
	step_id: &str,
	tables: &[String],
	timeout: Duration,
) -> Result<()> {
	let deadline = Instant::now() + timeout;
	loop {
		let mut running = Vec::new();
		let mut failed = Vec::new();
		for table in tables {
			let bare = crate::migrate::unquote(table);
			let info = super::retry_conflicts(&format!("reading table `{bare}`"), || async {
				super::info_json(db, "INFO FOR TABLE $table;", Some(&bare)).await
			})
			.await?;
			let now = progress(&bare, &info);
			running.extend(now.running);
			failed.extend(now.failed);
		}
		if !failed.is_empty() {
			bail!(
				"step '{step_id}': SurrealDB finished checking, and some records do not fit:\n  - {}",
				failed.join("\n  - ")
			);
		}
		if running.is_empty() {
			return Ok(());
		}
		if Instant::now() >= deadline {
			bail!(
				"step '{step_id}': SurrealDB is still working after {}s:\n  - {}\nRun the step \
				 again to keep waiting, or raise its timeout_secs",
				timeout.as_secs(),
				running.join("\n  - ")
			);
		}
		log::debug!("step '{step_id}': waiting for {}", running.join("; "));
		tokio::time::sleep(AWAIT_POLL).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn splices_the_clause_before_graphql_names() {
		let spliced = splice_clause(
			"DEFINE FIELD price ON item TYPE string GRAPHQL_ALIAS 'cost' PERMISSIONS FULL",
			"MIGRATE EXISTING ROWS AS r USING <string> $value",
		)
		.unwrap();
		assert_eq!(
			spliced,
			"DEFINE FIELD price ON item TYPE string MIGRATE EXISTING ROWS AS r USING <string> \
			 $value GRAPHQL_ALIAS 'cost' PERMISSIONS FULL"
		);
	}

	#[test]
	fn splices_the_clause_at_the_end_otherwise() {
		let spliced = revert_statement(
			"DEFINE FIELD price ON item TYPE string PERMISSIONS FULL",
			"price to string",
			"<string> $value",
			Some("''"),
		)
		.unwrap();
		assert_eq!(
			spliced,
			"DEFINE FIELD price ON item TYPE string PERMISSIONS FULL MIGRATE EXISTING ROWS AS \
			 `price to string` USING <string> $value FALLBACK ''"
		);
	}

	#[test]
	fn reads_progress_and_failures() {
		let info = serde_json::json!({
			"migrations": {
				"progress": { "lap": 2, "scanned": 10, "rewritten": 4, "fallbacks": 1 },
				"verification": [
					{ "field": "price", "failures": 0, "finished": false },
					{ "field": "stock", "failures": 3, "finished": true },
				],
			}
		});
		let now = progress("item", &info);
		assert_eq!(now.running.len(), 2, "{now:?}");
		assert_eq!(now.failed, vec!["`item.stock`: 3 record(s) do not fit its definition"]);
		assert_eq!(progress("item", &serde_json::json!({})), Progress::default());
	}
}
