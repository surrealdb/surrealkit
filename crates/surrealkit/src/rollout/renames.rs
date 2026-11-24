//! Field renames that `rollout plan` can propose.
//!
//! SurrealDB 4.0 cannot rename a field: `ALTER FIELD` and `ALTER TABLE` have no
//! rename clause, and `MIGRATE EXISTING ROWS ... USING` reads only the field's
//! own `$value`. Moving the values to a new name is an `UPDATE` that copies
//! them, run while both fields exist.
//!
//! So when a plan removes one field and adds another on the same table, and the
//! two definitions are the same apart from the name, `plan` adds an
//! expand-phase `run_sql` step with that copy, and records the pair in the
//! manifest's `renames` with the step's id and `confirmed = false`. The
//! pairing is inferred, so `rollout lint`, `rollout start` and `rollout up`
//! refuse the manifest until the author sets `confirmed = true`. They also
//! refuse an entry whose copy step is gone: deleting the step must not leave a
//! plan that adds the new field empty and removes the old one with its values,
//! which `plan` refused before it could propose renames. The old field is still
//! removed in the contract phase, like any other removal.
//!
//! An author who reads the pair as no rename discards the rollout and plans the
//! addition and the removal separately, as the refusal always advised.
//!
//! Before rename proposals, `renames` was a reserved field of four keys that
//! execution ignored. An entry with no `step` that is not confirmed is such a
//! hint: it is still ignored, with a warning, and serializes as it did, so a
//! rollout that carries one keeps its checksum.
//!
//! Only a `SCHEMAFULL` table whose own definition this rollout leaves alone gets
//! a proposal. A schemaless table can already hold values under the new name,
//! which the copy would overwrite, and removing a field there does not clear its
//! values, so a rollback would leave the copies behind. Neither can be undone
//! without guessing which values were there first.
//!
//! Anything less certain proposes nothing, and the plan is refused as an add
//! and a remove that could be a rename, as it was before.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};

use super::{RolloutAction, RolloutPhase, RolloutRename, RolloutSpec, RolloutStep, slugify};
use crate::schema_state::{CatalogDiff, CatalogEntity, EntityKey, EntityKind, SchemaFile};

/// `RolloutRename::kind` for a field.
pub(crate) const FIELD: &str = "field";

/// One removed field and the added field it becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FieldRename {
	pub table: String,
	pub from: String,
	pub to: String,
}

impl FieldRename {
	fn old_key(&self) -> EntityKey {
		field_key(&self.table, &self.from)
	}

	fn new_key(&self) -> EntityKey {
		field_key(&self.table, &self.to)
	}

	/// The manifest's record of the pair, copied by the step `step`, and not
	/// yet confirmed.
	pub(crate) fn record(&self, step: &str) -> RolloutRename {
		RolloutRename::field(&self.table, &self.from, &self.to).copied_by(step)
	}

	/// The copy. It touches only records that hold a value, and running it
	/// again copies the same values, so a retried step is harmless.
	pub(crate) fn sql(&self) -> String {
		format!(
			"UPDATE {table} SET {to} = {from} WHERE {from} != NONE;",
			table = self.table,
			from = self.from,
			to = self.to
		)
	}
}

fn field_key(table: &str, name: &str) -> EntityKey {
	EntityKey {
		kind: EntityKind::Field,
		scope: Some(table.to_string()),
		name: name.to_string(),
	}
}

/// Whether `name` is one identifier, bare or quoted: a field at the top of the
/// record. A nested field (`address.city`, `tags[*]`) is copied with its
/// parent, so it is never paired on its own.
fn is_top_level(name: &str) -> bool {
	use crate::surql_scan::TokKind;
	matches!(
		crate::surql_scan::scan(name).as_deref(),
		Ok([stmt]) if matches!(stmt.tokens.as_slice(), [tok] if matches!(tok.kind, TokKind::Word | TokKind::QuotedIdent))
	)
}

/// Why an `UPDATE` cannot copy a field with this clause as it was.
fn uncopyable_reason(clause: &str) -> &'static str {
	match clause {
		"READONLY" => "SurrealDB refuses to write a READONLY field once its record exists",
		"VALUE" => {
			"an UPDATE evaluates the VALUE clause again, so the copy would not always hold the old \
			 values"
		}
		_ => "a COMPUTED field stores no values to copy",
	}
}

/// The renames this diff proposes: pairs of a removed and an added field on
/// one `SCHEMAFULL` table whose definitions are the same apart from the name,
/// where no other pairing is possible and the table's own definition does not
/// change. Pairs it cannot propose are logged with the reason.
pub(crate) fn find(diff: &CatalogDiff, files: &[SchemaFile]) -> Result<Vec<FieldRename>> {
	let by_table = |entities: &[CatalogEntity]| {
		let mut out: BTreeMap<String, Vec<CatalogEntity>> = BTreeMap::new();
		for entity in entities.iter().filter(|e| e.kind == EntityKind::Field) {
			if let Some(table) = &entity.scope
				&& is_top_level(&entity.name)
			{
				out.entry(table.clone()).or_default().push(entity.clone());
			}
		}
		out
	};
	let removed = by_table(&diff.removed);
	let added = by_table(&diff.added);

	let mut proposals = Vec::new();
	for (table, removed) in &removed {
		let Some(added) = added.get(table) else {
			continue;
		};
		// Every pairing whose definitions match, removed name -> added names.
		let mut candidates: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
		let mut uncopyable: BTreeMap<(&str, &str), &'static str> = BTreeMap::new();
		for new in added {
			let Some(file) = files.iter().find(|file| file.path == new.source_path) else {
				continue;
			};
			for old in removed {
				let Some(view) =
					crate::schema_state::field_under_name(file, table, &new.name, &old.name)?
				else {
					continue;
				};
				if view.statement_hash != old.statement_hash {
					continue;
				}
				candidates.entry(old.name.as_str()).or_default().insert(new.name.as_str());
				if let Some(clause) = view.uncopyable {
					uncopyable.insert((old.name.as_str(), new.name.as_str()), clause);
				}
			}
		}
		if candidates.is_empty() {
			continue;
		}

		let mut claimed: BTreeMap<&str, usize> = BTreeMap::new();
		for to in candidates.values().flatten() {
			*claimed.entry(to).or_default() += 1;
		}
		let ambiguous =
			candidates.values().any(|to| to.len() > 1) || claimed.values().any(|n| *n > 1);
		if ambiguous {
			log::warn!(
				"fields of `{table}` could be renamed in more than one way, so `rollout plan` \
				 proposes none of them: {}. Rename one field per rollout, or write the copies by hand",
				candidates
					.iter()
					.map(|(from, to)| format!(
						"`{from}` could be {}",
						to.iter().map(|name| format!("`{name}`")).collect::<Vec<_>>().join(" or ")
					))
					.collect::<Vec<_>>()
					.join("; ")
			);
			continue;
		}

		let unsafe_table = if diff
			.modified
			.iter()
			.any(|change| change.new.kind == EntityKind::Table && change.new.name == *table)
		{
			Some(
				"its own definition changes in this rollout, so `rollout plan` cannot tell whether it \
				 was SCHEMAFULL before",
			)
		} else if !crate::schema_state::table_is_schemafull(files, table)? {
			Some("it is not SCHEMAFULL")
		} else {
			None
		};

		for (from, to) in &candidates {
			let Some(to) = to.first() else {
				continue;
			};
			if let Some(why) = unsafe_table {
				log::warn!(
					"`{table}.{from}` and `{table}.{to}` are the same field apart from the name, but \
					 `rollout plan` proposes no rename on `{table}`: {why}. A schemaless table can \
					 already hold values under `{to}`, which the copy would overwrite, and removing a \
					 field there does not clear its values, so a rollback would leave the copies \
					 behind. Plan the rename by hand"
				);
				continue;
			}
			if let Some(clause) = uncopyable.get(&(*from, *to)) {
				log::warn!(
					"`{table}.{from}` and `{table}.{to}` are the same field apart from the name, but \
					 `rollout plan` proposes no copy for a {clause} field: {}",
					uncopyable_reason(clause)
				);
				continue;
			}
			proposals.push(FieldRename {
				table: table.clone(),
				from: (*from).to_string(),
				to: (*to).to_string(),
			});
		}
	}
	Ok(proposals)
}

/// Every key a proposal accounts for, removed and added.
pub(crate) fn paired_keys(renames: &[FieldRename]) -> BTreeSet<EntityKey> {
	renames.iter().flat_map(|rename| [rename.old_key(), rename.new_key()]).collect()
}

/// The id of the copy step for `rename`, unique among `taken`.
fn step_id(rename: &FieldRename, taken: &BTreeSet<String>) -> String {
	let base = format!(
		"copy_{}_{}_to_{}",
		slugify(&rename.table),
		slugify(&rename.from),
		slugify(&rename.to)
	);
	if !taken.contains(&base) {
		return base;
	}
	(2..).map(|n| format!("{base}_{n}")).find(|id| !taken.contains(id)).unwrap_or(base)
}

/// Record the proposals in `spec`, unconfirmed, and add their copy steps after
/// the start phase's other steps, so the new fields exist when they run.
pub(crate) fn apply(spec: &mut RolloutSpec, renames: &[FieldRename]) {
	let mut taken: BTreeSet<String> = spec.steps.iter().map(|step| step.id.clone()).collect();
	let at = spec
		.steps
		.iter()
		.rposition(|step| step.phase == RolloutPhase::Start)
		.map_or(0, |idx| idx + 1);
	let mut steps = Vec::with_capacity(renames.len());
	for rename in renames {
		let id = step_id(rename, &taken);
		taken.insert(id.clone());
		log::warn!(
			"`rollout plan` reads `{table}.{from}` and `{table}.{to}` as a rename: step '{id}' \
			 copies the values in the start phase, and the complete phase removes `{from}`. The \
			 rename is unconfirmed: check the step's SQL, then set `confirmed = true` on its \
			 `[[renames]]` entry. If it is not a rename, run `surrealkit rollout discard {rollout}` \
			 and plan the addition and the removal in separate rollouts. Records the old \
			 application writes after the copy keep that write only in `{from}`",
			table = rename.table,
			from = rename.from,
			to = rename.to,
			rollout = spec.id
		);
		spec.renames.push(rename.record(&id));
		steps.push(RolloutStep::run_sql(id, RolloutPhase::Start, rename.sql()));
	}
	spec.steps.splice(at..at, steps);
}

/// Refuse a rollout whose renames cannot run as recorded: an entry the author
/// has not confirmed, or one whose copy step is gone. `lint` and every way of
/// starting a rollout call this, as they refuse an empty `revert_migration`.
///
/// A legacy hint, an entry with no `step` that is not confirmed, is what
/// releases before rename proposals carried and never ran. It is not refused:
/// it is ignored, as it always was, with a warning.
pub(crate) fn check_renames(spec: &RolloutSpec) -> Result<()> {
	for rename in &spec.renames {
		let what = match &rename.scope {
			Some(scope) => {
				format!("the rename of `{scope}.{}` to `{scope}.{}`", rename.from, rename.to)
			}
			None => format!("the rename of `{}` to `{}`", rename.from, rename.to),
		};
		if rename.is_legacy_hint() {
			log::warn!(
				"rollout '{rollout}' records {what} as a legacy rename hint: a `[[renames]]` entry \
				 with no `step`, as releases before rename proposals wrote it. Nothing runs it, as \
				 before; the rollout copies no values for it",
				rollout = spec.id
			);
			continue;
		}
		let copy = rename.step.as_deref().and_then(|id| spec.steps.iter().find(|s| s.id == id));
		let copies = copy.is_some_and(|step| {
			step.phase == RolloutPhase::Start && matches!(step.action, RolloutAction::RunSql { .. })
		});
		if !copies {
			bail!(
				"rollout '{rollout}' records {what}, but {}. Without its copy the rollout would add \
				 the new field empty and remove the old one, with its values. If it is a rename, \
				 put the copy step back. If it is not, run `surrealkit rollout discard {rollout}` \
				 and plan the addition and the removal in separate rollouts",
				match (&rename.step, copy) {
					(Some(id), None) => format!("its copy step '{id}' is not in the rollout"),
					(Some(id), Some(_)) =>
						format!("its copy step '{id}' is not a start-phase run_sql step"),
					(None, _) => "names no copy step".to_string(),
				},
				rollout = spec.id
			);
		}
		if !rename.confirmed {
			bail!(
				"{what} in rollout '{rollout}' is unconfirmed. `rollout plan` inferred it from two \
				 definitions that differ only in the name. Check that step '{}' copies what you \
				 mean, then set `confirmed = true` on its `[[renames]]` entry. If it is not a \
				 rename, run `surrealkit rollout discard {rollout}` and plan the addition and the \
				 removal in separate rollouts",
				rename.step.as_deref().unwrap_or_default(),
				rollout = spec.id
			);
		}
	}
	Ok(())
}

/// Refuse a rollout with a confirmed rename on a database below compatibility
/// level 4. There `REMOVE FIELD` leaves a field's values in place, so a
/// rollback could not undo the copy: the copies would stay under the new name,
/// and a value a record already held there would be gone. `start`, `up` and
/// [`super::Rollout::start`] call this; `plan` and `lint` do not connect.
pub(crate) fn check_level(spec: &RolloutSpec, level: i64) -> Result<()> {
	if level >= 4 {
		return Ok(());
	}
	let pairs: Vec<String> = spec
		.renames
		.iter()
		.filter(|rename| rename.confirmed)
		.map(|rename| match &rename.scope {
			Some(scope) => format!("`{scope}.{}` to `{scope}.{}`", rename.from, rename.to),
			None => format!("`{}` to `{}`", rename.from, rename.to),
		})
		.collect();
	if pairs.is_empty() {
		return Ok(());
	}
	bail!(
		"rollout '{rollout}' renames {pairs}, and this database is at compatibility level {level}. \
		 Below level 4 `REMOVE FIELD` does not clear a field's values, so a rollback could not \
		 undo the copy: the copies would stay under the new name, and a value a record already \
		 held there would be lost. Raise the database to level 4 with `surrealkit compat raise \
		 --yes --wait` once every node runs SurrealDB 4.0, then start the rollout again",
		pairs = pairs.join(", "),
		rollout = spec.id
	);
}

/// [`check_level`] against the connected database's level. Reads the level
/// only for a rollout that carries a confirmed rename.
pub(crate) async fn check_database_level(
	db: &surrealdb::Surreal<surrealdb::engine::any::Any>,
	spec: &RolloutSpec,
) -> Result<()> {
	if !spec.renames.iter().any(|rename| rename.confirmed) {
		return Ok(());
	}
	let info = super::retry_conflicts("reading the database", || async {
		super::info_json(db, "INFO FOR DB;", None).await
	})
	.await?;
	check_level(spec, crate::migrate::compatibility(&info))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn file(sql: &str) -> SchemaFile {
		SchemaFile {
			path: "schema/person.surql".into(),
			sql: sql.into(),
			hash: "h".into(),
		}
	}

	fn diff(old: &str, new: &str) -> (CatalogDiff, Vec<SchemaFile>) {
		let old = [file(old)];
		let new = [file(new)];
		let diff = crate::schema_state::diff_catalog(
			&crate::schema_state::build_catalog_snapshot(&old, false).unwrap(),
			&crate::schema_state::build_catalog_snapshot(&new, false).unwrap(),
		);
		(diff, new.to_vec())
	}

	/// The proposals for `old` to `new`, where each table they name is defined
	/// `SCHEMAFULL` in both.
	fn proposals(old: &str, new: &str) -> Vec<(String, String, String)> {
		const TABLES: &str = "DEFINE TABLE person SCHEMAFULL; DEFINE TABLE contact SCHEMAFULL; \
		                      DEFINE TABLE computed SCHEMAFULL;\n";
		proposals_as_written(&format!("{TABLES}{old}"), &format!("{TABLES}{new}"))
	}

	fn proposals_as_written(old: &str, new: &str) -> Vec<(String, String, String)> {
		let (diff, files) = diff(old, new);
		find(&diff, &files).unwrap().into_iter().map(|r| (r.table, r.from, r.to)).collect()
	}

	fn pair(table: &str, from: &str, to: &str) -> (String, String, String) {
		(table.into(), from.into(), to.into())
	}

	#[test]
	fn a_field_with_only_a_new_name_is_a_rename() {
		assert_eq!(
			proposals(
				"DEFINE FIELD email ON person TYPE string ASSERT string::is_email($value);",
				"DEFINE FIELD mail ON person TYPE string ASSERT string::is_email($value);"
			),
			vec![pair("person", "email", "mail")]
		);
		// Whitespace and comments are not part of the definition; a migration
		// clause is left out of the hash, which level 4 needs on a new field.
		assert_eq!(
			proposals(
				"DEFINE FIELD email ON TABLE person TYPE string;",
				"-- renamed\nDEFINE FIELD   mail ON TABLE person\n  TYPE string\n  MIGRATE EXISTING \
				 ROWS AS add_mail USING $value FALLBACK '';"
			),
			vec![pair("person", "email", "mail")]
		);
		// Quoted names are kept as written, so the copy's SQL reads them.
		assert_eq!(
			proposals(
				"DEFINE FIELD `e-mail` ON person TYPE string;",
				"DEFINE FIELD `mail address` ON person TYPE string;"
			),
			vec![pair("person", "`e-mail`", "`mail address`")]
		);
	}

	#[test]
	fn anything_else_is_not() {
		for (old, new) in [
			// Another definition.
			("DEFINE FIELD email ON person TYPE string;", "DEFINE FIELD mail ON person TYPE int;"),
			(
				"DEFINE FIELD email ON person TYPE string;",
				"DEFINE FIELD OVERWRITE mail ON person TYPE string;",
			),
			// Another table.
			(
				"DEFINE FIELD email ON person TYPE string;",
				"DEFINE FIELD mail ON contact TYPE string;",
			),
			// The name inside the definition changed with it.
			(
				"DEFINE FIELD email ON person TYPE string VALUE string::lowercase(email);",
				"DEFINE FIELD mail ON person TYPE string VALUE string::lowercase(mail);",
			),
			// Nested fields go with their parent.
			(
				"DEFINE FIELD a ON person TYPE object; DEFINE FIELD a.city ON person TYPE string;",
				"DEFINE FIELD a ON person TYPE object; DEFINE FIELD a.town ON person TYPE string;",
			),
			// Clauses an UPDATE cannot carry.
			(
				"DEFINE FIELD email ON person TYPE string READONLY;",
				"DEFINE FIELD mail ON person TYPE string READONLY;",
			),
			(
				"DEFINE FIELD at ON person TYPE datetime VALUE time::now();",
				"DEFINE FIELD seen ON person TYPE datetime VALUE time::now();",
			),
			("DEFINE FIELD n ON person COMPUTED 1;", "DEFINE FIELD m ON person COMPUTED 1;"),
			// Two ways to pair.
			(
				"DEFINE FIELD home ON person TYPE string; DEFINE FIELD work ON person TYPE string;",
				"DEFINE FIELD private ON person TYPE string; DEFINE FIELD office ON person TYPE string;",
			),
			(
				"DEFINE FIELD home ON person TYPE string;",
				"DEFINE FIELD private ON person TYPE string; DEFINE FIELD office ON person TYPE string;",
			),
			(
				"DEFINE FIELD home ON person TYPE string; DEFINE FIELD work ON person TYPE string;",
				"DEFINE FIELD office ON person TYPE string;",
			),
		] {
			assert_eq!(proposals(old, new), vec![], "{old} -> {new}");
		}
	}

	/// A schemaless table can already hold values under the new name, and
	/// removing a field there leaves its values, so only a `SCHEMAFULL` table
	/// whose definition stays as it was gets a proposal.
	#[test]
	fn only_a_schemafull_table_that_keeps_its_definition_gets_a_proposal() {
		let fields = |name: &str| format!("DEFINE FIELD {name} ON person TYPE string;");
		for (old_table, new_table) in [
			("DEFINE TABLE person SCHEMALESS;", "DEFINE TABLE person SCHEMALESS;"),
			("DEFINE TABLE person;", "DEFINE TABLE person;"),
			("", ""),
			("DEFINE TABLE person SCHEMALESS;", "DEFINE TABLE person SCHEMAFULL;"),
			("DEFINE TABLE person SCHEMAFULL;", "DEFINE TABLE person SCHEMALESS;"),
			("DEFINE TABLE person SCHEMAFULL;", "DEFINE TABLE person SCHEMAFULL COMMENT 'p';"),
			// A view's SELECT is not the table's own clause.
			(
				"DEFINE TABLE person AS SELECT * FROM schemafull;",
				"DEFINE TABLE person AS SELECT * FROM schemafull;",
			),
		] {
			assert_eq!(
				proposals_as_written(
					&format!("{old_table}\n{}", fields("email")),
					&format!("{new_table}\n{}", fields("mail"))
				),
				vec![],
				"{old_table} -> {new_table}"
			);
		}
		for table in [
			"DEFINE TABLE person SCHEMAFULL;",
			"DEFINE TABLE OVERWRITE person TYPE NORMAL schemafull;",
		] {
			assert_eq!(
				proposals_as_written(
					&format!("{table}\n{}", fields("email")),
					&format!("{table}\n{}", fields("mail"))
				),
				vec![pair("person", "email", "mail")],
				"{table}"
			);
		}
	}

	#[test]
	fn a_field_or_table_named_like_a_clause_is_not_a_clause() {
		assert_eq!(
			proposals(
				"DEFINE FIELD value ON computed TYPE string;",
				"DEFINE FIELD amount ON computed TYPE string;"
			),
			vec![pair("computed", "value", "amount")]
		);
	}

	#[test]
	fn unambiguous_pairs_beside_unrelated_changes_are_proposed() {
		assert_eq!(
			proposals(
				"DEFINE FIELD email ON person TYPE string; DEFINE FIELD age ON person TYPE int;",
				"DEFINE FIELD mail ON person TYPE string; DEFINE FIELD years ON person TYPE int;"
			),
			vec![pair("person", "age", "years"), pair("person", "email", "mail")]
		);
	}

	#[test]
	fn a_confirmed_rename_needs_level_4() {
		let rename = FieldRename {
			table: "person".into(),
			from: "email".into(),
			to: "mail".into(),
		};
		let mut spec = RolloutSpec::builder("20260101000000__r").build();
		spec.renames.push(rename.record("copy_person_email_to_mail"));
		// Unconfirmed is `check_renames`' refusal, not this one.
		check_level(&spec, 3).unwrap();
		spec.renames[0].confirmed = true;
		check_level(&spec, 4).unwrap();
		check_level(&spec, 5).unwrap();
		let err = check_level(&spec, 3).unwrap_err().to_string();
		assert!(
			err.contains("rollout '20260101000000__r' renames `person.email` to `person.mail`")
				&& err.contains("compatibility level 3")
				&& err.contains("surrealkit compat raise"),
			"{err}"
		);
		spec.renames.clear();
		check_level(&spec, 3).unwrap();
	}

	#[test]
	fn the_copy_runs_after_the_schema_step_and_is_unconfirmed() {
		let rename = FieldRename {
			table: "person".into(),
			from: "email".into(),
			to: "mail".into(),
		};
		let mut spec = RolloutSpec::builder("20260101000000__r")
			.step(RolloutStep::apply_files(
				"apply_expand_schema",
				RolloutPhase::Start,
				["schema/a"],
			))
			.step(RolloutStep::remove_entities(
				"rollback_expand_schema",
				RolloutPhase::Rollback,
				vec![rename.new_key()],
			))
			.step(RolloutStep::remove_entities(
				"copy_person_email_to_mail",
				RolloutPhase::Complete,
				vec![rename.old_key()],
			))
			.build();
		apply(&mut spec, std::slice::from_ref(&rename));
		let ids: Vec<&str> = spec.steps.iter().map(|s| s.id.as_str()).collect();
		assert_eq!(
			ids,
			[
				"apply_expand_schema",
				"copy_person_email_to_mail_2",
				"rollback_expand_schema",
				"copy_person_email_to_mail"
			]
		);
		// The copy is an ordinary run_sql step; the entry carries the rest.
		assert_eq!(
			spec.steps[1],
			RolloutStep::run_sql("copy_person_email_to_mail_2", RolloutPhase::Start, rename.sql())
		);
		assert_eq!(spec.renames, vec![rename.record("copy_person_email_to_mail_2")]);
		let err = check_renames(&spec).unwrap_err().to_string();
		assert!(
			err.contains("is unconfirmed") && err.contains("'copy_person_email_to_mail_2'"),
			"{err}"
		);

		spec.renames[0].confirmed = true;
		check_renames(&spec).unwrap();

		// Confirmed or not, an entry stands only with a start-phase run_sql copy.
		let refusal = |spec: &RolloutSpec| check_renames(spec).unwrap_err().to_string();
		let mut wrong = spec.clone();
		wrong.renames[0].step = Some("copy_person_email_to_mail".into());
		assert!(
			refusal(&wrong).contains("is not a start-phase run_sql step"),
			"{}",
			refusal(&wrong)
		);
		let mut missing = spec.clone();
		missing.steps.remove(1);
		assert!(refusal(&missing).contains("is not in the rollout"), "{}", refusal(&missing));
		assert!(
			refusal(&missing).contains("rollout discard 20260101000000__r"),
			"{}",
			refusal(&missing)
		);
		// A confirmed entry with no step is not a legacy hint: refused.
		let mut unnamed = spec.clone();
		unnamed.renames[0].step = None;
		assert!(unnamed.renames[0].confirmed);
		assert!(refusal(&unnamed).contains("names no copy step"), "{}", refusal(&unnamed));
		// Without the step and unconfirmed, it is a hint as releases before
		// rename proposals wrote it, which nothing ran: ignored, and serialized
		// with neither field.
		unnamed.renames[0].confirmed = false;
		assert!(unnamed.renames[0].is_legacy_hint());
		check_renames(&unnamed).unwrap();
		assert_eq!(
			toml::to_string(&unnamed.renames[0]).unwrap(),
			"kind = \"field\"\nscope = \"person\"\nfrom = \"email\"\nto = \"mail\"\n"
		);
		// With a step, `confirmed` is written whatever its value, so the author
		// sees what to change.
		assert!(
			toml::to_string(&rename.record("copy")).unwrap().ends_with("confirmed = false\n"),
			"{}",
			toml::to_string(&rename.record("copy")).unwrap()
		);
	}
}
