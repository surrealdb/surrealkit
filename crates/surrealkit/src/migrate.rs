//! SurrealDB 4.0's `MIGRATE EXISTING ROWS`, as SurrealKit applies it.
//!
//! A `DEFINE FIELD` can say what the records a table already holds become:
//!
//! ```surql
//! DEFINE FIELD stock ON product TYPE int
//!     MIGRATE EXISTING ROWS AS stock_to_int USING <int> $value FALLBACK 0;
//! ```
//!
//! and `ALTER TABLE t MIGRATE EXISTING ROWS AS n REMOVE UNDECLARED FIELDS` clears
//! the fields a table no longer declares. Each migration is named, and a table
//! runs a name once. The clause belongs in the schema file next to the
//! definition it describes, where sync and rollouts re-apply it.
//!
//! Re-applying it as written does not work, for two reasons the server gives:
//!
//! - Once a table has run a name, a `DEFINE FIELD` carrying that name writes
//!   nothing at all. A later change to the field's `PERMISSIONS` or `COMMENT`
//!   would be dropped without a word.
//! - A field an index reads cannot be migrated, and that check comes before the
//!   check for a name already run, so restating the clause is refused for as long
//!   as the index exists.
//!
//! So [`apply`] reads which names each table has run and sends a statement whose
//! name has run without its clause. For a name that has not run, it removes the
//! indexes that read the field, applies the file and defines those indexes again.
//! SurrealKit also records the clause it ran under each name, in the `migration`
//! partition of `__entity`, so a name reused for another transform is refused
//! here instead of being skipped by the server.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use surrealdb::Surreal;
use surrealdb::engine::any::Any;

use crate::core::sha256_hex;
use crate::module::{Module, Partition};
use crate::surql_scan::{Stmt, TokKind};

/// What a migration does to the records stored before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MigrationStep {
	/// `DEFINE FIELD f ON t ... MIGRATE EXISTING ROWS AS n USING e [FALLBACK v]`.
	Rewrite {
		/// The field, as the statement names it.
		field: String,
		/// The transform, over `$value`.
		using: String,
		/// The value stored when the transform fails.
		fallback: Option<String>,
	},
	/// `ALTER TABLE t MIGRATE EXISTING ROWS AS n REMOVE UNDECLARED FIELDS`.
	RemoveUndeclared,
}

/// A `MIGRATE EXISTING ROWS` clause and the statement that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Migration {
	/// The table, without quotes.
	pub table: String,
	/// The migration's name, without quotes. A table runs a name once.
	pub name: String,
	pub step: MigrationStep,
	/// The bytes to cut to leave the statement without its clause: from the end
	/// of the token before `MIGRATE` to the end of the clause.
	pub cut: Range<usize>,
	/// The bytes of the name after `AS`, as written.
	pub name_span: Range<usize>,
	/// SHA-256 of the clause, comments dropped and whitespace collapsed.
	pub clause_hash: String,
	/// 1-based line of the statement.
	pub line: u32,
}

impl Migration {
	/// The field this migration rewrites, if it rewrites one.
	pub fn field(&self) -> Option<&str> {
		match &self.step {
			MigrationStep::Rewrite {
				field,
				..
			} => Some(field),
			MigrationStep::RemoveUndeclared => None,
		}
	}
}

/// The `MIGRATE EXISTING ROWS` clause of `stmt`, if it has one.
///
/// The clause's keywords stay ordinary identifiers everywhere else, so only the
/// run `MIGRATE EXISTING ROWS` outside any bracket starts one. An `Err` is a
/// clause SurrealDB would not parse, worded for the user.
pub(crate) fn find(stmt: &Stmt, src: &str) -> Result<Option<Migration>, String> {
	let top = |i: usize| stmt.tokens.get(i).is_some_and(|tok| tok.depth == 0);
	let starts_at = |i: usize| {
		top(i)
			&& stmt.is_word(src, i, "MIGRATE")
			&& stmt.is_word(src, i + 1, "EXISTING")
			&& stmt.is_word(src, i + 2, "ROWS")
	};
	let Some(start) = (0..stmt.tokens.len()).find(|&i| starts_at(i)) else {
		return Ok(None);
	};
	if (start + 3..stmt.tokens.len()).any(starts_at) {
		return Err("a statement takes one MIGRATE EXISTING ROWS clause".to_string());
	}
	if !stmt.is_word(src, start + 3, "AS") {
		return Err("MIGRATE EXISTING ROWS needs AS <name>".to_string());
	}
	let name_tok = stmt
		.tokens
		.get(start + 4)
		.filter(|tok| matches!(tok.kind, TokKind::Word | TokKind::QuotedIdent))
		.ok_or("MIGRATE EXISTING ROWS AS needs a name")?;
	let name = unquote(&src[name_tok.span.clone()]);

	let define_field = stmt.is_word(src, 0, "DEFINE") && stmt.is_word(src, 1, "FIELD");
	let alter_field = stmt.is_word(src, 0, "ALTER") && stmt.is_word(src, 1, "FIELD");
	let alter_table = stmt.is_word(src, 0, "ALTER") && stmt.is_word(src, 1, "TABLE");
	let (step, last) = if define_field || alter_field {
		if !stmt.is_word(src, start + 5, "USING") {
			return Err(format!("migration `{name}` needs USING <expression over $value>"));
		}
		let ends_using = |i: usize| {
			top(i)
				&& ["FALLBACK", "GRAPHQL_ALIAS", "GRAPHQL_DEPRECATED"]
					.iter()
					.any(|word| stmt.is_word(src, i, word))
		};
		let using_from = start + 6;
		let using_to = (using_from..stmt.tokens.len()).find(|&i| ends_using(i));
		let using_end = using_to.unwrap_or(stmt.tokens.len());
		if using_end == using_from {
			return Err(format!("migration `{name}` has an empty USING"));
		}
		let using = token_text(stmt, src, using_from..using_end);
		let (fallback, last) = match using_to {
			Some(at) if stmt.is_word(src, at, "FALLBACK") => {
				let ends_fallback = |i: usize| {
					top(i)
						&& (stmt.is_word(src, i, "GRAPHQL_ALIAS")
							|| stmt.is_word(src, i, "GRAPHQL_DEPRECATED"))
				};
				let from = at + 1;
				let to = (from..stmt.tokens.len()).find(|&i| ends_fallback(i));
				let end = to.unwrap_or(stmt.tokens.len());
				if end == from {
					return Err(format!("migration `{name}` has an empty FALLBACK"));
				}
				(Some(token_text(stmt, src, from..end)), end - 1)
			}
			_ => (None, using_end - 1),
		};
		let field = field_name(stmt, src).ok_or("cannot read the field's name")?;
		(
			MigrationStep::Rewrite {
				field,
				using,
				fallback,
			},
			last,
		)
	} else if alter_table {
		if !(stmt.is_word(src, start + 5, "REMOVE")
			&& stmt.is_word(src, start + 6, "UNDECLARED")
			&& stmt.is_word(src, start + 7, "FIELDS"))
		{
			return Err(format!(
				"migration `{name}` on a table must be REMOVE UNDECLARED FIELDS, the only table \
				 migration SurrealDB has"
			));
		}
		(MigrationStep::RemoveUndeclared, start + 7)
	} else {
		return Err(
			"MIGRATE EXISTING ROWS belongs on DEFINE FIELD, ALTER FIELD or ALTER TABLE".to_string()
		);
	};

	let table = table_name(stmt, src).ok_or("cannot read the table's name")?;
	let clause = stmt.tokens[start].span.start..stmt.tokens[last].span.end;
	let cut_start = start.checked_sub(1).map_or(clause.start, |prev| stmt.tokens[prev].span.end);
	Ok(Some(Migration {
		table,
		name,
		step,
		cut: cut_start..clause.end,
		name_span: name_tok.span.clone(),
		clause_hash: sha256_hex(collapse(&without_comments(stmt, src, clause)).as_bytes()),
		line: stmt.line,
	}))
}

/// Every migration in `sql`, with the index of the statement that carries it.
pub(crate) fn find_all(sql: &str) -> Result<Vec<(usize, Migration)>> {
	let statements = crate::surql_scan::scan(sql).map_err(|err| anyhow!("{err}"))?;
	let mut out = Vec::new();
	for (idx, stmt) in statements.iter().enumerate() {
		match find(stmt, sql) {
			Ok(Some(migration)) => out.push((idx, migration)),
			Ok(None) => {}
			Err(reason) => bail!("line {}: {reason}", stmt.line),
		}
	}
	Ok(out)
}

/// Whether `stmt` is the one `ALTER` a schema file may hold:
/// `ALTER TABLE [IF EXISTS] t MIGRATE EXISTING ROWS AS n REMOVE UNDECLARED FIELDS`.
///
/// Any other `ALTER` changes a definition the catalog would not see, so it
/// belongs in a rollout step. This one changes no definition: it is a named
/// migration of the table's records, run once.
pub(crate) fn is_table_migration(stmt: &Stmt, src: &str) -> bool {
	if !(stmt.is_word(src, 0, "ALTER") && stmt.is_word(src, 1, "TABLE")) {
		return false;
	}
	let Ok(Some(migration)) = find(stmt, src) else {
		return false;
	};
	let name_at = if stmt.is_word(src, 2, "IF") && stmt.is_word(src, 3, "EXISTS") {
		4
	} else {
		2
	};
	// Nothing but the table's name before the clause, and nothing after it.
	let after_name = crate::schema_state::glued_end(stmt, src, name_at);
	migration.step == MigrationStep::RemoveUndeclared
		&& stmt.is_word(src, after_name, "MIGRATE")
		&& migration.cut.end == stmt.span.end
}

/// `stmt` without the clause `migration` cut from it.
pub(crate) fn without_clause(stmt: &Stmt, src: &str, migration: &Migration) -> String {
	format!(
		"{}{}",
		&src[stmt.span.start..migration.cut.start],
		&src[migration.cut.end..stmt.span.end]
	)
}

/// `stmt` with its comments, and the clause when given, replaced by a space:
/// what the catalog hashes for a statement that carries a migration.
pub(crate) fn stripped_without(stmt: &Stmt, src: &str, cut: Option<&Range<usize>>) -> String {
	let mut out = String::with_capacity(stmt.span.len());
	let mut at = stmt.span.start;
	let mut gaps: Vec<Range<usize>> = stmt.comments.clone();
	if let Some(cut) = cut {
		gaps.retain(|comment| comment.end <= cut.start || comment.start >= cut.end);
		gaps.push(cut.clone());
		gaps.sort_by_key(|gap| gap.start);
	}
	for gap in gaps {
		out.push_str(&src[at..gap.start]);
		out.push(' ');
		at = gap.end;
	}
	out.push_str(&src[at..stmt.span.end]);
	out
}

/// The text of `tokens` as written between the first and the last.
fn token_text(stmt: &Stmt, src: &str, tokens: Range<usize>) -> String {
	let from = stmt.tokens[tokens.start].span.start;
	let to = stmt.tokens[tokens.end - 1].span.end;
	collapse(&without_comments(stmt, src, from..to))
}

/// `src[range]` with the comments of `stmt` that fall inside it dropped.
fn without_comments(stmt: &Stmt, src: &str, range: Range<usize>) -> String {
	let mut out = String::new();
	let mut at = range.start;
	for comment in &stmt.comments {
		if comment.start >= range.start && comment.end <= range.end {
			out.push_str(&src[at..comment.start]);
			out.push(' ');
			at = comment.end;
		}
	}
	out.push_str(&src[at..range.end]);
	out
}

fn collapse(text: &str) -> String {
	text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The field a `DEFINE FIELD` or `ALTER FIELD` names.
fn field_name(stmt: &Stmt, src: &str) -> Option<String> {
	crate::schema_state::ident_at(stmt, src, field_name_index(stmt, src))
}

/// The index of the name after `DEFINE <KIND>` or `ALTER <KIND>` and its
/// modifier.
fn field_name_index(stmt: &Stmt, src: &str) -> usize {
	if stmt.is_word(src, 2, "OVERWRITE") {
		3
	} else if stmt.is_word(src, 2, "IF") && stmt.is_word(src, 3, "NOT") {
		5
	} else if stmt.is_word(src, 2, "IF") {
		4
	} else {
		2
	}
}

/// The table a statement names: after `ON [TABLE]` for a field, or right after
/// `ALTER TABLE [IF EXISTS]`.
fn table_name(stmt: &Stmt, src: &str) -> Option<String> {
	if stmt.is_word(src, 1, "TABLE") {
		let idx = if stmt.is_word(src, 2, "IF") && stmt.is_word(src, 3, "EXISTS") {
			4
		} else {
			2
		};
		return crate::schema_state::ident_at(stmt, src, idx).map(|name| unquote(&name));
	}
	let on = (2..stmt.tokens.len())
		.find(|&i| stmt.tokens[i].depth == 0 && stmt.is_word(src, i, "ON"))?;
	let idx = if stmt.is_word(src, on + 1, "TABLE") {
		on + 2
	} else {
		on + 1
	};
	crate::schema_state::ident_at(stmt, src, idx).map(|name| unquote(&name))
}

/// The table and field a `DEFINE FIELD` defines, without quotes.
pub(crate) fn defines_field(stmt: &Stmt, src: &str) -> Option<(String, String)> {
	if !(stmt.is_word(src, 0, "DEFINE") && stmt.is_word(src, 1, "FIELD")) {
		return None;
	}
	Some((table_name(stmt, src)?, unquote(&field_name(stmt, src)?)))
}

/// The table a `DEFINE TABLE` defines, without quotes.
pub(crate) fn defines_table(stmt: &Stmt, src: &str) -> Option<String> {
	if !(stmt.is_word(src, 0, "DEFINE") && stmt.is_word(src, 1, "TABLE")) {
		return None;
	}
	crate::schema_state::ident_at(stmt, src, field_name_index(stmt, src)).map(|name| unquote(&name))
}

/// A name without its backticks or `⟨⟩`.
pub(crate) fn unquote(name: &str) -> String {
	crate::rollout::unquote_ident(name)
}

/// An index [`apply`] removes so a field it reads can be migrated, with the
/// definition it is put back from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DroppedIndex {
	pub table: String,
	pub name: String,
	/// The definition `INFO FOR TABLE` listed for it.
	pub definition: String,
}

/// What a table looked like before an apply: the migrations it has run, and
/// its indexes by name.
struct TableState {
	ran: BTreeSet<String>,
	/// The latest migration SurrealDB ran on each field, by the generation it
	/// minted.
	latest: BTreeMap<String, String>,
	indexes: BTreeMap<String, String>,
}

async fn table_state(db: &Surreal<Any>, table: &str) -> Result<TableState> {
	let info = crate::rollout::retry_conflicts(&format!("reading table `{table}`"), || async {
		crate::rollout::info_json(db, "INFO FOR TABLE $table;", Some(table)).await
	})
	.await?;
	Ok(TableState {
		ran: run_names(&info),
		latest: latest_runs(&info),
		indexes: info
			.get("indexes")
			.and_then(Value::as_object)
			.map(|indexes| {
				indexes
					.iter()
					.filter_map(|(name, def)| {
						def.as_str().map(|def| (unquote(name), def.to_string()))
					})
					.collect()
			})
			.unwrap_or_default(),
	})
}

/// For each field, the migration an `INFO FOR TABLE` answer says ran on it
/// last, by the generation each minted.
fn latest_runs(info: &Value) -> BTreeMap<String, String> {
	let mut latest: BTreeMap<String, (i64, String)> = BTreeMap::new();
	let runs = info
		.get("migrations")
		.and_then(|section| section.get("migrations"))
		.and_then(Value::as_array);
	for run in runs.into_iter().flatten() {
		let (Some(name), Some(field)) =
			(run.get("name").and_then(Value::as_str), run.get("field").and_then(Value::as_str))
		else {
			continue;
		};
		let generation = run.get("generation").and_then(Value::as_i64).unwrap_or(0);
		let field = unquote(field);
		if latest.get(&field).is_none_or(|(at, _)| generation > *at) {
			latest.insert(field, (generation, name.to_string()));
		}
	}
	latest.into_iter().map(|(field, (_, name))| (field, name)).collect()
}

/// What applying one migration does, given what the table has run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
	/// The table has not run it: send the statement as written.
	Run,
	/// It is the field's latest migration: send the definition without it.
	InEffect,
	/// It ran, and a later migration of the field (a revert, say) has replaced
	/// it since. A table never runs a name twice, so it runs under this one.
	RunAs(String),
}

fn decide(migration: &Migration, state: &TableState, entry: Option<&LedgerEntry>) -> Decision {
	let mut names: Vec<String> = entry.map(|e| e.server_names.clone()).unwrap_or_default();
	if !names.contains(&migration.name) {
		names.push(migration.name.clone());
	}
	if !names.iter().any(|name| state.ran.contains(name)) {
		return Decision::Run;
	}
	let Some(field) = migration.field() else {
		return Decision::InEffect;
	};
	match state.latest.get(&unquote(field)) {
		Some(latest) if !names.contains(latest) => {
			let fresh = (2..)
				.map(|n| format!("{}_{n}", migration.name))
				.find(|name| !state.ran.contains(name))
				.expect("an unused name");
			Decision::RunAs(fresh)
		}
		_ => Decision::InEffect,
	}
}

/// `stmt` with its migration's name replaced by `name`.
fn renamed(stmt: &Stmt, src: &str, migration: &Migration, name: &str) -> String {
	format!(
		"{}{}{}",
		&src[stmt.span.start..migration.name_span.start],
		crate::rollout::quote_ident_if_needed(name),
		&src[migration.name_span.end..stmt.span.end]
	)
}

/// The names of the migrations an `INFO FOR TABLE` answer says the table has run.
pub(crate) fn run_names(info: &Value) -> BTreeSet<String> {
	info.get("migrations")
		.and_then(|section| section.get("migrations"))
		.and_then(Value::as_array)
		.map(|runs| {
			runs.iter()
				.filter_map(|run| run.get("name").and_then(Value::as_str).map(str::to_string))
				.collect()
		})
		.unwrap_or_default()
}

/// Whether the index `definition` reads `field`: as one of its columns, or in
/// the condition of a `COUNT` index. A column and a field meet when either is
/// the other or a path beneath it, which is how SurrealDB decides.
pub(crate) fn index_reads(definition: &str, field: &str) -> bool {
	let Ok(statements) = crate::surql_scan::scan(definition) else {
		return false;
	};
	let Some(stmt) = statements.first() else {
		return false;
	};
	let field = unquote(field);
	let root = path_root(&field);
	let n = stmt.tokens.len();
	if let Some(at) = (0..n)
		.find(|&i| stmt.is_word(definition, i, "FIELDS") || stmt.is_word(definition, i, "COLUMNS"))
	{
		let mut idx = at + 1;
		while let Some(column) = crate::schema_state::ident_at(stmt, definition, idx) {
			if paths_meet(&unquote(&column), &field) {
				return true;
			}
			let next = crate::schema_state::glued_end(stmt, definition, idx);
			if stmt.tok_text(definition, next) != Some(",") {
				break;
			}
			idx = next + 1;
		}
	}
	let counts = (0..n).any(|i| stmt.is_word(definition, i, "COUNT"));
	if counts && let Some(at) = (0..n).find(|&i| stmt.is_word(definition, i, "WHERE")) {
		return (at + 1..n).any(|i| {
			matches!(stmt.tokens[i].kind, TokKind::Word | TokKind::QuotedIdent)
				&& unquote(stmt.tok_text(definition, i).unwrap_or_default()) == root
		});
	}
	false
}

fn path_root(path: &str) -> &str {
	path.split(['.', '[']).next().unwrap_or(path)
}

fn paths_meet(a: &str, b: &str) -> bool {
	let beneath = |path: &str, parent: &str| {
		path.strip_prefix(parent).is_some_and(|rest| rest.starts_with('.') || rest.starts_with('['))
	};
	a == b || beneath(a, b) || beneath(b, a)
}

/// One migration's entry in the `migration` partition.
fn ledger_key(table: &str, name: &str) -> String {
	format!("{table}:{name}")
}

/// What SurrealKit recorded about a migration it ran.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LedgerEntry {
	clause_hash: String,
	/// The names SurrealDB ran it under: its own, and one for each time it ran
	/// again after a later migration of its field replaced it.
	server_names: Vec<String>,
}

async fn load_ledger(
	db: &Surreal<Any>,
	module: &Module,
	keys: &[String],
) -> Result<BTreeMap<String, LedgerEntry>> {
	let ns = module.partition(Partition::Migration);
	let rows: Vec<Value> =
		crate::rollout::retry_conflicts("reading the migration ledger", || async {
			let mut response = db
				.query("SELECT key, val FROM __entity WHERE ns = $ns AND key IN $keys;")
				.bind(("ns", ns.clone()))
				.bind(("keys", keys.to_vec()))
				.await?
				.check()?;
			Ok(response.take(0)?)
		})
		.await?;
	Ok(rows
		.iter()
		.filter_map(|row| {
			let key = row.get("key")?.as_str()?.to_string();
			let val = row.get("val")?;
			let clause_hash = val.get("clause_hash")?.as_str()?.to_string();
			let server_names = val
				.get("server_names")
				.and_then(Value::as_array)
				.map(|names| names.iter().filter_map(|n| n.as_str().map(str::to_string)).collect())
				.unwrap_or_default();
			Some((
				key,
				LedgerEntry {
					clause_hash,
					server_names,
				},
			))
		})
		.collect())
}

async fn record_in_ledger(
	db: &Surreal<Any>,
	module: &Module,
	migration: &Migration,
	server_names: &[String],
	source: &str,
) -> Result<()> {
	let ns = module.partition(Partition::Migration);
	let key = ledger_key(&migration.table, &migration.name);
	let val = serde_json::json!({
		"field": migration.field(),
		"clause_hash": migration.clause_hash,
		"server_names": server_names,
		"source_path": source,
	});
	crate::rollout::retry_conflicts("recording a migration", || async {
		db.query(
			"DELETE __entity WHERE ns = $ns AND key = $key; \
			 CREATE __entity CONTENT { ns: $ns, key: $key, val: $val, updated_at: time::now() };",
		)
		.bind(("ns", ns.clone()))
		.bind(("key", key.clone()))
		.bind(("val", val.clone()))
		.await?
		.check()?;
		Ok(())
	})
	.await
}

/// What [`plan`] expects one migration to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Planned {
	pub migration: Migration,
	/// Whether the table has run the name already.
	pub ran_before: bool,
	/// The indexes that will be removed and defined again around it.
	pub indexes: Vec<String>,
}

/// What applying `sql` would do to the migrations it carries, without writing.
pub(crate) async fn plan(db: &Surreal<Any>, sql: &str) -> Result<Vec<Planned>> {
	let found = find_all(sql)?;
	let mut states = BTreeMap::new();
	let mut out = Vec::new();
	for (_, migration) in found {
		if !states.contains_key(&migration.table) {
			states.insert(migration.table.clone(), table_state(db, &migration.table).await?);
		}
		let state = &states[&migration.table];
		let ran_before = decide(&migration, state, None) == Decision::InEffect;
		let indexes = match (ran_before, migration.field()) {
			(false, Some(field)) => state
				.indexes
				.iter()
				.filter(|(_, def)| index_reads(def, field))
				.map(|(name, _)| name.clone())
				.collect(),
			_ => Vec::new(),
		};
		out.push(Planned {
			migration,
			ran_before,
			indexes,
		});
	}
	Ok(out)
}

/// How [`apply`] makes sure an index it removes comes back if the run stops
/// before it defines the index again.
pub(crate) enum IndexGuard<'a> {
	/// Sync: forget the hashes of the files that define the index, so the next
	/// sync applies them. `entities` is the catalog the files describe.
	Sync {
		entities: &'a [crate::schema_state::CatalogEntity],
	},
	/// A rollout step: keep the definition in the rollout's record, where
	/// [`restore_dropped_indexes`] finds it when the rollout carries on.
	Rollout {
		rollout_id: &'a str,
	},
}

/// What [`apply`] did besides applying the SQL.
#[derive(Debug, Default)]
pub(crate) struct Applied {
	/// The files whose sync hashes an [`IndexGuard::Sync`] cleared. Once the
	/// apply succeeds, the indexes are back, and the caller stores them again.
	pub cleared: Vec<String>,
}

/// The prefix of a `restore_definitions` key for an index [`apply`] removed
/// during a rollout step.
const DROPPED_PREFIX: &str = "dropped:";

/// Apply prepared schema SQL, running each migration it carries at most once.
///
/// SQL with no migration is sent as it is.
pub(crate) async fn apply(
	db: &Surreal<Any>,
	module: &Module,
	sql: &str,
	source: &str,
	guard: &IndexGuard<'_>,
) -> Result<Applied> {
	let exec = async |sql: String| crate::core::exec_surql(db, &sql).await;
	let statements = crate::surql_scan::scan(sql).map_err(|err| anyhow!("{source}: {err}"))?;
	let mut found = BTreeMap::new();
	for (idx, stmt) in statements.iter().enumerate() {
		match find(stmt, sql) {
			Ok(Some(migration)) => {
				found.insert(idx, migration);
			}
			Ok(None) => {}
			Err(reason) => bail!("{source}, line {}: {reason}", stmt.line),
		}
	}
	if found.is_empty() {
		exec(sql.to_string()).await.map_err(|err| hint(err, source))?;
		return Ok(Applied::default());
	}

	let mut states = BTreeMap::new();
	for migration in found.values() {
		if !states.contains_key(&migration.table) {
			states.insert(migration.table.clone(), table_state(db, &migration.table).await?);
		}
	}
	let keys: Vec<String> = found.values().map(|m| ledger_key(&m.table, &m.name)).collect();
	let ledger = load_ledger(db, module, &keys).await?;

	let mut out = String::with_capacity(sql.len());
	let mut pending = Vec::new();
	let mut dropping: Vec<DroppedIndex> = Vec::new();
	for (idx, stmt) in statements.iter().enumerate() {
		let Some(migration) = found.get(&idx) else {
			out.push_str(stmt.text(sql));
			out.push_str(";\n");
			continue;
		};
		let state = &states[&migration.table];
		let entry = ledger.get(&ledger_key(&migration.table, &migration.name));
		let decision = decide(migration, state, entry);
		if decision != Decision::Run
			&& entry.is_some_and(|e| e.clause_hash != migration.clause_hash)
		{
			bail!(
				"{source}, line {}: migration `{}` on `{}` already ran with a different \
					 transform. A table runs a name once, so the new transform needs a name of its \
					 own",
				migration.line,
				migration.name,
				migration.table
			);
		}
		if decision == Decision::InEffect {
			log::info!(
				"{source}: migration `{}` already ran on `{}`; applying the definition only",
				migration.name,
				migration.table
			);
			// A table migration changes no definition, so nothing is left to apply.
			if migration.field().is_some() {
				out.push_str(&without_clause(stmt, sql, migration));
				out.push_str(";\n");
			}
			continue;
		}
		let mut server_names = entry.map(|e| e.server_names.clone()).unwrap_or_default();
		let text = match &decision {
			Decision::RunAs(fresh) => {
				log::info!(
					"{source}: migration `{}` ran on `{}` before, and a later migration of `{}` \
					 has replaced it since; running it again as `{fresh}`",
					migration.name,
					migration.table,
					migration.field().unwrap_or_default()
				);
				server_names.push(fresh.clone());
				renamed(stmt, sql, migration, fresh)
			}
			_ => {
				server_names.push(migration.name.clone());
				stmt.text(sql).to_string()
			}
		};
		server_names.dedup();
		if let Some(field) = migration.field() {
			for (name, definition) in &state.indexes {
				if index_reads(definition, field)
					&& !dropping.iter().any(|d| d.table == migration.table && d.name == *name)
				{
					dropping.push(DroppedIndex {
						table: migration.table.clone(),
						name: name.clone(),
						definition: definition.clone(),
					});
				}
			}
		}
		let ran_as = server_names.last().cloned().unwrap_or_else(|| migration.name.clone());
		pending.push((migration.clone(), ran_as, server_names));
		out.push_str(&text);
		out.push_str(";\n");
	}

	let mut cleared = Vec::new();
	if !dropping.is_empty() {
		match guard {
			IndexGuard::Sync {
				entities,
			} => {
				cleared = index_sources(entities, &dropping, source);
				crate::rollout::delete_sync_hashes(db, module, &cleared).await?;
			}
			IndexGuard::Rollout {
				rollout_id,
			} => keep_dropped_indexes(db, rollout_id, &dropping).await?,
		}
		for index in &dropping {
			if index_is_unique(&index.definition) {
				log::warn!(
					"{source}: removing UNIQUE index `{}` on `{}` while a field it reads is \
					 migrated. Until it is defined again, the table does not enforce it",
					index.name,
					index.table
				);
			} else {
				log::info!(
					"{source}: removing index `{}` on `{}` while a field it reads is migrated",
					index.name,
					index.table
				);
			}
			let remove = format!(
				"REMOVE INDEX IF EXISTS {} ON {};",
				crate::rollout::quote_ident(&index.name),
				crate::rollout::quote_ident(&index.table)
			);
			crate::rollout::retry_conflicts(&format!("removing index `{}`", index.name), || {
				crate::core::exec_surql(db, &remove)
			})
			.await?;
		}
	}

	let applied = exec(out).await;

	let restored = restore_indexes(db, &dropping, source).await;

	for table in pending.iter().map(|(m, ..)| m.table.clone()).collect::<BTreeSet<_>>() {
		let now = table_state(db, &table).await?;
		for (migration, ran_as, server_names) in pending.iter().filter(|(m, ..)| m.table == table) {
			if now.ran.contains(ran_as) {
				record_in_ledger(db, module, migration, server_names, source).await?;
				log::info!("{source}: ran migration `{}` on `{table}`", migration.name);
			}
		}
	}

	applied.map_err(|err| hint(err, source))?;
	restored?;
	Ok(Applied {
		cleared,
	})
}

/// The schema files, other than `applying`, that define the indexes in `dropped`.
fn index_sources(
	entities: &[crate::schema_state::CatalogEntity],
	dropped: &[DroppedIndex],
	applying: &str,
) -> Vec<String> {
	let mut out = BTreeSet::new();
	for index in dropped {
		for entity in entities {
			if entity.kind == crate::schema_state::EntityKind::Index
				&& unquote(&entity.name) == index.name
				&& entity.scope.as_deref().map(unquote).as_deref() == Some(index.table.as_str())
				&& entity.source_path != applying
			{
				out.insert(entity.source_path.clone());
			}
		}
	}
	out.into_iter().collect()
}

fn dropped_key(table: &str, index: &str) -> String {
	format!("{DROPPED_PREFIX}index:{table}:{index}")
}

/// Record, in the rollout's `restore_definitions`, the indexes about to be removed.
async fn keep_dropped_indexes(
	db: &Surreal<Any>,
	rollout_id: &str,
	dropped: &[DroppedIndex],
) -> Result<()> {
	let defs: serde_json::Map<String, Value> = dropped
		.iter()
		.map(|d| (dropped_key(&d.table, &d.name), Value::String(d.definition.clone())))
		.collect();
	let defs = Value::Object(defs);
	crate::rollout::retry_conflicts("keeping the indexes a migration removes", || async {
		db.query("UPDATE type::record('__rollout', $id) MERGE { restore_definitions: $defs };")
			.bind(("id", rollout_id.to_string()))
			.bind(("defs", defs.clone()))
			.await?
			.check()?;
		Ok(())
	})
	.await
}

/// Define again the indexes an earlier attempt at a rollout step removed for a
/// migration and did not get to define again.
pub(crate) async fn restore_dropped_indexes(db: &Surreal<Any>, rollout_id: &str) -> Result<()> {
	let captured = crate::rollout::load_restore_definitions(db, rollout_id).await?;
	let dropped: Vec<DroppedIndex> = captured
		.iter()
		.filter_map(|(key, definition)| {
			let rest = key.strip_prefix(DROPPED_PREFIX)?.strip_prefix("index:")?;
			let (table, name) = rest.split_once(':')?;
			Some(DroppedIndex {
				table: table.to_string(),
				name: name.to_string(),
				definition: definition.clone(),
			})
		})
		.collect();
	if dropped.is_empty() {
		return Ok(());
	}
	restore_indexes(db, &dropped, &format!("rollout '{rollout_id}'")).await
}

/// Define again each index in `dropped` that the apply did not define itself.
async fn restore_indexes(db: &Surreal<Any>, dropped: &[DroppedIndex], source: &str) -> Result<()> {
	let tables: BTreeSet<&str> = dropped.iter().map(|d| d.table.as_str()).collect();
	let mut failed = Vec::new();
	for table in tables {
		let now = table_state(db, table).await?;
		for index in dropped.iter().filter(|d| d.table == table) {
			if now.indexes.contains_key(&index.name) {
				continue;
			}
			let prepared = crate::schema_state::prepare_schema_sql(&index.definition)?;
			let result = crate::rollout::retry_conflicts(
				&format!("defining index `{}`", index.name),
				|| crate::core::exec_surql(db, &prepared.sql),
			)
			.await;
			match result {
				Ok(()) => log::info!("{source}: defined index `{}` on `{table}` again", index.name),
				Err(err) => {
					log::error!(
						"{source}: could not define index `{}` on `{table}` again: {err:#}. Its \
						 definition was: {}",
						index.name,
						index.definition
					);
					failed.push(format!("{table}.{}", index.name));
				}
			}
		}
	}
	if !failed.is_empty() {
		bail!(
			"{source}: the indexes {} were removed to migrate a field they read and could not be \
			 defined again. The next run defines them again",
			failed.join(", ")
		);
	}
	Ok(())
}

fn index_is_unique(definition: &str) -> bool {
	crate::surql_scan::scan(definition)
		.ok()
		.and_then(|statements| statements.into_iter().next())
		.is_some_and(|stmt| (0..stmt.tokens.len()).any(|i| stmt.is_word(definition, i, "UNIQUE")))
}

/// Add what to do next to a refusal the server gave because a change needs a
/// migration.
pub(crate) fn hint(err: anyhow::Error, source: &str) -> anyhow::Error {
	let message = format!("{err:#}");
	if message.contains("add MIGRATE EXISTING ROWS") {
		let target = message
			.split_once("Cannot change field `")
			.and_then(|(_, rest)| {
				let (field, rest) = rest.split_once("` of table `")?;
				let (table, _) = rest.split_once('`')?;
				Some(format!("`{table}.{field}`"))
			})
			.unwrap_or_else(|| "the field".to_string());
		return err.context(format!(
			"{source}: the table has records, and the new definition of {target} accepts less \
			 than the old one did. SurrealDB compares the two definitions, not the values the \
			 records hold, so it asks what they become even when every value would fit. Say so \
			 in the same DEFINE FIELD: \
			 `MIGRATE EXISTING ROWS AS <name> USING <expression over $value> FALLBACK <value>`, \
			 for example `MIGRATE EXISTING ROWS AS stock_to_int USING <int> $value FALLBACK 0`"
		));
	}
	if message.contains("reads it: remove the index, migrate") {
		return err.context(format!(
			"{source}: define the index after the field it reads, so SurrealKit can remove it for \
			 the migration and define it again"
		));
	}
	if message.contains("feature level") && message.contains("MIGRATE") {
		return err.context(format!(
			"{source}: this datastore is still at the 3.x feature level. Raise it before using \
			 MIGRATE EXISTING ROWS (on TiKV, SURREAL_FEATURE_LEVEL=4.0)"
		));
	}
	err.context(source.to_string())
}

/// The fields among `entities` whose stored values go with them when they are
/// removed: at compatibility level 4, `REMOVE FIELD` on a `SCHEMAFULL` table
/// also clears what its records hold.
pub(crate) async fn fields_whose_values_go(
	db: &Surreal<Any>,
	entities: &[crate::schema_state::EntityKey],
) -> Result<Vec<crate::schema_state::EntityKey>> {
	let fields: Vec<_> =
		entities.iter().filter(|e| e.kind == crate::schema_state::EntityKind::Field).collect();
	if fields.is_empty() {
		return Ok(Vec::new());
	}
	let info = crate::rollout::retry_conflicts("reading the database", || async {
		crate::rollout::info_json(db, "INFO FOR DB;", None).await
	})
	.await?;
	if compatibility(&info) < 4 {
		return Ok(Vec::new());
	}
	let tables = info.get("tables").and_then(Value::as_object);
	let schemafull = |table: &str| {
		tables.and_then(|tables| tables.get(table)).and_then(Value::as_str).is_some_and(
			|definition| {
				definition.split_whitespace().any(|word| word.eq_ignore_ascii_case("SCHEMAFULL"))
			},
		)
	};
	Ok(fields
		.into_iter()
		.filter(|field| field.scope.as_deref().map(unquote).is_some_and(|t| schemafull(&t)))
		.cloned()
		.collect())
}

/// The compatibility level an `INFO FOR DB` answer reports: 3 when it reports
/// none, as a database a 3.x release created does.
pub(crate) fn compatibility(info: &Value) -> i64 {
	info.get("compatibility").and_then(Value::as_i64).unwrap_or(3)
}

/// Words that start a statement, which a migration's transform may not hold.
const STATEMENT_WORDS: &[&str] = &[
	"IF", "ELSE", "SELECT", "LET", "FOR", "RETURN", "CREATE", "UPDATE", "UPSERT", "DELETE",
	"INSERT", "RELATE", "DEFINE", "REMOVE", "ALTER", "BEGIN", "COMMIT", "CANCEL", "THROW", "BREAK",
	"CONTINUE", "SLEEP", "INFO", "LIVE", "KILL", "USE", "SHOW", "REBUILD",
];

/// Why SurrealDB would refuse `text` as a migration's transform, or with
/// `constant` as its FALLBACK, where that shows without the server: a
/// statement, a parameter other than `$value`, or a call SurrealDB does not
/// let a transform make. The server checks more; this catches the mistakes
/// that would otherwise surface only when the step runs.
pub(crate) fn transform_refusal(text: &str, constant: bool) -> Option<String> {
	let statements = crate::surql_scan::scan(text).ok()?;
	if statements.len() > 1 {
		return Some("it may not hold more than one statement".to_string());
	}
	let stmt = statements.first()?;
	for (i, tok) in stmt.tokens.iter().enumerate() {
		let word = &text[tok.span.clone()];
		let prev = i.checked_sub(1).and_then(|p| stmt.tok_text(text, p));
		let next = stmt.tok_text(text, i + 1);
		let after = stmt.tok_text(text, i + 2);
		match tok.kind {
			TokKind::Param => {
				let name = unquote(word.trim_start_matches('$'));
				if constant {
					return Some(format!("a FALLBACK is a constant, so it cannot read `${name}`"));
				}
				if name != "value" {
					return Some(format!("it reads only `$value`, not `${name}`"));
				}
			}
			TokKind::Word => {
				// A path step (`$value.select`) or an object key (`{ select: 1 }`).
				if prev == Some(".") || (next == Some(":") && after != Some(":")) {
					continue;
				}
				let upper = word.to_ascii_uppercase();
				if STATEMENT_WORDS.contains(&upper.as_str()) {
					return Some(format!(
						"it may not hold a statement (`{word}`). Write it as an expression over \
						 `$value`, for example `$value != NONE AND $value`, with a FALLBACK for \
						 what does not convert"
					));
				}
				if matches!(upper.as_str(), "FN" | "ML" | "MOD" | "EXT")
					&& next == Some(":")
					&& after == Some(":")
				{
					return Some(format!(
						"it may not call `{word}::` functions; only built-in functions are allowed"
					));
				}
			}
			_ => {}
		}
	}
	None
}

/// Offline checks of the migrations in a set of schema files: the ones the
/// server is known to refuse, and a name used twice on a table for different
/// transforms.
pub(crate) fn check_files(files: &[crate::schema_state::SchemaFile]) -> Result<()> {
	let mut seen: BTreeMap<(String, String), (String, String)> = BTreeMap::new();
	for file in files {
		let statements = crate::surql_scan::scan(&file.sql)
			.map_err(|err| anyhow!("schema file '{}': {err}", file.path))?;
		for stmt in &statements {
			let migration = match find(stmt, &file.sql) {
				Ok(Some(migration)) => migration,
				Ok(None) => continue,
				Err(reason) => {
					bail!("schema file '{}', line {}: {reason}", file.path, stmt.line)
				}
			};
			if let Some(field) = migration.field() {
				if let Some(reason) = unmigratable(stmt, &file.sql, field) {
					bail!(
						"schema file '{}', line {}: field `{field}` of `{}` cannot be migrated: \
						 {reason}",
						file.path,
						stmt.line,
						migration.table
					);
				}
				if let MigrationStep::Rewrite {
					using,
					fallback,
					..
				} = &migration.step
				{
					let refusal =
						transform_refusal(using, false).map(|r| ("USING", r)).or_else(|| {
							fallback
								.as_deref()
								.and_then(|f| transform_refusal(f, true))
								.map(|r| ("FALLBACK", r))
						});
					if let Some((clause, reason)) = refusal {
						bail!(
							"schema file '{}', line {}: the {clause} of migration `{}` would be \
							 refused: {reason}",
							file.path,
							stmt.line,
							migration.name
						);
					}
				}
				if let MigrationStep::Rewrite {
					fallback: None,
					..
				} = &migration.step
				{
					log::warn!(
						"schema file '{}', line {}: migration `{}` has no FALLBACK. SurrealDB \
						 accepts that only when it can prove the transform never fails; if it \
						 refuses, add one",
						file.path,
						stmt.line,
						migration.name
					);
				}
			}
			let key = (migration.table.clone(), migration.name.clone());
			let here = (file.path.clone(), migration.clause_hash.clone());
			if let Some((path, hash)) = seen.get(&key) {
				if *hash != here.1 {
					bail!(
						"migration `{}` on `{}` is written twice with different transforms, in \
						 '{path}' and '{}'. A table runs a name once",
						migration.name,
						migration.table,
						file.path
					);
				}
			} else {
				seen.insert(key, here);
			}
		}
	}
	Ok(())
}

/// Why SurrealDB refuses to migrate `field` as `stmt` defines it, if it does.
fn unmigratable(stmt: &Stmt, src: &str, field: &str) -> Option<&'static str> {
	if matches!(unquote(field).as_str(), "id" | "in" | "out") {
		return Some("it is fixed by the record's key");
	}
	let top_word = |word: &str| {
		(0..stmt.tokens.len()).any(|i| stmt.tokens[i].depth == 0 && stmt.is_word(src, i, word))
	};
	if top_word("COMPUTED") {
		return Some("a COMPUTED field is never stored");
	}
	if top_word("REFERENCE") {
		return Some("a REFERENCE field's backlinks hold its stored values");
	}
	if top_word("INLINE") {
		return Some("an INLINE field's adjacency keys hold its stored values");
	}
	None
}

#[cfg(test)]
mod tests {
	use test_case::test_case;

	use super::*;

	fn first(sql: &str) -> (Stmt, Option<Migration>) {
		let stmt = crate::surql_scan::scan(sql).unwrap().remove(0);
		let found = find(&stmt, sql).unwrap();
		(stmt, found)
	}

	#[test]
	fn reads_a_field_migration() {
		let sql = "DEFINE FIELD OVERWRITE stock ON TABLE product TYPE int \
		           MIGRATE EXISTING ROWS AS stock_to_int USING <int> $value FALLBACK 0;";
		let (stmt, migration) = first(sql);
		let migration = migration.expect("a migration");
		assert_eq!(migration.table, "product");
		assert_eq!(migration.name, "stock_to_int");
		assert_eq!(
			migration.step,
			MigrationStep::Rewrite {
				field: "stock".into(),
				using: "<int> $value".into(),
				fallback: Some("0".into()),
			}
		);
		assert_eq!(
			without_clause(&stmt, sql, &migration),
			"DEFINE FIELD OVERWRITE stock ON TABLE product TYPE int"
		);
	}

	#[test]
	fn the_clause_ends_before_graphql_names() {
		let sql = "DEFINE FIELD f ON t TYPE int COMMENT 'c' MIGRATE EXISTING ROWS AS m \
		           USING $value * 100 GRAPHQL_ALIAS 'g';";
		let (stmt, migration) = first(sql);
		let migration = migration.unwrap();
		assert_eq!(
			migration.step,
			MigrationStep::Rewrite {
				field: "f".into(),
				using: "$value * 100".into(),
				fallback: None,
			}
		);
		assert_eq!(
			without_clause(&stmt, sql, &migration),
			"DEFINE FIELD f ON t TYPE int COMMENT 'c' GRAPHQL_ALIAS 'g'"
		);
	}

	#[test]
	fn the_clause_keywords_stay_identifiers_elsewhere() {
		let sql = "DEFINE FIELD using ON rows TYPE int MIGRATE EXISTING ROWS AS fallback \
		           USING using FALLBACK existing;";
		let (_, migration) = first(sql);
		let migration = migration.unwrap();
		assert_eq!(migration.table, "rows");
		assert_eq!(migration.name, "fallback");
		assert_eq!(
			migration.step,
			MigrationStep::Rewrite {
				field: "using".into(),
				using: "using".into(),
				fallback: Some("existing".into()),
			}
		);
	}

	#[test]
	fn a_quoted_name_is_read_without_quotes() {
		let sql = "DEFINE FIELD f ON `my table` MIGRATE EXISTING ROWS AS `two words` USING $value;";
		let (_, migration) = first(sql);
		let migration = migration.unwrap();
		assert_eq!(migration.table, "my table");
		assert_eq!(migration.name, "two words");
	}

	#[test]
	fn a_clause_inside_brackets_is_not_one() {
		let sql = "DEFINE FIELD f ON t VALUE { MIGRATE: 1 } TYPE object;";
		assert_eq!(first(sql).1, None);
	}

	#[test_case("DEFINE FIELD f ON t MIGRATE EXISTING ROWS AS a USING 1 MIGRATE EXISTING ROWS AS b USING 2;", "one MIGRATE" ; "twice")]
	#[test_case("DEFINE FIELD f ON t MIGRATE EXISTING ROWS USING 1;", "AS <name>" ; "no name")]
	#[test_case("DEFINE FIELD f ON t MIGRATE EXISTING ROWS AS a;", "USING" ; "no using")]
	#[test_case("DEFINE FIELD f ON t MIGRATE EXISTING ROWS AS a USING 1 FALLBACK;", "empty FALLBACK" ; "empty fallback")]
	#[test_case("ALTER TABLE t MIGRATE EXISTING ROWS AS a USING 1;", "REMOVE UNDECLARED" ; "table using")]
	fn refuses_a_malformed_clause(sql: &str, expected: &str) {
		let stmt = crate::surql_scan::scan(sql).unwrap().remove(0);
		let err = find(&stmt, sql).unwrap_err();
		assert!(err.contains(expected), "{err}");
	}

	#[test]
	fn the_hash_ignores_comments_and_spacing() {
		let a = first(
			"DEFINE FIELD f ON t TYPE int MIGRATE EXISTING ROWS AS m USING <int> $value FALLBACK 0;",
		)
		.1
		.unwrap();
		let b = first("DEFINE FIELD f ON t TYPE int\n  MIGRATE EXISTING ROWS AS m -- why\n  USING <int>   $value FALLBACK 0;").1.unwrap();
		let c = first(
			"DEFINE FIELD f ON t TYPE int MIGRATE EXISTING ROWS AS m USING <int> $value FALLBACK 1;",
		)
		.1
		.unwrap();
		assert_eq!(a.clause_hash, b.clause_hash);
		assert_ne!(a.clause_hash, c.clause_hash);
	}

	#[test_case("ALTER TABLE person MIGRATE EXISTING ROWS AS clean REMOVE UNDECLARED FIELDS;", true ; "plain")]
	#[test_case("ALTER TABLE IF EXISTS person MIGRATE EXISTING ROWS AS clean REMOVE UNDECLARED FIELDS;", true ; "if exists")]
	#[test_case("ALTER TABLE person SCHEMAFULL MIGRATE EXISTING ROWS AS clean REMOVE UNDECLARED FIELDS;", false ; "another clause first")]
	#[test_case("ALTER TABLE person MIGRATE EXISTING ROWS AS clean REMOVE UNDECLARED FIELDS COMPACT;", false ; "another clause after")]
	#[test_case("ALTER TABLE person SCHEMAFULL;", false ; "no migration")]
	#[test_case("ALTER FIELD f ON person TYPE int MIGRATE EXISTING ROWS AS m USING 1;", false ; "alter field")]
	fn accepts_only_the_bare_table_migration(sql: &str, accepted: bool) {
		let stmt = crate::surql_scan::scan(sql).unwrap().remove(0);
		assert_eq!(is_table_migration(&stmt, sql), accepted);
	}

	#[test_case("DEFINE INDEX i ON t FIELDS price", "price", true ; "the column")]
	#[test_case("DEFINE INDEX i ON t FIELDS name, price UNIQUE", "price", true ; "a later column")]
	#[test_case("DEFINE INDEX i ON t COLUMNS address.city", "address", true ; "beneath the field")]
	#[test_case("DEFINE INDEX i ON t FIELDS address", "address.city", true ; "above the field")]
	#[test_case("DEFINE INDEX i ON t FIELDS prices", "price", false ; "a longer name")]
	#[test_case("DEFINE INDEX i ON t COUNT WHERE active = true", "active", true ; "a count condition")]
	#[test_case("DEFINE INDEX i ON t COUNT", "active", false ; "a plain count")]
	fn knows_which_indexes_read_a_field(definition: &str, field: &str, reads: bool) {
		assert_eq!(index_reads(definition, field), reads);
	}

	#[test_case("<int> $value", false, None ; "a cast")]
	#[test_case("$value != NONE AND $value", false, None ; "a boolean guard")]
	#[test_case("{ published: 'live' }[$value] ?? $value", false, None ; "an object lookup")]
	#[test_case("string::uppercase($value)", false, None ; "a builtin")]
	#[test_case("$value.select", false, None ; "a path named like a keyword")]
	#[test_case("IF $value = 1 { NONE } ELSE { $value }", false, Some("statement") ; "an if block")]
	#[test_case("(SELECT VALUE 1 FROM ONLY t:1)", false, Some("statement") ; "a subquery")]
	#[test_case("$value + $other", false, Some("`$other`") ; "another parameter")]
	#[test_case("fn::fix($value)", false, Some("`fn::`") ; "a custom function")]
	#[test_case("0", true, None ; "a constant fallback")]
	#[test_case("$value", true, Some("constant") ; "a fallback that reads the value")]
	fn checks_a_transform_without_the_server(text: &str, constant: bool, refused: Option<&str>) {
		let found = transform_refusal(text, constant);
		match refused {
			None => assert_eq!(found, None),
			Some(expected) => {
				let found = found.expect("refused");
				assert!(found.contains(expected), "{found}");
			}
		}
	}

	#[test]
	fn runs_a_replaced_migration_again_under_a_new_name() {
		let sql = "DEFINE FIELD f ON t TYPE int MIGRATE EXISTING ROWS AS to_int USING <int> $value FALLBACK 0;";
		let (_, migration) = first(sql);
		let migration = migration.unwrap();
		let state = |ran: &[&str], latest: &str| TableState {
			ran: ran.iter().map(|s| s.to_string()).collect(),
			latest: [("f".to_string(), latest.to_string())].into(),
			indexes: BTreeMap::new(),
		};
		assert_eq!(decide(&migration, &state(&[], ""), None), Decision::Run);
		assert_eq!(decide(&migration, &state(&["to_int"], "to_int"), None), Decision::InEffect);
		assert_eq!(
			decide(&migration, &state(&["to_int", "to_int_revert"], "to_int_revert"), None),
			Decision::RunAs("to_int_2".into())
		);
		let entry = LedgerEntry {
			clause_hash: migration.clause_hash.clone(),
			server_names: vec!["to_int".into(), "to_int_2".into()],
		};
		assert_eq!(
			decide(
				&migration,
				&state(&["to_int", "to_int_revert", "to_int_2"], "to_int_2"),
				Some(&entry)
			),
			Decision::InEffect
		);
	}

	#[test]
	fn hints_at_the_clause_the_server_asks_for() {
		let err = anyhow!(
			"Cannot change field `age` of table `person`: the table is SCHEMAFULL in a database \
			 at compatibility level 4 and has rows, and the new definition does not accept every \
			 value the old one did: add MIGRATE EXISTING ROWS"
		);
		let message = format!("{:#}", hint(err, "schema/person.surql"));
		assert!(message.contains("`person.age`"), "{message}");
		assert!(message.contains("MIGRATE EXISTING ROWS AS <name>"), "{message}");
	}
}
