use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use walkdir::WalkDir;

use crate::constants::{
	catalog_snapshot_path, rollouts_dir, schema_dir, schema_snapshot_path, state_dir,
};
use crate::core::sha256_hex;

#[derive(Debug, Clone)]
pub struct SchemaFile {
	pub path: String,
	pub sql: String,
	pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaSnapshot {
	pub version: u32,
	pub files: Vec<SchemaSnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaSnapshotEntry {
	pub path: String,
	pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogSnapshot {
	pub version: u32,
	pub entities: Vec<CatalogEntity>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub operations: Vec<Operation>,
}

/// The category of a SurrealDB schema object that SurrealKit manages.
///
/// Each variant corresponds to a `DEFINE <KIND>` statement and serializes to the
/// lowercase SurrealDB keyword (e.g. [`EntityKind::Table`] ⇄ `"table"`,
/// [`EntityKind::Api`] ⇄ `"api"`). This keeps catalog snapshots, rollout TOML, and
/// the `__entity` metadata table wire-compatible with previous releases that stored
/// `kind` as a plain string.
///
/// [`EntityKind::Other`] is a forward-compatibility hatch: it preserves any kind
/// string written by a newer or foreign version of SurrealKit so existing state
/// still round-trips byte-for-byte instead of failing to deserialize. The schema
/// parser never produces `Other` — it accepts only the known keywords.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntityKind {
	Table,
	Field,
	Index,
	Event,
	Function,
	Param,
	Access,
	Analyzer,
	User,
	Api,
	Bucket,
	Model,
	Sequence,
	Config,
	Module,
	/// An unrecognized kind preserved verbatim from persisted state.
	Other(String),
}

impl EntityKind {
	/// The lowercase SurrealDB keyword for this kind (e.g. `"table"`).
	pub fn as_str(&self) -> &str {
		match self {
			Self::Table => "table",
			Self::Field => "field",
			Self::Index => "index",
			Self::Event => "event",
			Self::Function => "function",
			Self::Param => "param",
			Self::Access => "access",
			Self::Analyzer => "analyzer",
			Self::User => "user",
			Self::Api => "api",
			Self::Bucket => "bucket",
			Self::Model => "model",
			Self::Sequence => "sequence",
			Self::Config => "config",
			Self::Module => "module",
			Self::Other(s) => s.as_str(),
		}
	}

	/// Map a known lowercase keyword to its variant, or `None` if unrecognized.
	fn from_known(s: &str) -> Option<Self> {
		Some(match s {
			"table" => Self::Table,
			"field" => Self::Field,
			"index" => Self::Index,
			"event" => Self::Event,
			"function" => Self::Function,
			"param" => Self::Param,
			"access" => Self::Access,
			"analyzer" => Self::Analyzer,
			"user" => Self::User,
			"api" => Self::Api,
			"bucket" => Self::Bucket,
			"model" => Self::Model,
			"sequence" => Self::Sequence,
			"config" => Self::Config,
			"module" => Self::Module,
			_ => return None,
		})
	}

	/// Parse a kind string from persisted state, falling back to [`EntityKind::Other`]
	/// for unrecognized values so old/foreign data still round-trips.
	pub fn from_storage(s: &str) -> Self {
		Self::from_known(&s.to_ascii_lowercase()).unwrap_or_else(|| Self::Other(s.to_string()))
	}
}

impl fmt::Display for EntityKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

impl FromStr for EntityKind {
	type Err = anyhow::Error;

	/// Parse a SurrealDB keyword (case-insensitive) into a known [`EntityKind`].
	/// Unknown keywords are an error — use [`EntityKind::from_storage`] for the
	/// lenient, `Other`-preserving variant.
	fn from_str(s: &str) -> Result<Self> {
		Self::from_known(&s.to_ascii_lowercase())
			.ok_or_else(|| anyhow!("unknown entity kind: {s:?}"))
	}
}

// Ordering delegates to the keyword string so EntityKey/CatalogEntity sort
// identically to when `kind` was a plain `String` (lexical by keyword).
impl PartialOrd for EntityKind {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for EntityKind {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.as_str().cmp(other.as_str())
	}
}

impl Serialize for EntityKind {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(self.as_str())
	}
}

impl<'de> Deserialize<'de> for EntityKind {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let s = String::deserialize(deserializer)?;
		Ok(Self::from_storage(&s))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityKey {
	pub kind: EntityKind,
	pub scope: Option<String>,
	pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogEntity {
	pub kind: EntityKind,
	pub scope: Option<String>,
	pub name: String,
	pub source_path: String,
	pub statement_hash: String,
	pub file_hash: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileDiff {
	pub added: Vec<String>,
	pub modified: Vec<String>,
	pub removed: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CatalogDiff {
	pub added: Vec<CatalogEntity>,
	pub removed: Vec<CatalogEntity>,
	pub modified: Vec<CatalogChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogChange {
	pub old: CatalogEntity,
	pub new: CatalogEntity,
}

impl CatalogEntity {
	pub fn key(&self) -> EntityKey {
		EntityKey {
			kind: self.kind.clone(),
			scope: self.scope.clone(),
			name: self.name.clone(),
		}
	}
}

/// A non-DEFINE SQL statement collected from a schema file when
/// `allow_all_statements` is enabled (e.g. INSERT, UPDATE, CREATE).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Operation {
	pub sql: String,
	pub source_path: String,
}

pub fn ensure_local_state_dirs(folder: &str) -> Result<()> {
	let sd = schema_dir(folder);
	let rd = rollouts_dir(folder);
	let std = state_dir(folder);
	fs::create_dir_all(&sd).with_context(|| format!("creating {}", sd.display()))?;
	fs::create_dir_all(&rd).with_context(|| format!("creating {}", rd.display()))?;
	fs::create_dir_all(&std).with_context(|| format!("creating {}", std.display()))?;
	Ok(())
}

pub fn collect_schema_files(folder: &str) -> Result<Vec<SchemaFile>> {
	let sd = schema_dir(folder);
	let mut files: Vec<PathBuf> = WalkDir::new(&sd)
		.follow_links(true)
		.into_iter()
		.filter_map(|e| e.ok())
		.filter(|e| e.file_type().is_file())
		.map(|e| e.into_path())
		.filter(|p| p.extension().and_then(|s| s.to_str()) == Some("surql"))
		.collect();

	files.sort();

	let mut out = Vec::with_capacity(files.len());
	for path in files {
		let sql = fs::read_to_string(&path).with_context(|| format!("reading {:?}", path))?;
		let hash = sha256_hex(sql.as_bytes());
		let path_str = normalize_path(&path)?;
		out.push(SchemaFile {
			path: path_str,
			sql,
			hash,
		});
	}

	Ok(out)
}

pub fn snapshot_from_files(files: &[SchemaFile]) -> SchemaSnapshot {
	let mut entries: Vec<SchemaSnapshotEntry> = files
		.iter()
		.map(|f| SchemaSnapshotEntry {
			path: f.path.clone(),
			hash: f.hash.clone(),
		})
		.collect();
	entries.sort();
	SchemaSnapshot {
		version: 1,
		files: entries,
	}
}

pub fn hash_schema_snapshot(snapshot: &SchemaSnapshot) -> Result<String> {
	let canonical = serde_json::to_vec(snapshot).context("serializing schema snapshot")?;
	Ok(sha256_hex(&canonical))
}

pub fn load_schema_snapshot(folder: &str) -> Result<SchemaSnapshot> {
	load_json_or_default(
		schema_snapshot_path(folder),
		SchemaSnapshot {
			version: 1,
			files: Vec::new(),
		},
	)
}

pub fn save_schema_snapshot(folder: &str, snapshot: &SchemaSnapshot) -> Result<()> {
	save_json_pretty(schema_snapshot_path(folder), snapshot)
}

pub fn load_catalog_snapshot(folder: &str) -> Result<CatalogSnapshot> {
	load_json_or_default(
		catalog_snapshot_path(folder),
		CatalogSnapshot {
			version: 2,
			entities: Vec::new(),
			operations: Vec::new(),
		},
	)
}

pub fn save_catalog_snapshot(folder: &str, snapshot: &CatalogSnapshot) -> Result<()> {
	save_json_pretty(catalog_snapshot_path(folder), snapshot)
}

pub fn diff_schema(old: &SchemaSnapshot, new: &SchemaSnapshot) -> FileDiff {
	let old_map: BTreeMap<&str, &str> =
		old.files.iter().map(|f| (f.path.as_str(), f.hash.as_str())).collect();
	let new_map: BTreeMap<&str, &str> =
		new.files.iter().map(|f| (f.path.as_str(), f.hash.as_str())).collect();

	let mut added = Vec::new();
	let mut modified = Vec::new();
	let mut removed = Vec::new();

	for (path, hash) in &new_map {
		match old_map.get(path) {
			None => added.push((*path).to_string()),
			Some(old_hash) if old_hash != hash => modified.push((*path).to_string()),
			_ => {}
		}
	}

	for path in old_map.keys() {
		if !new_map.contains_key(path) {
			removed.push((*path).to_string());
		}
	}

	FileDiff {
		added,
		modified,
		removed,
	}
}

pub fn build_catalog_snapshot(
	files: &[SchemaFile],
	allow_all_statements: bool,
) -> Result<CatalogSnapshot> {
	let mut entities = BTreeSet::new();
	let mut operations = Vec::new();
	for file in files {
		let (file_entities, file_ops) = parse_schema_statements(file, allow_all_statements)?;
		for entity in file_entities {
			entities.insert(entity);
		}
		operations.extend(file_ops);
	}

	Ok(CatalogSnapshot {
		version: 2,
		entities: entities.into_iter().collect(),
		operations,
	})
}

/// Parse statements from a schema file.
///
/// Returns a tuple of `(entities, operations)`:
/// - `entities`: catalog entities extracted from `DEFINE` statements
/// - `operations`: raw SQL strings for non-`DEFINE` statements (only populated when
///   `allow_all_statements` is `true`; otherwise any non-`DEFINE` statement is a hard error)
pub fn parse_schema_statements(
	file: &SchemaFile,
	allow_all_statements: bool,
) -> Result<(Vec<CatalogEntity>, Vec<Operation>)> {
	let mut entities = Vec::new();
	let mut operations = Vec::new();
	for stmt in split_statements(&strip_comments(&file.sql)) {
		let normalized = stmt.trim();
		if normalized.is_empty() {
			continue;
		}
		let upper = normalized.to_ascii_uppercase();
		if upper.starts_with("REMOVE ") {
			bail!(
				"schema file '{}' contains a REMOVE statement; destructive SQL must live in rollout steps",
				file.path
			);
		}
		if upper.starts_with("LET ") {
			continue;
		}
		if !upper.starts_with("DEFINE ") {
			if allow_all_statements {
				operations.push(Operation {
					sql: normalized.to_string(),
					source_path: file.path.clone(),
				});
				continue;
			}
			bail!(
				"schema file '{}' contains a non-DEFINE statement: '{}'",
				file.path,
				truncate_stmt(normalized)
			);
		}
		let after_define = upper["DEFINE ".len()..].trim_start();
		if after_define.starts_with("NAMESPACE") || after_define.starts_with("DATABASE") {
			bail!(
				"schema file '{}' contains DEFINE NAMESPACE/DATABASE, which surrealkit does not manage: \
sync runs inside an already-selected namespace/database. Provision these out-of-band.",
				file.path
			);
		}
		let Some(mut entity) = parse_define_entity(normalized) else {
			bail!(
				"schema file '{}' contains an unsupported DEFINE statement: '{}'",
				file.path,
				truncate_stmt(normalized)
			);
		};
		entity.source_path = file.path.clone();
		entity.file_hash = file.hash.clone();
		entity.statement_hash = sha256_hex(normalize_statement(normalized).as_bytes());
		entities.push(entity);
	}
	Ok((entities, operations))
}

pub fn catalog_snapshot_to_map(snapshot: &CatalogSnapshot) -> BTreeMap<EntityKey, CatalogEntity> {
	snapshot.entities.iter().cloned().map(|entity| (entity.key(), entity)).collect()
}

pub fn diff_catalog(old: &CatalogSnapshot, new: &CatalogSnapshot) -> CatalogDiff {
	let old_map = catalog_snapshot_to_map(old);
	let new_map = catalog_snapshot_to_map(new);
	let mut diff = CatalogDiff::default();

	for (key, new_entity) in &new_map {
		match old_map.get(key) {
			None => diff.added.push(new_entity.clone()),
			Some(old_entity) if old_entity.statement_hash != new_entity.statement_hash => {
				diff.modified.push(CatalogChange {
					old: old_entity.clone(),
					new: new_entity.clone(),
				});
			}
			_ => {}
		}
	}

	for (key, old_entity) in &old_map {
		if !new_map.contains_key(key) {
			diff.removed.push(old_entity.clone());
		}
	}

	diff.added.sort();
	diff.removed.sort();
	diff.modified.sort_by(|a, b| a.old.cmp(&b.old));
	diff
}

pub fn render_remove_sql(entities: &[EntityKey], api_supported: bool) -> Result<Vec<String>> {
	let mut ordered = entities.to_vec();
	ordered.sort_by_key(removal_sort_key);

	// All emitted statements include `IF EXISTS` so the pruner is idempotent
	// against catalog drift. If an entity was removed from the live DB outside
	// of surrealkit (e.g., a `run_sql REMOVE …` rollout step that bypassed the
	// catalog), the next sync would otherwise fail with `X does not exist` and
	// halt — leaving the catalog and DB stuck out of sync. With `IF EXISTS` the
	// prune succeeds, the catalog row is deleted by the surrounding code, and
	// drift self-heals.
	let mut out = Vec::new();
	for entity in ordered {
		let stmt = match &entity.kind {
			EntityKind::Field => format!(
				"REMOVE FIELD IF EXISTS {} ON {};",
				entity.name,
				scope_or_err(&entity, "FIELD")?
			),
			EntityKind::Event => format!(
				"REMOVE EVENT IF EXISTS {} ON {};",
				entity.name,
				scope_or_err(&entity, "EVENT")?
			),
			EntityKind::Index => format!(
				"REMOVE INDEX IF EXISTS {} ON {};",
				entity.name,
				scope_or_err(&entity, "INDEX")?
			),
			EntityKind::Table => format!("REMOVE TABLE IF EXISTS {};", entity.name),
			EntityKind::Function => format!("REMOVE FUNCTION IF EXISTS {};", entity.name),
			EntityKind::Param => format!("REMOVE PARAM IF EXISTS {};", entity.name),
			EntityKind::Access => match &entity.scope {
				Some(scope) => format!("REMOVE ACCESS IF EXISTS {} ON {};", entity.name, scope),
				None => format!("REMOVE ACCESS IF EXISTS {};", entity.name),
			},
			EntityKind::Analyzer => format!("REMOVE ANALYZER IF EXISTS {};", entity.name),
			EntityKind::User => match &entity.scope {
				Some(scope) => format!("REMOVE USER IF EXISTS {} ON {};", entity.name, scope),
				None => format!("REMOVE USER IF EXISTS {};", entity.name),
			},
			EntityKind::Api => {
				if api_supported {
					format!("REMOVE API IF EXISTS {};", entity.name)
				} else {
					bail!(
						"API removal requested for '{}' but this SurrealDB server does not support `REMOVE API`. \
Use a manual migration or upgrade server support.",
						entity.name
					);
				}
			}
			EntityKind::Bucket => format!("REMOVE BUCKET IF EXISTS {};", entity.name),
			EntityKind::Model => format!("REMOVE MODEL IF EXISTS {};", entity.name),
			EntityKind::Sequence => format!("REMOVE SEQUENCE IF EXISTS {};", entity.name),
			EntityKind::Config => format!("REMOVE CONFIG IF EXISTS {};", entity.name),
			EntityKind::Module => format!("REMOVE MODULE IF EXISTS {};", entity.name),
			// Unknown kinds from foreign/newer state: skip rather than fail the prune batch.
			EntityKind::Other(_) => continue,
		};
		out.push(stmt);
	}
	Ok(out)
}

fn scope_or_err(entity: &EntityKey, object: &str) -> Result<String> {
	entity.scope.clone().ok_or_else(|| {
		anyhow!("cannot render REMOVE {} for '{}' because scope is missing", object, entity.name)
	})
}

fn removal_sort_key(entity: &EntityKey) -> (usize, Option<String>, String, String) {
	let weight = match entity.kind {
		EntityKind::Index => 0,
		EntityKind::Event => 1,
		EntityKind::Field => 2,
		EntityKind::Access => 3,
		EntityKind::User => 4,
		EntityKind::Function => 5,
		EntityKind::Param => 6,
		EntityKind::Api => 7,
		EntityKind::Analyzer => 8,
		EntityKind::Bucket => 9,
		EntityKind::Model => 10,
		EntityKind::Module => 11,
		EntityKind::Sequence => 12,
		EntityKind::Config => 13,
		EntityKind::Table => 14,
		EntityKind::Other(_) => 15,
	};
	(weight, entity.scope.clone(), entity.kind.to_string(), entity.name.clone())
}

fn normalize_path(path: &Path) -> Result<String> {
	let cwd = std::env::current_dir().context("resolving current directory")?;
	let rel = path.strip_prefix(&cwd).or_else(|_| path.strip_prefix(".")).unwrap_or(path);
	Ok(rel.to_string_lossy().replace('\\', "/"))
}

fn load_json_or_default<T>(path: impl AsRef<std::path::Path>, default: T) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	let p = path.as_ref();
	if !p.exists() {
		return Ok(default);
	}

	let raw = fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
	let parsed = serde_json::from_str(&raw).with_context(|| format!("parsing {}", p.display()))?;
	Ok(parsed)
}

fn save_json_pretty<T>(path: impl AsRef<std::path::Path>, value: &T) -> Result<()>
where
	T: Serialize,
{
	let p = path.as_ref();
	if let Some(parent) = p.parent() {
		fs::create_dir_all(parent).with_context(|| format!("creating dir {}", parent.display()))?;
	}
	let raw = serde_json::to_string_pretty(value).context("serializing json")?;
	fs::write(p, format!("{raw}\n")).with_context(|| format!("writing {}", p.display()))?;
	Ok(())
}

/// Ensures every `DEFINE` statement includes the `OVERWRITE` modifier so that
/// sync can re-apply schemas idempotently against an existing database.
pub fn ensure_overwrite(sql: &str) -> String {
	let stmts = split_statements(&strip_comments(sql));
	let mut out = Vec::with_capacity(stmts.len());
	for stmt in stmts {
		let trimmed = stmt.trim();
		if trimmed.is_empty() {
			continue;
		}
		let upper = trimmed.to_ascii_uppercase();
		if upper.starts_with("DEFINE ") {
			let tokens: Vec<&str> = trimmed.splitn(4, char::is_whitespace).collect();
			// tokens: ["DEFINE", "<KIND>", ...]
			if tokens.len() >= 3 {
				let after_kind = &trimmed[tokens[0].len()..].trim_start();
				let after_kind_word = &after_kind[tokens[1].len()..].trim_start();
				let rest_upper = after_kind_word.to_ascii_uppercase();
				if rest_upper.starts_with("OVERWRITE") {
					out.push(format!("{};", trimmed));
				} else if rest_upper.starts_with("IF NOT EXISTS") {
					// Replace IF NOT EXISTS with OVERWRITE so sync always applies the latest
					// schema; IF NOT EXISTS would silently skip updates to existing entities.
					let after_ine = after_kind_word["IF NOT EXISTS".len()..].trim_start();
					out.push(format!("DEFINE {} OVERWRITE {};", tokens[1], after_ine));
				} else {
					out.push(format!("DEFINE {} OVERWRITE {};", tokens[1], after_kind_word));
				}
			} else {
				out.push(format!("{};", trimmed));
			}
		} else {
			out.push(format!("{};", trimmed));
		}
	}
	out.join("\n")
}

fn strip_comments(sql: &str) -> String {
	let mut out = String::with_capacity(sql.len());
	let mut chars = sql.chars().peekable();
	let mut in_single = false;
	let mut in_double = false;
	let mut in_backtick = false;
	let mut prev_escape = false;

	while let Some(ch) = chars.next() {
		let in_string = in_single || in_double || in_backtick;
		if !in_string && !prev_escape {
			// Line comments: --, //, #
			if (ch == '-' || ch == '/') && chars.peek() == Some(&ch) {
				chars.next();
				for c in chars.by_ref() {
					if c == '\n' {
						out.push('\n');
						break;
					}
				}
				prev_escape = false;
				continue;
			}
			if ch == '#' {
				for c in chars.by_ref() {
					if c == '\n' {
						out.push('\n');
						break;
					}
				}
				prev_escape = false;
				continue;
			}
			// Block comment: /* ... */ (non-nesting, preserves newlines so line numbers stay sane)
			if ch == '/' && chars.peek() == Some(&'*') {
				chars.next();
				let mut prev = '\0';
				for c in chars.by_ref() {
					if c == '\n' {
						out.push('\n');
					}
					if prev == '*' && c == '/' {
						break;
					}
					prev = c;
				}
				out.push(' ');
				prev_escape = false;
				continue;
			}
		}

		match ch {
			'\'' if !in_double && !in_backtick && !prev_escape => in_single = !in_single,
			'"' if !in_single && !in_backtick && !prev_escape => in_double = !in_double,
			'`' if !in_single && !in_double && !prev_escape => in_backtick = !in_backtick,
			_ => {}
		}
		prev_escape = ch == '\\' && !prev_escape;
		out.push(ch);
	}
	out
}

fn split_statements(sql: &str) -> Vec<String> {
	let mut out = Vec::new();
	let mut buf = String::new();
	let mut in_single = false;
	let mut in_double = false;
	let mut in_backtick = false;
	let mut prev_escape = false;
	let mut brace_depth = 0usize;

	for ch in sql.chars() {
		match ch {
			'\'' if !in_double && !in_backtick && !prev_escape => in_single = !in_single,
			'"' if !in_single && !in_backtick && !prev_escape => in_double = !in_double,
			'`' if !in_single && !in_double && !prev_escape => in_backtick = !in_backtick,
			'{' if !in_single && !in_double && !in_backtick => brace_depth += 1,
			'}' if !in_single && !in_double && !in_backtick && brace_depth > 0 => brace_depth -= 1,
			';' if !in_single && !in_double && !in_backtick && brace_depth == 0 => {
				let stmt = buf.trim();
				if !stmt.is_empty() {
					out.push(stmt.to_string());
				}
				buf.clear();
				prev_escape = false;
				continue;
			}
			_ => {}
		}

		prev_escape = ch == '\\' && !prev_escape;
		buf.push(ch);
	}

	let tail = buf.trim();
	if !tail.is_empty() {
		out.push(tail.to_string());
	}

	out
}

fn parse_define_entity(stmt: &str) -> Option<CatalogEntity> {
	let tokens = tokenize(stmt);
	if tokens.len() < 3 || !eq(tokens[0], "DEFINE") {
		return None;
	}

	let kind = EntityKind::from_str(tokens[1]).ok()?;
	let mut idx = 2;
	idx = skip_modifiers(&tokens, idx);
	if idx >= tokens.len() {
		return None;
	}

	let (scope, name) = match &kind {
		EntityKind::Table => (None, clean_ident(tokens[idx])),
		EntityKind::Field | EntityKind::Event | EntityKind::Index => {
			let name = clean_ident(tokens[idx]);
			let on_idx = find_token(&tokens, idx + 1, "ON")?;
			let mut scope_idx = on_idx + 1;
			if scope_idx < tokens.len() && eq(tokens[scope_idx], "TABLE") {
				scope_idx += 1;
			}
			if scope_idx >= tokens.len() {
				return None;
			}
			(Some(clean_ident(tokens[scope_idx])), name)
		}
		EntityKind::Function
		| EntityKind::Param
		| EntityKind::Analyzer
		| EntityKind::Api
		| EntityKind::Bucket
		| EntityKind::Model
		| EntityKind::Sequence
		| EntityKind::Config
		| EntityKind::Module => (None, clean_ident(tokens[idx])),
		EntityKind::Access | EntityKind::User => {
			let name = clean_ident(tokens[idx]);
			let scope = find_token(&tokens, idx + 1, "ON").and_then(|on_idx| {
				let i = on_idx + 1;
				if i < tokens.len() {
					Some(clean_ident(tokens[i]))
				} else {
					None
				}
			});
			(scope, name)
		}
		_ => return None,
	};

	Some(CatalogEntity {
		kind,
		scope,
		name,
		source_path: String::new(),
		statement_hash: String::new(),
		file_hash: String::new(),
	})
}

fn tokenize(stmt: &str) -> Vec<&str> {
	stmt.split_whitespace().collect()
}

fn clean_ident(token: &str) -> String {
	let trimmed = token.trim_matches(|c: char| {
		c == ',' || c == ';' || c == '(' || c == ')' || c == '{' || c == '}'
	});
	let core = match trimmed.find('(') {
		Some(pos) => &trimmed[..pos],
		None => trimmed,
	};
	core.to_string()
}

fn skip_modifiers(tokens: &[&str], mut idx: usize) -> usize {
	while idx < tokens.len()
		&& (eq(tokens[idx], "OVERWRITE")
			|| eq(tokens[idx], "IF")
			|| eq(tokens[idx], "NOT")
			|| eq(tokens[idx], "EXISTS"))
	{
		idx += 1;
	}
	idx
}

fn find_token(tokens: &[&str], start: usize, target: &str) -> Option<usize> {
	(start..tokens.len()).find(|&i| eq(tokens[i], target))
}

fn eq(value: &str, expected: &str) -> bool {
	value.eq_ignore_ascii_case(expected)
}

fn normalize_statement(stmt: &str) -> String {
	let mut out = String::new();
	let mut prev_space = false;
	for ch in stmt.trim().chars() {
		if ch.is_whitespace() {
			if !prev_space {
				out.push(' ');
			}
			prev_space = true;
		} else {
			out.push(ch);
			prev_space = false;
		}
	}
	out
}

fn truncate_stmt(stmt: &str) -> String {
	const LIMIT: usize = 96;
	if stmt.len() <= LIMIT {
		stmt.to_string()
	} else {
		format!("{}...", &stmt[..LIMIT])
	}
}

#[cfg(test)]
mod tests {
	use test_case::test_case;

	use super::*;

	#[test]
	fn entity_kind_serializes_to_lowercase_keyword() {
		// Wire-compat: kind must serialize to the same lowercase strings that
		// previous releases stored, so existing catalog snapshots / TOML / __entity
		// rows keep round-tripping.
		assert_eq!(serde_json::to_string(&EntityKind::Table).unwrap(), "\"table\"");
		assert_eq!(serde_json::to_string(&EntityKind::Api).unwrap(), "\"api\"");
		assert_eq!(serde_json::to_string(&EntityKind::Module).unwrap(), "\"module\"");
	}

	#[test]
	fn entity_kind_deserializes_known_and_unknown() {
		assert_eq!(serde_json::from_str::<EntityKind>("\"table\"").unwrap(), EntityKind::Table);
		// An unknown kind from a newer/foreign writer must survive instead of failing.
		let other: EntityKind = serde_json::from_str("\"galaxy\"").unwrap();
		assert_eq!(other, EntityKind::Other("galaxy".to_string()));
		assert_eq!(other.as_str(), "galaxy");
		// ...and round-trip back to the original string.
		assert_eq!(serde_json::to_string(&other).unwrap(), "\"galaxy\"");
	}

	#[test]
	fn entity_kind_from_str_is_strict_and_case_insensitive() {
		assert_eq!("TABLE".parse::<EntityKind>().unwrap(), EntityKind::Table);
		assert_eq!("Field".parse::<EntityKind>().unwrap(), EntityKind::Field);
		assert!("galaxy".parse::<EntityKind>().is_err());
	}

	#[test]
	fn schema_diff_detects_added_modified_removed() {
		let old = SchemaSnapshot {
			version: 1,
			files: vec![
				SchemaSnapshotEntry {
					path: "database/schema/a.surql".to_string(),
					hash: "1".to_string(),
				},
				SchemaSnapshotEntry {
					path: "database/schema/b.surql".to_string(),
					hash: "2".to_string(),
				},
			],
		};
		let new = SchemaSnapshot {
			version: 1,
			files: vec![
				SchemaSnapshotEntry {
					path: "database/schema/b.surql".to_string(),
					hash: "3".to_string(),
				},
				SchemaSnapshotEntry {
					path: "database/schema/c.surql".to_string(),
					hash: "4".to_string(),
				},
			],
		};

		let diff = diff_schema(&old, &new);
		assert_eq!(diff.added, vec!["database/schema/c.surql"]);
		assert_eq!(diff.modified, vec!["database/schema/b.surql"]);
		assert_eq!(diff.removed, vec!["database/schema/a.surql"]);
	}

	#[test]
	fn catalog_extracts_supported_entities() {
		let files = vec![SchemaFile {
			path: "database/schema/root.surql".to_string(),
			hash: "x".to_string(),
			sql: r#"
				DEFINE TABLE OVERWRITE person SCHEMAFULL;
				DEFINE FIELD OVERWRITE name ON person TYPE string;
				DEFINE EVENT changed ON person WHEN true THEN ();
				DEFINE INDEX by_name ON TABLE person FIELDS name;
				DEFINE FUNCTION fn::greet($name: string) { RETURN $name; };
				DEFINE PARAM $env VALUE "dev";
				DEFINE ACCESS admin ON DATABASE TYPE RECORD;
				DEFINE ANALYZER english TOKENIZERS blank, class;
				DEFINE USER app ON DATABASE PASSHASH "x";
				DEFINE API v1;
				DEFINE BUCKET assets;
				DEFINE SEQUENCE order_no;
				DEFINE CONFIG GRAPHQL AUTO;
					DEFINE MODULE mod::math AS f"math:/math.surli";
			"#
			.to_string(),
		}];

		let catalog = build_catalog_snapshot(&files, false).expect("catalog build");
		assert!(catalog.entities.contains(&CatalogEntity {
			kind: EntityKind::Table,
			scope: None,
			name: "person".to_string(),
			source_path: "database/schema/root.surql".to_string(),
			statement_hash: sha256_hex("DEFINE TABLE OVERWRITE person SCHEMAFULL".as_bytes()),
			file_hash: "x".to_string(),
		}));
		assert!(catalog.entities.iter().any(|entity| {
			entity.kind == EntityKind::Field
				&& entity.scope.as_deref() == Some("person")
				&& entity.name == "name"
				&& entity.source_path == "database/schema/root.surql"
		}));
		assert!(catalog.entities.iter().any(|entity| entity.kind == EntityKind::Api && entity.name == "v1"));
		assert!(
			catalog.entities.iter().any(|e| e.kind == EntityKind::Bucket && e.name == "assets"),
			"bucket should be captured"
		);
		assert!(
			catalog.entities.iter().any(|e| e.kind == EntityKind::Sequence && e.name == "order_no"),
			"sequence should be captured"
		);
		assert!(
			catalog.entities.iter().any(|e| e.kind == EntityKind::Config && e.name == "GRAPHQL"),
			"config should be captured by its kind keyword"
		);
		assert!(
			catalog
				.entities
				.iter()
				.any(|e| { e.kind == EntityKind::Module && e.scope.is_none() && e.name == "mod::math" }),
			"module should be captured with its mod:: name"
		);
	}

	#[test_case("DEFINE NAMESPACE prod;")]
	#[test_case("DEFINE DATABASE prod;")]
	fn schema_rejects_define_namespace_and_database(stmt: &str) {
		let file = SchemaFile {
			path: "database/schema/root.surql".to_string(),
			hash: "x".to_string(),
			sql: stmt.to_string(),
		};
		let err = parse_schema_statements(&file, false)
			.expect_err("DEFINE NAMESPACE/DATABASE must be rejected");
		assert!(
			err.to_string().contains("DEFINE NAMESPACE/DATABASE"),
			"unexpected error for {stmt}: {err}"
		);
	}

	#[test]
	fn render_remove_sql_covers_new_kinds() {
		let entities = vec![
			EntityKey {
				kind: EntityKind::Bucket,
				scope: None,
				name: "assets".to_string(),
			},
			EntityKey {
				kind: EntityKind::Sequence,
				scope: None,
				name: "order_no".to_string(),
			},
			EntityKey {
				kind: EntityKind::Config,
				scope: None,
				name: "GRAPHQL".to_string(),
			},
			EntityKey {
				kind: EntityKind::Model,
				scope: None,
				name: "ml::sentiment".to_string(),
			},
			EntityKey {
				kind: EntityKind::Module,
				scope: None,
				name: "mod::math".to_string(),
			},
		];
		let out = render_remove_sql(&entities, true).expect("remove sql");
		assert!(out.iter().any(|l| l == "REMOVE BUCKET IF EXISTS assets;"));
		assert!(out.iter().any(|l| l == "REMOVE SEQUENCE IF EXISTS order_no;"));
		assert!(out.iter().any(|l| l == "REMOVE CONFIG IF EXISTS GRAPHQL;"));
		assert!(out.iter().any(|l| l == "REMOVE MODEL IF EXISTS ml::sentiment;"));
		assert!(out.iter().any(|l| l == "REMOVE MODULE IF EXISTS mod::math;"));
	}

	#[test]
	fn ensure_overwrite_handles_define_module() {
		// Sync sends DEFINE statements with OVERWRITE injected so re-applying is
		// idempotent. A `DEFINE MODULE mod::x AS f"..."` must gain OVERWRITE right
		// after the MODULE keyword (where surrealdb's parser expects it), and an
		// explicit IF NOT EXISTS must be rewritten to OVERWRITE.
		let plain = ensure_overwrite("DEFINE MODULE mod::math AS f\"math:/math.surli\";");
		assert_eq!(plain.trim(), "DEFINE MODULE OVERWRITE mod::math AS f\"math:/math.surli\";");

		let if_not_exists =
			ensure_overwrite("DEFINE MODULE IF NOT EXISTS mod::math AS f\"math:/math.surli\";");
		assert_eq!(
			if_not_exists.trim(),
			"DEFINE MODULE OVERWRITE mod::math AS f\"math:/math.surli\";"
		);
	}

	#[test]
	fn module_removed_after_dependents_and_before_table() {
		// A field/event default expression may call mod::x::fn(), so the module must
		// be removed after fields (which are dropped first) but before the table.
		let entities = vec![
			EntityKey {
				kind: EntityKind::Table,
				scope: None,
				name: "person".into(),
			},
			EntityKey {
				kind: EntityKind::Module,
				scope: None,
				name: "mod::math".into(),
			},
			EntityKey {
				kind: EntityKind::Field,
				scope: Some("person".into()),
				name: "age".into(),
			},
		];
		let out = render_remove_sql(&entities, true).expect("render");
		let field_idx = out.iter().position(|l| l.starts_with("REMOVE FIELD")).expect("field");
		let module_idx = out.iter().position(|l| l.starts_with("REMOVE MODULE")).expect("module");
		let table_idx = out.iter().position(|l| l.starts_with("REMOVE TABLE")).expect("table");
		assert!(field_idx < module_idx, "field must be removed before module");
		assert!(module_idx < table_idx, "module must be removed before table");
	}

	#[test]
	fn render_remove_sql_respects_api_support() {
		let entities = vec![
			EntityKey {
				kind: EntityKind::Table,
				scope: None,
				name: "person".to_string(),
			},
			EntityKey {
				kind: EntityKind::Field,
				scope: Some("person".to_string()),
				name: "nickname".to_string(),
			},
			EntityKey {
				kind: EntityKind::Api,
				scope: None,
				name: "v1".to_string(),
			},
		];

		let supported = render_remove_sql(&entities, true).expect("api should be supported");
		assert_eq!(supported[0], "REMOVE FIELD IF EXISTS nickname ON person;");
		assert!(supported.iter().any(|line| line == "REMOVE API IF EXISTS v1;"));
		assert_eq!(supported.last().expect("table removal"), "REMOVE TABLE IF EXISTS person;");

		let unsupported = render_remove_sql(&entities, false);
		assert!(unsupported.is_err());
	}

	#[test]
	fn render_remove_sql_emits_if_exists_for_every_kind() {
		// Without IF EXISTS, a single missing entity would halt the whole prune
		// batch in sync.rs::prune_managed_entities (REMOVEs are joined and
		// executed together). Verify every kind we render carries IF EXISTS so
		// catalog drift is self-healing.
		let entities = vec![
			EntityKey {
				kind: EntityKind::Field,
				scope: Some("person".into()),
				name: "nickname".into(),
			},
			EntityKey {
				kind: EntityKind::Event,
				scope: Some("person".into()),
				name: "audit".into(),
			},
			EntityKey {
				kind: EntityKind::Index,
				scope: Some("person".into()),
				name: "name_idx".into(),
			},
			EntityKey {
				kind: EntityKind::Table,
				scope: None,
				name: "person".into(),
			},
			EntityKey {
				kind: EntityKind::Function,
				scope: None,
				name: "fn::greet".into(),
			},
			EntityKey {
				kind: EntityKind::Param,
				scope: None,
				name: "$greeting".into(),
			},
			EntityKey {
				kind: EntityKind::Access,
				scope: Some("DATABASE".into()),
				name: "user_jwt".into(),
			},
			EntityKey {
				kind: EntityKind::Analyzer,
				scope: None,
				name: "blank".into(),
			},
			EntityKey {
				kind: EntityKind::User,
				scope: Some("DATABASE".into()),
				name: "admin".into(),
			},
			EntityKey {
				kind: EntityKind::Api,
				scope: None,
				name: "v1".into(),
			},
			EntityKey {
				kind: EntityKind::Bucket,
				scope: None,
				name: "assets".into(),
			},
			EntityKey {
				kind: EntityKind::Model,
				scope: None,
				name: "ml::sentiment".into(),
			},
			EntityKey {
				kind: EntityKind::Sequence,
				scope: None,
				name: "order_no".into(),
			},
			EntityKey {
				kind: EntityKind::Config,
				scope: None,
				name: "GRAPHQL".into(),
			},
			EntityKey {
				kind: EntityKind::Module,
				scope: None,
				name: "mod::math".into(),
			},
		];
		let out = render_remove_sql(&entities, true).expect("render");
		for stmt in &out {
			assert!(
				stmt.contains("IF EXISTS"),
				"every REMOVE must include IF EXISTS so prune is idempotent against catalog drift; offending: {stmt}"
			);
		}
		assert_eq!(out.len(), entities.len(), "every kind should produce a stmt");
	}

	#[test_case("CREATE person SET name = 'a';")]
	#[test_case("INSERT INTO person (name) VALUES ('Alice');")]
	#[test_case("UPDATE person SET name = 'Bob';")]
	#[test_case("DELETE FROM person WHERE name = 'Bob';")]
	#[test_case("SELECT * FROM person;")]
	fn schema_rejects_non_define_sql(stmt: &str) {
		let file = SchemaFile {
			path: "database/schema/root.surql".to_string(),
			hash: "x".to_string(),
			sql: stmt.to_string(),
		};
		let err = parse_schema_statements(&file, false)
			.expect_err(&format!("must reject non-DEFINE: {stmt}"));
		assert!(err.to_string().contains("non-DEFINE"), "unexpected error for {stmt}: {err}");
	}

	#[test]
	fn ensure_overwrite_passes_through_non_define_statements() {
		// When --allow-all-statements is used, schema files may contain INSERT/UPDATE/etc.
		// ensure_overwrite must pass these through unchanged (no OVERWRITE injection).
		let sql = "DEFINE TABLE person SCHEMAFULL;\n\
		           INSERT INTO person (name) VALUES ('seed');";
		let result = ensure_overwrite(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE person SCHEMAFULL;"));
		assert!(result.contains("INSERT INTO person (name) VALUES ('seed');"));
	}

	#[test_case("CREATE person SET name = 'a';")]
	#[test_case("INSERT INTO person (name) VALUES ('Alice');")]
	#[test_case("UPDATE person SET name = 'Bob';")]
	#[test_case("DELETE FROM person WHERE name = 'Bob';")]
	#[test_case("SELECT * FROM person;")]
	fn allow_all_statements_collects_non_define_as_operations(stmt: &str) {
		let file = SchemaFile {
			path: "database/schema/root.surql".to_string(),
			hash: "x".to_string(),
			sql: stmt.to_string(),
		};
		let (entities, ops) = parse_schema_statements(&file, true)
			.expect(&format!("allow_all_statements should not fail for: {stmt}"));
		assert!(entities.is_empty(), "no catalog entity expected for: {stmt}");
		assert_eq!(ops.len(), 1, "expected one operation for: {stmt}");
		assert_eq!(ops[0].source_path, "database/schema/root.surql");
	}

	#[test]
	fn allow_all_statements_mixed_define_and_operations() {
		let sql = "DEFINE TABLE person SCHEMAFULL;\n\
		           INSERT INTO person (name) VALUES ('seed');";
		let file = SchemaFile {
			path: "database/schema/mixed.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};
		let (entities, ops) = parse_schema_statements(&file, true)
			.expect("allow_all_statements should parse mixed file");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Table);
		assert_eq!(ops.len(), 1);
		assert!(ops[0].sql.contains("INSERT INTO person"));
	}

	#[test]
	fn allow_all_statements_still_rejects_remove() {
		let file = SchemaFile {
			path: "database/schema/root.surql".to_string(),
			hash: "x".to_string(),
			sql: "REMOVE TABLE person;".to_string(),
		};
		let err = parse_schema_statements(&file, true)
			.expect_err("REMOVE must still be rejected even with allow_all_statements");
		assert!(err.to_string().contains("REMOVE statement"));
	}

	#[test]
	fn schema_allows_inline_dash_dash_comments() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD kind ON foo TYPE string; -- enum: A, B, C\n\
		           DEFINE FIELD name ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("inline -- comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_allows_inline_slash_slash_comments() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD kind ON foo TYPE string; // enum: A, B, C\n\
		           DEFINE FIELD name ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("inline // comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_preserves_dash_dash_inside_string_literal() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD note ON foo TYPE string DEFAULT 'a -- b // c';";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("string literal must be preserved");
		assert_eq!(entities.len(), 2);
		assert!(entities.iter().any(|e| e.name == "note"));
	}

	#[test]
	fn schema_allows_full_line_comments_everywhere() {
		let sql = "-- file header comment\n\
		           // second header line\n\
		           DEFINE TABLE foo SCHEMAFULL;\n\
		           -- between statements\n\
		           DEFINE FIELD a ON foo TYPE string;\n\
		           // also between\n\
		           DEFINE FIELD b ON foo TYPE string;\n\
		           -- trailing comment";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("full-line comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_allows_comment_at_end_of_file_without_newline() {
		let sql = "DEFINE TABLE foo SCHEMAFULL; -- no trailing newline";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) = parse_schema_statements(&file, false)
			.expect("trailing comment without newline must parse");
		assert_eq!(entities.len(), 1);
	}

	#[test]
	fn schema_allows_inline_comment_with_no_space_after_semicolon() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;--tight\n\
		           DEFINE FIELD a ON foo TYPE string;//also tight\n\
		           DEFINE FIELD b ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("tight inline comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_allows_mid_statement_comment_across_newline() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD a ON foo -- mid-statement\n\
		           TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("mid-statement comment must parse");
		assert_eq!(entities.len(), 2);
		assert!(entities.iter().any(|e| e.name == "a"));
	}

	#[test]
	fn schema_does_not_strip_single_dash_or_slash() {
		// Single '-' (e.g. in DEFAULT -1) and single '/' (division) must not be treated as
		// comments.
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD n ON foo TYPE number DEFAULT -1;\n\
		           DEFINE FIELD m ON foo TYPE number VALUE 10 / 2;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("single - and / must not be stripped");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_preserves_comment_markers_inside_double_and_backtick_strings() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD a ON foo TYPE string DEFAULT \"x -- y // z\";\n\
		           DEFINE FIELD b ON `foo--bar` TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) = parse_schema_statements(&file, false)
			.expect("comment markers inside \"...\" and `...` must be preserved");
		assert_eq!(entities.len(), 3);
		// The field 'b' must survive — if '--' inside backticks were stripped, the table
		// scope token would be truncated and the statement would fail to parse.
		assert!(entities.iter().any(|e| e.name == "b"));
	}

	#[test]
	fn schema_handles_empty_comments() {
		let sql = "DEFINE TABLE foo SCHEMAFULL; --\n\
		           DEFINE FIELD a ON foo TYPE string; //\n\
		           DEFINE FIELD b ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("empty comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_handles_triple_dash_marker() {
		// '---' is a comment ('--' then '-' which is part of the comment body).
		let sql = "DEFINE TABLE foo SCHEMAFULL; --- triple dash\n\
		           DEFINE FIELD a ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("triple-dash must parse");
		assert_eq!(entities.len(), 2);
	}

	#[test]
	fn schema_allows_hash_line_comments() {
		let sql = "# header\n\
		           DEFINE TABLE foo SCHEMAFULL; # inline hash\n\
		           DEFINE FIELD a ON foo TYPE string;\n\
		           # trailing\n\
		           DEFINE FIELD b ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("# comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_preserves_hash_inside_string_literal() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD a ON foo TYPE string DEFAULT '#1 ranked';";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("# inside string must be preserved");
		assert_eq!(entities.len(), 2);
	}

	#[test]
	fn schema_allows_block_comments_inline() {
		let sql = "DEFINE TABLE foo SCHEMAFULL; /* inline block */\n\
		           DEFINE FIELD a ON foo /* mid */ TYPE string;\n\
		           DEFINE FIELD b ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("inline block comments must parse");
		assert_eq!(entities.len(), 3);
	}

	#[test]
	fn schema_allows_block_comments_spanning_multiple_lines() {
		let sql = "/*\n\
		            file header\n\
		            second line\n\
		           */\n\
		           DEFINE TABLE foo SCHEMAFULL;\n\
		           /* between\n\
		              statements */\n\
		           DEFINE FIELD a ON foo TYPE string;";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("multi-line block comments must parse");
		assert_eq!(entities.len(), 2);
	}

	#[test]
	fn schema_preserves_block_comment_markers_inside_string() {
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD a ON foo TYPE string DEFAULT '/* not a comment */';";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("/* inside string must be preserved");
		assert_eq!(entities.len(), 2);
	}

	#[test]
	fn schema_handles_unterminated_block_comment() {
		// An unterminated /* swallows the rest of input — same as treating the rest as comment.
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n/* never closes";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("unterminated block must parse");
		assert_eq!(entities.len(), 1);
	}

	#[test]
	fn schema_handles_escaped_quote_before_comment() {
		// Escaped quote inside a string should not terminate the string, so any '--' that
		// follows on the same line (still inside the string) must not be treated as a comment.
		let sql = "DEFINE TABLE foo SCHEMAFULL;\n\
		           DEFINE FIELD a ON foo TYPE string DEFAULT 'it\\'s -- still in string';";
		let file = SchemaFile {
			path: "database/schema/foo.surql".to_string(),
			hash: "x".to_string(),
			sql: sql.to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("escaped quote inside string must parse");
		assert_eq!(entities.len(), 2);
	}

	#[test]
	fn schema_allows_let_variables() {
		let file = SchemaFile {
			path: "database/schema/storage.surql".to_string(),
			hash: "x".to_string(),
			sql: "LET $types = ['image/png', 'image/jpeg'];\nDEFINE TABLE OVERWRITE storage SCHEMAFULL;".to_string(),
		};

		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("LET should be allowed");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Table);
	}

	#[test]
	fn catalog_diff_detects_statement_changes() {
		let old = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: EntityKind::Field,
				scope: Some("person".to_string()),
				name: "nickname".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: "a".to_string(),
				file_hash: "file-a".to_string(),
			}],
			operations: Vec::new(),
		};
		let new = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: EntityKind::Field,
				scope: Some("person".to_string()),
				name: "nickname".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: "b".to_string(),
				file_hash: "file-b".to_string(),
			}],
			operations: Vec::new(),
		};

		let diff = diff_catalog(&old, &new);
		assert_eq!(diff.modified.len(), 1);
		assert_eq!(diff.modified[0].old.statement_hash, "a");
		assert_eq!(diff.modified[0].new.statement_hash, "b");
	}

	#[test]
	fn snapshot_from_files_is_sorted_for_determinism() {
		let files = vec![
			SchemaFile {
				path: "database/schema/z.surql".to_string(),
				sql: String::new(),
				hash: "z".to_string(),
			},
			SchemaFile {
				path: "database/schema/a.surql".to_string(),
				sql: String::new(),
				hash: "a".to_string(),
			},
		];

		let snap = snapshot_from_files(&files);
		assert_eq!(snap.files[0].path, "database/schema/a.surql");
		assert_eq!(snap.files[1].path, "database/schema/z.surql");
	}

	#[test]
	fn ensure_overwrite_injects_when_missing() {
		let sql = "DEFINE TABLE post SCHEMAFULL;\nDEFINE FIELD name ON post TYPE string;";
		let result = ensure_overwrite(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE post SCHEMAFULL;"));
		assert!(result.contains("DEFINE FIELD OVERWRITE name ON post TYPE string;"));
	}

	#[test]
	fn ensure_overwrite_preserves_existing() {
		let sql = "DEFINE TABLE OVERWRITE post SCHEMAFULL;";
		let result = ensure_overwrite(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE post SCHEMAFULL;"));
		// Should not double up OVERWRITE
		assert!(!result.contains("OVERWRITE OVERWRITE"));
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_with_overwrite() {
		// IF NOT EXISTS prevents schema changes from being applied in sync;
		// ensure_overwrite must replace it with OVERWRITE so updates are not silently skipped.
		let sql = "DEFINE TABLE IF NOT EXISTS post SCHEMAFULL;";
		let result = ensure_overwrite(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE post SCHEMAFULL;"), "got: {result}");
		assert!(!result.contains("IF NOT EXISTS"), "IF NOT EXISTS should be replaced: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_field() {
		let sql = "DEFINE FIELD IF NOT EXISTS email ON person TYPE string;";
		let result = ensure_overwrite(sql);
		assert!(
			result.contains("DEFINE FIELD OVERWRITE email ON person TYPE string;"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_event() {
		let sql = "DEFINE EVENT IF NOT EXISTS changed ON person WHEN true THEN ();";
		let result = ensure_overwrite(sql);
		assert!(
			result.contains("DEFINE EVENT OVERWRITE changed ON person WHEN true THEN ();"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_index() {
		let sql = "DEFINE INDEX IF NOT EXISTS by_email ON TABLE person FIELDS email;";
		let result = ensure_overwrite(sql);
		assert!(
			result.contains("DEFINE INDEX OVERWRITE by_email ON TABLE person FIELDS email;"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_function() {
		let sql = "DEFINE FUNCTION IF NOT EXISTS fn::greet($name: string) { RETURN $name; };";
		let result = ensure_overwrite(sql);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
		assert!(result.contains("DEFINE FUNCTION OVERWRITE"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_param() {
		let sql = "DEFINE PARAM IF NOT EXISTS $env VALUE 'dev';";
		let result = ensure_overwrite(sql);
		assert!(result.contains("DEFINE PARAM OVERWRITE $env VALUE 'dev';"), "got: {result}");
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_analyzer() {
		let sql = "DEFINE ANALYZER IF NOT EXISTS english TOKENIZERS blank, class;";
		let result = ensure_overwrite(sql);
		assert!(
			result.contains("DEFINE ANALYZER OVERWRITE english TOKENIZERS blank, class;"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_access() {
		let sql = "DEFINE ACCESS IF NOT EXISTS admin ON DATABASE TYPE RECORD;";
		let result = ensure_overwrite(sql);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
		assert!(result.contains("DEFINE ACCESS OVERWRITE"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_user() {
		let sql = "DEFINE USER IF NOT EXISTS app ON DATABASE PASSHASH 'x';";
		let result = ensure_overwrite(sql);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
		assert!(result.contains("DEFINE USER OVERWRITE"), "got: {result}");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_table() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE TABLE IF NOT EXISTS person SCHEMAFULL;".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS table");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Table);
		assert_eq!(entities[0].name, "person");
		assert!(entities[0].scope.is_none());
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_field() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE FIELD IF NOT EXISTS email ON person TYPE string;".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS field");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Field);
		assert_eq!(entities[0].name, "email");
		assert_eq!(entities[0].scope.as_deref(), Some("person"));
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_event() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE EVENT IF NOT EXISTS changed ON person WHEN true THEN ();".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS event");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Event);
		assert_eq!(entities[0].name, "changed");
		assert_eq!(entities[0].scope.as_deref(), Some("person"));
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_index() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE INDEX IF NOT EXISTS by_email ON TABLE person FIELDS email;".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS index");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Index);
		assert_eq!(entities[0].name, "by_email");
		assert_eq!(entities[0].scope.as_deref(), Some("person"));
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_function() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE FUNCTION IF NOT EXISTS fn::greet($name: string) { RETURN $name; };"
				.to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS function");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Function);
		assert_eq!(entities[0].name, "fn::greet");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_param() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE PARAM IF NOT EXISTS $env VALUE 'dev';".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS param");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Param);
		assert_eq!(entities[0].name, "$env");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_analyzer() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE ANALYZER IF NOT EXISTS english TOKENIZERS blank, class;".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS analyzer");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Analyzer);
		assert_eq!(entities[0].name, "english");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_access() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE ACCESS IF NOT EXISTS admin ON DATABASE TYPE RECORD;".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS access");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Access);
		assert_eq!(entities[0].name, "admin");
		assert_eq!(entities[0].scope.as_deref(), Some("DATABASE"));
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_user() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE USER IF NOT EXISTS app ON DATABASE PASSHASH 'x';".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS user");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::User);
		assert_eq!(entities[0].name, "app");
		assert_eq!(entities[0].scope.as_deref(), Some("DATABASE"));
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_module() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE MODULE IF NOT EXISTS mod::math AS f\"math:/math.surli\";".to_string(),
		};
		let (entities, _ops) =
			parse_schema_statements(&file, false).expect("should parse IF NOT EXISTS module");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, EntityKind::Module);
		assert_eq!(entities[0].name, "mod::math");
		assert!(entities[0].scope.is_none());
	}

	#[test]
	fn build_catalog_snapshot_handles_all_if_not_exists_types() {
		let files = vec![SchemaFile {
			path: "database/schema/ine.surql".to_string(),
			hash: "ine".to_string(),
			sql: r#"
				DEFINE TABLE IF NOT EXISTS person SCHEMAFULL;
				DEFINE FIELD IF NOT EXISTS name ON person TYPE string;
				DEFINE EVENT IF NOT EXISTS audit ON person WHEN true THEN ();
				DEFINE INDEX IF NOT EXISTS by_name ON TABLE person FIELDS name;
				DEFINE FUNCTION IF NOT EXISTS fn::greet($n: string) { RETURN $n; };
				DEFINE PARAM IF NOT EXISTS $env VALUE 'dev';
				DEFINE ACCESS IF NOT EXISTS admin ON DATABASE TYPE RECORD;
				DEFINE ANALYZER IF NOT EXISTS eng TOKENIZERS blank;
				DEFINE USER IF NOT EXISTS ops ON DATABASE PASSHASH 'x';
			"#
			.to_string(),
		}];

		let catalog =
			build_catalog_snapshot(&files, false).expect("catalog should handle IF NOT EXISTS");
		assert_eq!(catalog.entities.len(), 9, "all 9 entity types should be extracted");

		let kinds: Vec<&str> = catalog.entities.iter().map(|e| e.kind.as_str()).collect();
		assert!(kinds.contains(&"table"));
		assert!(kinds.contains(&"field"));
		assert!(kinds.contains(&"event"));
		assert!(kinds.contains(&"index"));
		assert!(kinds.contains(&"function"));
		assert!(kinds.contains(&"param"));
		assert!(kinds.contains(&"access"));
		assert!(kinds.contains(&"analyzer"));
		assert!(kinds.contains(&"user"));
	}

	#[test]
	fn catalog_diff_treats_if_not_exists_and_overwrite_as_different() {
		// Changing from IF NOT EXISTS to OVERWRITE (or vice versa) should register
		// as a modification so the updated statement is applied on next sync.
		let ine_hash = sha256_hex("DEFINE TABLE IF NOT EXISTS person SCHEMAFULL".as_bytes());
		let ow_hash = sha256_hex("DEFINE TABLE OVERWRITE person SCHEMAFULL".as_bytes());

		let old = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: EntityKind::Table,
				scope: None,
				name: "person".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: ine_hash,
				file_hash: "f1".to_string(),
			}],
			operations: Vec::new(),
		};
		let new = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: EntityKind::Table,
				scope: None,
				name: "person".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: ow_hash,
				file_hash: "f2".to_string(),
			}],
			operations: Vec::new(),
		};

		let diff = diff_catalog(&old, &new);
		assert_eq!(diff.modified.len(), 1, "modifier change should be a modification");
	}
}
