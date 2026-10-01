use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::str::FromStr;
use std::{fmt, fs};

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

/// Identifies one SurrealDB object managed by SurrealKit. The `(kind, scope, name)`
/// triple is the object's identity for diffing and for the `__entity` metadata key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityKey {
	/// What kind of object this is.
	pub kind: EntityKind,
	/// The parent scope: the table name for `field`/`event`/`index`, the level
	/// (`DATABASE`/`NAMESPACE`) for `access`/`user`, and `None` for top-level
	/// objects like `table`/`function`/`param`.
	pub scope: Option<String>,
	/// The object's name.
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

/// Create the on-disk directories a module needs (`schema/`, `rollouts/`,
/// `snapshots/`).
pub fn ensure_local_state_dirs_for(layout: &crate::constants::Layout) -> Result<()> {
	for dir in [layout.schema_dir(), layout.rollouts_dir(), layout.state_dir()] {
		fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
	}
	Ok(())
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
	collect_schema_files_at(folder, &schema_dir(folder))
}

/// Collect `.surql` files from an explicit directory, recursively and in sorted
/// order. Used by module-aware callers, which resolve the directory through
/// [`Layout`](crate::constants::Layout) rather than the project folder.
///
/// `root` is the project folder. Tracking keys are relative to it, so the same
/// file yields the same key no matter where the process runs from.
pub fn collect_schema_files_at(root: &str, sd: &std::path::Path) -> Result<Vec<SchemaFile>> {
	if !sd.exists() {
		return Ok(Vec::new());
	}

	let mut files = Vec::new();
	for entry in WalkDir::new(sd).follow_links(true) {
		let entry = entry.with_context(|| format!("walking schema directory {}", sd.display()))?;
		if entry.file_type().is_file()
			&& entry.path().extension().and_then(|suffix| suffix.to_str()) == Some("surql")
		{
			files.push(entry.into_path());
		}
	}

	files.sort();

	let mut out = Vec::with_capacity(files.len());
	for path in files {
		let sql = fs::read_to_string(&path).with_context(|| format!("reading {:?}", path))?;
		let hash = sha256_hex(sql.as_bytes());
		let path_str = folder_relative_key(root, &path)?;
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

/// Map a path a manifest recorded onto the canonical key it should have been
/// written as, together with the prefix it carried.
///
/// `/database/schema/a.surql` against `schema/a.surql` yields
/// `("schema/a.surql", "/database")`. A path already canonical yields an empty
/// prefix, and so does one rooted at `/`, which is a real prefix and not an
/// absent one.
///
/// Longest match wins, matching [`canonicalise_keys`]: with modules, both
/// `schema/a.surql` and `modules/billing/schema/a.surql` are suffixes of the same
/// legacy key, and taking the first would bind it to the wrong module and apply
/// the wrong file.
pub fn canonicalise_recorded_path(
	recorded: &str,
	canonical: &[String],
) -> Option<(String, String)> {
	let key = canonical
		.iter()
		.filter(|candidate| is_legacy_key_for(recorded, candidate))
		.max_by_key(|candidate| candidate.len())?;
	let prefix = recorded.strip_suffix(key.as_str())?.trim_end_matches('/').to_string();
	Some((key.clone(), prefix))
}

/// The hashes a pre-1.0.0-beta.2 manifest could carry for this same content.
///
/// `hash_schema_snapshot` includes each file's path, and paths used to be
/// working-directory-relative, so a manifest planned in one environment hashes
/// differently from the same schema read in another.
///
/// `legacy_prefixes` are recovered from the paths the manifest itself recorded,
/// which is the only reliable source: the spelling belongs to whichever machine
/// ran `plan`, so a manifest planned in a container carries `/database` while the
/// folder here is somewhere else entirely. The configured folder is tried too,
/// for a manifest that recorded no paths to recover from.
///
/// Pure by design. This sits on the path that reports a hash mismatch, and an
/// unreadable file here would replace that actionable error with an I/O one.
#[doc(hidden)]
pub fn legacy_schema_hashes(
	snapshot: &SchemaSnapshot,
	folder: &str,
	legacy_prefixes: &[String],
) -> Result<Vec<String>> {
	let trimmed = folder.trim_end_matches('/');
	let bare = trimmed.strip_prefix("./").unwrap_or(trimmed);
	let basename = Path::new(bare).file_name().and_then(|n| n.to_str()).unwrap_or(bare);

	let mut prefixes: Vec<&str> = Vec::new();
	for prefix in legacy_prefixes.iter().map(String::as_str).chain([bare, trimmed, basename]) {
		if !prefixes.contains(&prefix) {
			prefixes.push(prefix);
		}
	}

	let mut out = Vec::new();
	for prefix in prefixes {
		let mut legacy = SchemaSnapshot {
			version: snapshot.version,
			files: snapshot
				.files
				.iter()
				.map(|f| SchemaSnapshotEntry {
					// An empty prefix is the filesystem root, not an absent one.
					path: format!("{prefix}/{}", f.path),
					hash: f.hash.clone(),
				})
				.collect(),
		};
		legacy.files.sort();
		let hash = hash_schema_snapshot(&legacy)?;
		if !out.contains(&hash) {
			out.push(hash);
		}
	}
	Ok(out)
}

/// Verify a manifest's recorded schema hash against the current files.
///
/// Accepts the canonical hash, or a pre-1.0.0-beta.2 path-dependent one with a
/// warning naming the manifest. `legacy_prefixes` come from
/// [`canonicalise_recorded_path`] over the paths the manifest recorded.
#[doc(hidden)]
pub fn verify_schema_hash(
	snapshot: &SchemaSnapshot,
	folder: &str,
	recorded: &str,
	rollout_id: &str,
	legacy_prefixes: &[String],
) -> Result<()> {
	let current = hash_schema_snapshot(snapshot)?;
	if current == recorded {
		return Ok(());
	}
	if legacy_schema_hashes(snapshot, folder, legacy_prefixes)?.iter().any(|h| h == recorded) {
		log::warn!(
			"rollout '{rollout_id}' carries a pre-1.0.0-beta.2 schema hash, which depended on \
			 the working directory. Accepting it for now -- re-run `surrealkit rollout plan` to \
			 regenerate the manifest. This fallback is removed in 1.1.0."
		);
		return Ok(());
	}
	bail!("target schema hash mismatch for '{rollout_id}': manifest={recorded}, current={current}");
}

pub fn load_schema_snapshot(folder: &str) -> Result<SchemaSnapshot> {
	let mut snapshot: SchemaSnapshot = load_json_or_default(
		schema_snapshot_path(folder),
		SchemaSnapshot {
			version: 1,
			files: Vec::new(),
		},
	)?;
	// Committed snapshots written before 1.0.0-beta.2 carry working-directory
	// paths, which differ between a developer's checkout and CI. Normalise on load
	// so the diff is not polluted by the spelling; `plan` and `baseline` write the
	// canonical form back, so the file itself is rewritten the next time either
	// runs.
	for entry in &mut snapshot.files {
		entry.path = strip_folder_prefix(folder, &entry.path);
	}
	snapshot.files.sort();
	Ok(snapshot)
}

/// Drop a legacy folder prefix from a stored key, leaving it folder-relative.
///
/// Handles the spellings a pre-1.0.0-beta.2 build could produce:
/// `database/schema/x`, `./database/schema/x` and `/database/schema/x`.
///
/// Deliberately conservative. It only strips a prefix that matches the configured
/// folder, because it is applied to every snapshot entry on load and an
/// over-eager match mangles keys that are already correct: a project whose folder
/// is named `schema` would see `schema/a.surql` reduced to `a.surql`, and a
/// basename search anywhere in the path would fire on an interior `schema/database/`
/// directory. Both leave `run_plan` diffing a mangled old snapshot against a
/// canonical new one, so every file reads as removed-plus-added.
pub fn strip_folder_prefix(folder: &str, stored: &str) -> String {
	let trimmed = folder.trim_end_matches('/');
	let bare = trimmed.strip_prefix("./").unwrap_or(trimmed);
	// A leading `./` is noise on either side and never part of a key.
	let candidate = stored.strip_prefix("./").unwrap_or(stored);

	// The folder and the stored key can disagree about absoluteness: a container
	// with `SURREALDB_FOLDER=/database` writes `/database/...`, a checkout
	// configured with `database` writes `database/...`, and the snapshot file is
	// committed and read by both. Try the same folder spelled either way.
	// Searching for the folder's name anywhere in the path is a different thing
	// and is not safe, so it is not done.
	let relative = bare.trim_start_matches('/');
	let absolute = format!("/{relative}");

	for prefix in [trimmed, bare, relative, absolute.as_str()] {
		if prefix.is_empty() || prefix == "/" {
			continue;
		}
		let Some(rest) = candidate.strip_prefix(prefix).and_then(|r| r.strip_prefix('/')) else {
			continue;
		};
		// Every canonical key has a directory component: `schema/x.surql`,
		// `seed/x.surql`, `modules/<name>/schema/x.surql`. A strip that leaves a
		// bare filename has eaten one, which is what happens when the folder shares
		// its name with the key's first segment (a project folder called `schema`).
		if rest.contains('/') {
			return rest.to_string();
		}
	}
	stored.to_string()
}

pub fn save_schema_snapshot(folder: &str, snapshot: &SchemaSnapshot) -> Result<()> {
	save_json_pretty(schema_snapshot_path(folder), snapshot)
}

pub fn load_catalog_snapshot(folder: &str) -> Result<CatalogSnapshot> {
	let mut snapshot: CatalogSnapshot = load_json_or_default(
		catalog_snapshot_path(folder),
		CatalogSnapshot {
			version: 2,
			entities: Vec::new(),
			operations: Vec::new(),
		},
	)?;
	// `source_path` is informational for the diff (entities key on kind/scope/name),
	// but it is written straight back out, so normalising here keeps the committed
	// file from churning between a checkout and a container.
	for entity in &mut snapshot.entities {
		entity.source_path = strip_folder_prefix(folder, &entity.source_path);
	}
	for op in &mut snapshot.operations {
		op.source_path = strip_folder_prefix(folder, &op.source_path);
	}
	Ok(snapshot)
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
	let src = file.sql.as_str();
	let statements = crate::surql_scan::scan(src)
		.map_err(|err| anyhow!("schema file '{}': {err}", file.path))?;
	let mut entities = Vec::new();
	let mut operations = Vec::new();
	for stmt in &statements {
		let stripped = stmt.stripped(src);
		let normalized = stripped.trim();
		if stmt.is_word(src, 0, "REMOVE") {
			bail!(
				"schema file '{}' contains a REMOVE statement; destructive SQL must live in rollout steps",
				file.path
			);
		}
		if stmt.is_word(src, 0, "LET") {
			continue;
		}
		if !stmt.is_word(src, 0, "DEFINE") {
			if allow_all_statements {
				operations.push(Operation {
					sql: normalized.to_string(),
					source_path: file.path.clone(),
				});
				continue;
			}
			bail!(
				"schema file '{}' contains a non-DEFINE statement at line {}: '{}'",
				file.path,
				stmt.line,
				truncate_stmt(normalized)
			);
		}
		if stmt.is_word(src, 1, "NAMESPACE") || stmt.is_word(src, 1, "DATABASE") {
			bail!(
				"schema file '{}' contains DEFINE NAMESPACE/DATABASE, which surrealkit does not manage: \
sync runs inside an already-selected namespace/database. Provision these out-of-band.",
				file.path
			);
		}
		let Some(mut entity) = parse_define_entity(stmt, src) else {
			bail!(
				"schema file '{}' contains an unsupported DEFINE statement at line {}: '{}'",
				file.path,
				stmt.line,
				truncate_stmt(normalized)
			);
		};
		entity.source_path.clone_from(&file.path);
		entity.file_hash.clone_from(&file.hash);
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

/// The stable tracking key for a file inside the project folder: its path
/// relative to `root`, forward-slashed.
///
/// Through 1.0.0-beta.1 this stripped the process **working directory** instead,
/// so the same file was keyed `database/schema/a.surql` from a repo root but
/// `/database/schema/a.surql` inside a container whose `WORKDIR` was not `/`.
/// Every consumer of these keys — the `__entity` sync hashes, `__seed`, the
/// committed snapshots, and a manifest's `target_schema_hash` — then disagreed
/// across environments. Keying on `root` removes the working directory from the
/// equation entirely.
pub fn folder_relative_key(root: &str, path: &Path) -> Result<String> {
	let root_path = Path::new(root);
	if let Ok(rel) = path.strip_prefix(root_path) {
		return Ok(slashed(rel));
	}
	// The walked path and the configured root can be spelled differently (`./db`
	// vs `db`, relative vs absolute). Canonicalising both settles it; if either
	// side cannot be canonicalised, fall back to the path as given.
	if let (Ok(abs_root), Ok(abs_path)) = (root_path.canonicalize(), path.canonicalize())
		&& let Ok(rel) = abs_path.strip_prefix(&abs_root)
	{
		return Ok(slashed(rel));
	}
	Ok(slashed(path))
}

fn slashed(path: &Path) -> String {
	path.to_string_lossy().replace('\\', "/")
}

/// Whether `stored` is a pre-1.0.0-beta.2 spelling of the folder-relative key
/// `canonical`.
///
/// Legacy keys were the canonical key behind some prefix — `database/`,
/// `./database/`, `/database/`, or any working-directory-relative path. Matching
/// on a trailing path-segment boundary covers all of them without the code
/// needing to know the historical folder or working directory.
pub fn is_legacy_key_for(stored: &str, canonical: &str) -> bool {
	if stored == canonical {
		return true;
	}
	stored.strip_suffix(canonical).is_some_and(|prefix| prefix.ends_with('/'))
}

/// Rewrite a map of stored keys onto canonical ones, reporting what moved.
///
/// Returns `(canonicalised, re_keyed)` where `re_keyed` pairs each legacy key
/// with the canonical key it matched, so the caller can migrate the store and
/// say how many rows it touched.
pub fn canonicalise_keys<V: Clone>(
	stored: &BTreeMap<String, V>,
	canonical: &[String],
) -> (BTreeMap<String, V>, Vec<(String, String)>) {
	let mut out = BTreeMap::new();
	let mut re_keyed = Vec::new();

	// Exact matches first, and they win. A database can hold both a canonical row
	// and a legacy one for the same file, which is the mixed CLI/embedded state
	// this migration exists for, and the canonical row carries the current hash.
	// Resolving that by iteration order would let a stale legacy hash overwrite it
	// whenever the folder name happened to sort after the canonical key.
	for (key, value) in stored {
		if canonical.iter().any(|c| c == key) {
			out.insert(key.clone(), value.clone());
		}
	}

	for (key, value) in stored {
		if out.contains_key(key) {
			continue;
		}
		// Longest match wins. With modules, `modules/billing/schema/a.surql` and
		// `schema/a.surql` are both suffixes of a legacy absolute key, and picking
		// whichever came first would bind it to the wrong module.
		match canonical.iter().filter(|c| is_legacy_key_for(key, c)).max_by_key(|c| c.len()) {
			// Already carried by a canonical row: retire the legacy key without
			// touching the live hash.
			Some(target) if out.contains_key(target) => {
				re_keyed.push((key.clone(), target.clone()));
			}
			Some(target) => {
				re_keyed.push((key.clone(), target.clone()));
				out.insert(target.clone(), value.clone());
			}
			// No match: a genuinely removed file. Keep it so prune still sees it.
			None => {
				out.insert(key.clone(), value.clone());
			}
		}
	}
	(out, re_keyed)
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

/// SurrealQL ready to apply, from [`prepare_schema_sql`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct PreparedSql {
	/// The statements, each terminated with `;` on its own line.
	pub sql: String,
	/// How many statements `sql` holds.
	pub statements: usize,
	/// Things the caller should tell the user about, with the file or step they
	/// came from.
	pub warnings: Vec<PrepareWarning>,
}

/// Something worth telling the user about a statement [`prepare_schema_sql`]
/// prepared.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PrepareWarning {
	/// `DEFINE SEQUENCE ... OVERWRITE` resets the sequence to its `START`, so every
	/// apply re-issues values that are already in use.
	SequenceOverwrite {
		name: String,
		line: u32,
	},
	/// A record access method with no explicit JWT key gets a new random signing
	/// key from every `OVERWRITE`, which invalidates every token it has issued.
	KeylessRecordAccess {
		name: String,
		line: u32,
	},
}

impl fmt::Display for PrepareWarning {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::SequenceOverwrite {
				name,
				line,
			} => write!(
				f,
				"line {line}: sequence `{name}` is defined with OVERWRITE, which resets it to its \
				 START every time it is applied (immediately on SurrealDB 3.3, after a restart or \
				 its cached batch on 3.2) and re-issues values already in use. Drop OVERWRITE to \
				 let SurrealKit apply it with IF NOT EXISTS"
			),
			Self::KeylessRecordAccess {
				name,
				line,
			} => write!(
				f,
				"line {line}: record access `{name}` has no JWT key, so SurrealDB generates a new \
				 random one each time it is applied with OVERWRITE, and every session token it \
				 issued stops working. Give it a stable key, e.g. \
				 `WITH JWT ALGORITHM HS512 KEY ${{JWT_SECRET}}`"
			),
		}
	}
}

/// Make a schema file's `DEFINE` statements safe to apply again.
///
/// Sync and rollouts re-apply whole files, so each `DEFINE` needs a modifier
/// that tolerates the object already existing:
///
/// - Most kinds get `OVERWRITE`, and an explicit `IF NOT EXISTS` becomes
///   `OVERWRITE` too, so a changed definition is actually applied.
/// - `DEFINE SEQUENCE` is the exception. A sequence's definition carries state:
///   `OVERWRITE` resets its counter to `START`, and the ids it hands out next
///   collide with records that already exist. A plain `DEFINE SEQUENCE` gets
///   `IF NOT EXISTS`, an explicit `IF NOT EXISTS` is kept, and an explicit
///   `OVERWRITE` is kept as written with a [`PrepareWarning::SequenceOverwrite`].
///
/// The modifier is spliced in by position, so everything else, comments and
/// regex literals included, reaches the server exactly as written. Non-`DEFINE`
/// statements pass through unchanged.
pub fn prepare_schema_sql(sql: &str) -> Result<PreparedSql> {
	let statements = crate::surql_scan::scan(sql).map_err(|err| anyhow!("{err}"))?;
	let mut out = String::with_capacity(sql.len() + statements.len() * 16);
	let mut warnings = Vec::new();
	for stmt in &statements {
		let text = stmt.text(sql);
		let Some(head) = define_head(stmt, sql) else {
			out.push_str(text);
			out.push_str(";\n");
			continue;
		};
		let kind = stmt.tok_text(sql, head.kind_index).unwrap_or_default();
		let name = || ident_at(stmt, sql, head.name_index).unwrap_or_default();
		let is_sequence = kind.eq_ignore_ascii_case("SEQUENCE");
		let replacement = match (is_sequence, head.modifier) {
			(true, DefineModifier::None) => Some(" IF NOT EXISTS"),
			(true, DefineModifier::IfNotExists) => None,
			(true, DefineModifier::Overwrite) => {
				warnings.push(PrepareWarning::SequenceOverwrite {
					name: name(),
					line: stmt.line,
				});
				None
			}
			(false, DefineModifier::None) => Some(" OVERWRITE"),
			(false, DefineModifier::IfNotExists) => Some("OVERWRITE"),
			(false, DefineModifier::Overwrite) => None,
		};
		// Every kind but a sequence is applied with OVERWRITE.
		if !is_sequence
			&& kind.eq_ignore_ascii_case("ACCESS")
			&& is_keyless_record_access(stmt, sql)
		{
			warnings.push(PrepareWarning::KeylessRecordAccess {
				name: name(),
				line: stmt.line,
			});
		}
		match replacement {
			Some(replacement) => {
				let span = &head.modifier_span;
				out.push_str(&sql[stmt.span.start..span.start]);
				out.push_str(replacement);
				out.push_str(&sql[span.end..stmt.span.end]);
			}
			None => out.push_str(text),
		}
		out.push_str(";\n");
	}
	Ok(PreparedSql {
		sql: out,
		statements: statements.len(),
		warnings,
	})
}

/// Whether a `DEFINE ACCESS` is a record (or bearer-for-record) access method
/// with no `WITH JWT` clause, which leaves SurrealDB to pick a random key.
fn is_keyless_record_access(stmt: &crate::surql_scan::Stmt, src: &str) -> bool {
	let n = stmt.tokens.len();
	let record = (0..n).any(|i| {
		(stmt.is_word(src, i, "TYPE") || stmt.is_word(src, i, "FOR"))
			&& stmt.is_word(src, i + 1, "RECORD")
	});
	let explicit_jwt =
		(0..n).any(|i| stmt.is_word(src, i, "WITH") && stmt.is_word(src, i + 1, "JWT"));
	record && !explicit_jwt
}

/// The names of sequences these files define with an explicit `OVERWRITE`, the
/// only form [`prepare_schema_sql`] lets reset a counter.
pub(crate) fn overwrite_sequences(files: &[SchemaFile]) -> BTreeSet<String> {
	let mut out = BTreeSet::new();
	for file in files {
		let Ok(statements) = crate::surql_scan::scan(&file.sql) else {
			continue;
		};
		for stmt in &statements {
			if let Some(head) = define_head(stmt, &file.sql)
				&& stmt.is_word(&file.sql, head.kind_index, "SEQUENCE")
				&& head.modifier == DefineModifier::Overwrite
				&& let Some(name) = ident_at(stmt, &file.sql, head.name_index)
			{
				out.insert(name);
			}
		}
	}
	out
}

/// Ensures every `DEFINE` statement can be re-applied. See [`prepare_schema_sql`],
/// which this wraps: it logs the warnings, and on SurrealQL it cannot read it
/// logs that too and returns the input unchanged, so the server reports the
/// problem.
#[deprecated(note = "use prepare_schema_sql, which returns its warnings and errors")]
pub fn ensure_overwrite(sql: &str) -> String {
	match prepare_schema_sql(sql) {
		Ok(prepared) => {
			for warning in &prepared.warnings {
				log::warn!("{warning}");
			}
			prepared.sql
		}
		Err(err) => {
			log::warn!("could not prepare SQL for re-apply, sending it unchanged: {err:#}");
			sql.to_string()
		}
	}
}

/// The `DEFINE` modifier a statement carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefineModifier {
	None,
	Overwrite,
	IfNotExists,
}

/// Where a `DEFINE` statement's modifier sits: what it is, the token index of
/// the name that follows it, and the byte range it occupies (empty, right after
/// the kind keyword, when there is none).
pub(crate) struct DefineHead {
	pub kind_index: usize,
	pub modifier: DefineModifier,
	pub modifier_span: std::ops::Range<usize>,
	pub name_index: usize,
}

/// Locate the kind, modifier and name of a `DEFINE <kind>` statement.
pub(crate) fn define_head(stmt: &crate::surql_scan::Stmt, src: &str) -> Option<DefineHead> {
	use crate::surql_scan::TokKind;
	if !stmt.is_word(src, 0, "DEFINE") || stmt.tokens.get(1)?.kind != TokKind::Word {
		return None;
	}
	let kind_end = stmt.tokens[1].span.end;
	let (modifier, modifier_span, name_index) = if stmt.is_word(src, 2, "OVERWRITE") {
		(DefineModifier::Overwrite, stmt.tokens[2].span.clone(), 3)
	} else if stmt.is_word(src, 2, "IF")
		&& stmt.is_word(src, 3, "NOT")
		&& stmt.is_word(src, 4, "EXISTS")
	{
		(DefineModifier::IfNotExists, stmt.tokens[2].span.start..stmt.tokens[4].span.end, 5)
	} else {
		(DefineModifier::None, kind_end..kind_end, 2)
	};
	Some(DefineHead {
		kind_index: 1,
		modifier,
		modifier_span,
		name_index,
	})
}

fn parse_define_entity(stmt: &crate::surql_scan::Stmt, src: &str) -> Option<CatalogEntity> {
	let head = define_head(stmt, src)?;
	let kind = EntityKind::from_str(stmt.tok_text(src, head.kind_index)?).ok()?;
	let idx = head.name_index;
	if idx >= stmt.tokens.len() {
		return None;
	}

	let (scope, name) = match &kind {
		EntityKind::Table => (None, ident_at(stmt, src, idx)?),
		EntityKind::Field | EntityKind::Event | EntityKind::Index => {
			let name = ident_at(stmt, src, idx)?;
			let on_idx = find_word(stmt, src, idx + 1, "ON")?;
			let mut scope_idx = on_idx + 1;
			if stmt.is_word(src, scope_idx, "TABLE") {
				scope_idx += 1;
			}
			(Some(ident_at(stmt, src, scope_idx)?), name)
		}
		EntityKind::Function
		| EntityKind::Param
		| EntityKind::Analyzer
		| EntityKind::Api
		| EntityKind::Bucket
		| EntityKind::Model
		| EntityKind::Sequence
		| EntityKind::Config
		| EntityKind::Module => (None, ident_at(stmt, src, idx)?),
		EntityKind::Access | EntityKind::User => {
			let name = ident_at(stmt, src, idx)?;
			let scope = find_word(stmt, src, idx + 1, "ON")
				.and_then(|on_idx| ident_at(stmt, src, on_idx + 1));
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

/// The identifier starting at token `idx`: that token plus every token written
/// directly against it, so `a.b[*]`, `fn::greet` and `ml::model<1.0.0>` come back
/// whole. It stops at whitespace and at `( , ; { } )`, which is where the old
/// whitespace split and its trimming used to cut a name.
fn ident_at(stmt: &crate::surql_scan::Stmt, src: &str, idx: usize) -> Option<String> {
	let is_stop =
		|i: usize| matches!(stmt.tok_text(src, i), Some("(" | ")" | "," | ";" | "{" | "}"));
	let first = stmt.tokens.get(idx)?;
	if is_stop(idx) {
		return None;
	}
	let mut end = first.span.end;
	let mut next = idx + 1;
	while let Some(tok) = stmt.tokens.get(next) {
		if tok.span.start != end || is_stop(next) {
			break;
		}
		end = tok.span.end;
		next += 1;
	}
	Some(src[first.span.start..end].to_string())
}

fn find_word(stmt: &crate::surql_scan::Stmt, src: &str, start: usize, word: &str) -> Option<usize> {
	(start..stmt.tokens.len()).find(|&i| stmt.is_word(src, i, word))
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
	match stmt.char_indices().nth(LIMIT) {
		// Cut on a character boundary: slicing at a byte offset panicked when a
		// multi-byte character such as `⟨` straddled it.
		Some((cut, _)) => format!("{}...", &stmt[..cut]),
		None => stmt.to_string(),
	}
}

/// The comment stripper and splitter this module used before `surql_scan`, kept
/// so tests can prove the new pipeline produces the same entity keys and hashes
/// wherever the old one read a file correctly.
#[cfg(test)]
pub(crate) mod legacy {
	pub(crate) fn strip_comments(sql: &str) -> String {
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

	pub(crate) fn split_statements(sql: &str) -> Vec<String> {
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
				'}' if !in_single && !in_double && !in_backtick && brace_depth > 0 => {
					brace_depth -= 1
				}
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

	use std::str::FromStr;

	use super::{CatalogEntity, EntityKind, normalize_statement};
	use crate::core::sha256_hex;

	/// The entities the old pipeline extracted from `sql`, with their statement
	/// hashes, skipping anything it could not read (which the new pipeline is
	/// allowed to read differently).
	pub(crate) fn entities(sql: &str) -> Vec<CatalogEntity> {
		let mut out = Vec::new();
		for stmt in split_statements(&strip_comments(sql)) {
			let normalized = stmt.trim();
			if !normalized.to_ascii_uppercase().starts_with("DEFINE ") {
				continue;
			}
			if let Some(mut entity) = parse_define_entity(normalized) {
				entity.statement_hash = sha256_hex(normalize_statement(normalized).as_bytes());
				out.push(entity);
			}
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
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use test_case::test_case;

	use super::*;

	fn prep(sql: &str) -> String {
		prepare_schema_sql(sql).expect("prepare").sql
	}

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
		assert!(
			catalog
				.entities
				.iter()
				.any(|entity| entity.kind == EntityKind::Api && entity.name == "v1")
		);
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
			catalog.entities.iter().any(|e| {
				e.kind == EntityKind::Module && e.scope.is_none() && e.name == "mod::math"
			}),
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
		let plain = prep("DEFINE MODULE mod::math AS f\"math:/math.surli\";");
		assert_eq!(plain.trim(), "DEFINE MODULE OVERWRITE mod::math AS f\"math:/math.surli\";");

		let if_not_exists = prep("DEFINE MODULE IF NOT EXISTS mod::math AS f\"math:/math.surli\";");
		assert_eq!(
			if_not_exists.trim(),
			"DEFINE MODULE OVERWRITE mod::math AS f\"math:/math.surli\";"
		);

		// SurrealDB 3.3 spells it `FROM ... UNSIGNED`; the modifier still goes
		// straight after MODULE.
		let v33 = prep("DEFINE MODULE mod::math FROM f\"math:/math.surli\" UNSIGNED;");
		assert_eq!(
			v33.trim(),
			"DEFINE MODULE OVERWRITE mod::math FROM f\"math:/math.surli\" UNSIGNED;"
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
		let result = prep(sql);
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
			.unwrap_or_else(|e| panic!("allow_all_statements should not fail for {stmt}: {e:#}"));
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
		let result = prep(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE post SCHEMAFULL;"));
		assert!(result.contains("DEFINE FIELD OVERWRITE name ON post TYPE string;"));
	}

	#[test]
	fn ensure_overwrite_preserves_existing() {
		let sql = "DEFINE TABLE OVERWRITE post SCHEMAFULL;";
		let result = prep(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE post SCHEMAFULL;"));
		// Should not double up OVERWRITE
		assert!(!result.contains("OVERWRITE OVERWRITE"));
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_with_overwrite() {
		// IF NOT EXISTS prevents schema changes from being applied in sync;
		// ensure_overwrite must replace it with OVERWRITE so updates are not silently skipped.
		let sql = "DEFINE TABLE IF NOT EXISTS post SCHEMAFULL;";
		let result = prep(sql);
		assert!(result.contains("DEFINE TABLE OVERWRITE post SCHEMAFULL;"), "got: {result}");
		assert!(!result.contains("IF NOT EXISTS"), "IF NOT EXISTS should be replaced: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_field() {
		let sql = "DEFINE FIELD IF NOT EXISTS email ON person TYPE string;";
		let result = prep(sql);
		assert!(
			result.contains("DEFINE FIELD OVERWRITE email ON person TYPE string;"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_event() {
		let sql = "DEFINE EVENT IF NOT EXISTS changed ON person WHEN true THEN ();";
		let result = prep(sql);
		assert!(
			result.contains("DEFINE EVENT OVERWRITE changed ON person WHEN true THEN ();"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_index() {
		let sql = "DEFINE INDEX IF NOT EXISTS by_email ON TABLE person FIELDS email;";
		let result = prep(sql);
		assert!(
			result.contains("DEFINE INDEX OVERWRITE by_email ON TABLE person FIELDS email;"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_function() {
		let sql = "DEFINE FUNCTION IF NOT EXISTS fn::greet($name: string) { RETURN $name; };";
		let result = prep(sql);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
		assert!(result.contains("DEFINE FUNCTION OVERWRITE"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_param() {
		let sql = "DEFINE PARAM IF NOT EXISTS $env VALUE 'dev';";
		let result = prep(sql);
		assert!(result.contains("DEFINE PARAM OVERWRITE $env VALUE 'dev';"), "got: {result}");
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_analyzer() {
		let sql = "DEFINE ANALYZER IF NOT EXISTS english TOKENIZERS blank, class;";
		let result = prep(sql);
		assert!(
			result.contains("DEFINE ANALYZER OVERWRITE english TOKENIZERS blank, class;"),
			"got: {result}"
		);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_access() {
		let sql = "DEFINE ACCESS IF NOT EXISTS admin ON DATABASE TYPE RECORD;";
		let result = prep(sql);
		assert!(!result.contains("IF NOT EXISTS"), "got: {result}");
		assert!(result.contains("DEFINE ACCESS OVERWRITE"), "got: {result}");
	}

	#[test]
	fn ensure_overwrite_replaces_if_not_exists_user() {
		let sql = "DEFINE USER IF NOT EXISTS app ON DATABASE PASSHASH 'x';";
		let result = prep(sql);
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

	// --- Folder-relative tracking keys (BUG: cwd-relative keys diverged between a
	// local checkout and a container, re-keying every tracked file).

	#[test]
	fn keys_are_relative_to_the_folder_not_the_working_directory() {
		for folder in ["./database", "database", "/srv/database"] {
			let path = PathBuf::from(folder).join("schema").join("user.surql");
			assert_eq!(
				folder_relative_key(folder, &path).expect("key"),
				"schema/user.surql",
				"folder {folder:?} produced a non-canonical key"
			);
		}
	}

	#[test]
	fn module_keys_keep_their_module_path() {
		let path = PathBuf::from("database/modules/billing/schema/001.surql");
		assert_eq!(
			folder_relative_key("database", &path).expect("key"),
			"modules/billing/schema/001.surql"
		);
	}

	#[test]
	fn every_legacy_spelling_maps_onto_one_canonical_key() {
		let canonical = "schema/user.surql";
		for stored in [
			"schema/user.surql",
			"database/schema/user.surql",
			"./database/schema/user.surql",
			"/database/schema/user.surql",
			"/home/runner/work/app/database/schema/user.surql",
		] {
			assert!(is_legacy_key_for(stored, canonical), "{stored:?} should match {canonical:?}");
		}
	}

	#[test]
	fn a_genuinely_different_file_is_not_treated_as_a_legacy_key() {
		assert!(!is_legacy_key_for("schema/other.surql", "schema/user.surql"));
		// A suffix that is not on a path-segment boundary must not match, or
		// `admin_user.surql` would be mistaken for `user.surql`.
		assert!(!is_legacy_key_for("schema/admin_user.surql", "schema/user.surql"));
	}

	#[test]
	fn canonicalise_keys_migrates_legacy_and_keeps_genuinely_removed_files() {
		let mut stored = BTreeMap::new();
		stored.insert("/database/schema/user.surql".to_string(), "hash-a".to_string());
		stored.insert("database/schema/post.surql".to_string(), "hash-b".to_string());
		stored.insert("schema/deleted.surql".to_string(), "hash-c".to_string());

		let canonical = ["schema/user.surql".to_string(), "schema/post.surql".to_string()];
		let (migrated, re_keyed) = canonicalise_keys(&stored, &canonical);

		assert_eq!(migrated.get("schema/user.surql"), Some(&"hash-a".to_string()));
		assert_eq!(migrated.get("schema/post.surql"), Some(&"hash-b".to_string()));
		// A file that really was deleted must survive as a stale key so prune sees it.
		assert_eq!(migrated.get("schema/deleted.surql"), Some(&"hash-c".to_string()));
		assert_eq!(re_keyed.len(), 2, "both legacy spellings should be re-keyed");
	}

	/// Both rows for one file, with a folder name that sorts after the canonical
	/// key. The canonical row holds the current hash and must survive.
	#[test]
	fn canonicalise_keys_prefers_the_canonical_row_over_a_legacy_one() {
		let mut stored = BTreeMap::new();
		stored.insert("schema/a.surql".to_string(), "current".to_string());
		stored.insert("zzz/schema/a.surql".to_string(), "stale".to_string());

		let canonical = ["schema/a.surql".to_string()];
		let (migrated, re_keyed) = canonicalise_keys(&stored, &canonical);

		assert_eq!(
			migrated.get("schema/a.surql"),
			Some(&"current".to_string()),
			"a stale legacy hash must not overwrite the canonical row"
		);
		assert_eq!(migrated.len(), 1, "the legacy row must not survive as a separate key");
		assert_eq!(re_keyed.len(), 1, "the legacy row still needs retiring");
	}

	#[test]
	fn strip_folder_prefix_handles_every_folder_spelling() {
		for folder in ["database", "./database", "/database"] {
			for stored in
				["database/schema/a.surql", "./database/schema/a.surql", "/database/schema/a.surql"]
			{
				assert_eq!(
					strip_folder_prefix(folder, stored),
					"schema/a.surql",
					"folder={folder:?} stored={stored:?}"
				);
			}
		}
		// Already canonical: left alone.
		assert_eq!(strip_folder_prefix("database", "schema/a.surql"), "schema/a.surql");

		// A key that is already canonical must be left alone, even when the folder
		// name collides with its first segment.
		assert_eq!(strip_folder_prefix("schema", "schema/a.surql"), "schema/a.surql");
		// And an interior directory sharing the folder's name is not a prefix.
		assert_eq!(
			strip_folder_prefix("database", "schema/database/tables.surql"),
			"schema/database/tables.surql"
		);
		// An unrecognised prefix is left intact rather than guessed at; the sync
		// migration matches those on a path-segment boundary instead.
		assert_eq!(
			strip_folder_prefix("database", "/srv/app/database/schema/a.surql"),
			"/srv/app/database/schema/a.surql"
		);
	}

	/// The manifest-portability bug: a rollout planned from a repo root could not
	/// be started in a container, because the hash covered the paths.
	#[test]
	fn the_schema_hash_no_longer_depends_on_where_the_command_ran() {
		let files = |root: &str| {
			vec![SchemaFile {
				path: folder_relative_key(root, &PathBuf::from(root).join("schema/a.surql"))
					.expect("key"),
				sql: "DEFINE TABLE a SCHEMAFULL;".to_string(),
				hash: "content-hash".to_string(),
			}]
		};

		let local = hash_schema_snapshot(&snapshot_from_files(&files("./database"))).expect("hash");
		let container =
			hash_schema_snapshot(&snapshot_from_files(&files("/database"))).expect("hash");
		assert_eq!(local, container, "the same schema must hash identically in any environment");
	}

	/// The case a real project hits: the manifest was planned inside a container
	/// with `SURREALDB_FOLDER=/database`, so its keys carry `/database`, and the
	/// developer's folder is somewhere else entirely. No amount of guessing from
	/// the local folder string reaches that prefix, but the manifest records it in
	/// its own `apply_files` paths.
	#[test]
	fn a_manifest_planned_elsewhere_verifies_from_its_recorded_paths() {
		let snapshot = snapshot_from_files(&[SchemaFile {
			path: "schema/014-sku.surql".to_string(),
			sql: "DEFINE TABLE sku SCHEMAFULL;".to_string(),
			hash: "content-hash".to_string(),
		}]);

		// What the container recorded when it planned.
		let legacy = SchemaSnapshot {
			version: snapshot.version,
			files: vec![SchemaSnapshotEntry {
				path: "/database/schema/014-sku.surql".to_string(),
				hash: "content-hash".to_string(),
			}],
		};
		let recorded = hash_schema_snapshot(&legacy).expect("legacy hash");

		// Derived the way callers derive it: from the paths the manifest recorded.
		let canonical = vec!["schema/014-sku.surql".to_string()];
		let (key, prefix) =
			canonicalise_recorded_path("/database/schema/014-sku.surql", &canonical)
				.expect("the recorded path should map onto its canonical key");
		assert_eq!(key, "schema/014-sku.surql");
		assert_eq!(prefix, "/database");
		let hints = vec![prefix];

		// The local folder shares no prefix with the container's.
		verify_schema_hash(
			&snapshot,
			"/home/dev/project/database",
			&recorded,
			"20260911194216__sku_on_hand",
			&hints,
		)
		.expect("a manifest planned in another environment must still verify");

		// Without the recorded paths there is nothing to derive the prefix from.
		assert!(
			verify_schema_hash(
				&snapshot,
				"/home/dev/project/database",
				&recorded,
				"20260911194216__sku_on_hand",
				&[],
			)
			.is_err(),
			"the hint is what makes this work; guard against it becoming a no-op"
		);
	}

	#[test]
	fn a_pre_beta2_manifest_hash_is_accepted_with_a_warning() {
		let snapshot = snapshot_from_files(&[SchemaFile {
			path: "schema/a.surql".to_string(),
			sql: "DEFINE TABLE a SCHEMAFULL;".to_string(),
			hash: "content-hash".to_string(),
		}]);

		let legacy = legacy_schema_hashes(&snapshot, "./database", &[]).expect("legacy hashes");
		assert!(!legacy.is_empty(), "expected at least one legacy spelling");

		// Every legacy spelling the old code could have recorded must still verify.
		for recorded in &legacy {
			verify_schema_hash(&snapshot, "./database", recorded, "20260101000000__demo", &[])
				.expect("a pre-beta2 manifest must still start");
		}

		// A genuinely different schema must still be rejected.
		let err =
			verify_schema_hash(&snapshot, "./database", "deadbeef", "20260101000000__demo", &[])
				.expect_err("a real mismatch must fail");
		assert!(err.to_string().contains("target schema hash mismatch"), "got: {err}");
	}

	// ---- surql_scan migration -------------------------------------------------

	/// Every `.surql` file in the repository, outside build output.
	fn repo_corpus() -> Vec<(String, String)> {
		let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
		let mut out: Vec<(String, String)> = WalkDir::new(&root)
			.into_iter()
			.filter_entry(|entry| {
				let name = entry.file_name().to_string_lossy();
				!matches!(name.as_ref(), "target" | ".claude" | "node_modules" | ".git")
			})
			.filter_map(Result::ok)
			.filter(|entry| entry.path().extension().is_some_and(|ext| ext == "surql"))
			.map(|entry| {
				let path = entry.path().strip_prefix(&root).unwrap().display().to_string();
				(path, fs::read_to_string(entry.path()).unwrap())
			})
			.collect();
		out.sort();
		assert!(out.len() >= 20, "corpus went missing: {} files", out.len());
		out
	}

	/// Statements the old splitter read correctly, taken from this module's tests.
	const LEGACY_VECTORS: &[&str] = &[
		"DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD name ON TABLE person TYPE string;",
		"DEFINE FIELD a.b[*] ON t TYPE string;\nDEFINE FIELD emails.*.address ON user TYPE string;",
		"DEFINE FUNCTION fn::greet($n: string) { RETURN 'hi ' + $n; };\nDEFINE FUNCTION fn::two ($a: int) { RETURN $a; };",
		"DEFINE INDEX by_email ON TABLE person FIELDS email UNIQUE;\nDEFINE EVENT changed ON person WHEN true THEN { CREATE log; };",
		"DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user WHERE email = $email) DURATION FOR TOKEN 1h;",
		"DEFINE USER admin ON ROOT PASSWORD 'secret' ROLES OWNER;\nDEFINE PARAM $env VALUE 'dev';",
		"DEFINE ANALYZER english TOKENIZERS blank, class FILTERS lowercase;\nDEFINE SEQUENCE order_no BATCH 10 START 100;",
		"DEFINE API \"/users/:id\" FOR get THEN { RETURN 1; };\nDEFINE BUCKET files BACKEND \"memory\";",
		"DEFINE MODEL ml::sentiment<1.0.0>;\nDEFINE CONFIG GRAPHQL AUTO;\nDEFINE MODULE mod::math AS f\"math:/math.surli\";",
		"DEFINE TABLE IF NOT EXISTS a;\nDEFINE TABLE OVERWRITE b;\ndefine table c schemaless;",
		"-- c\nDEFINE TABLE a SCHEMAFULL; -- trailing\n// x\nDEFINE FIELD f ON a TYPE string; # hash\n/* block\n */ DEFINE TABLE b;",
		"DEFINE FIELD a ON foo TYPE string DEFAULT 'it\\'s -- still in string';\nDEFINE FIELD b ON foo DEFAULT \"/* not */\";",
		"DEFINE TABLE ${PREFIX}_users;\nDEFINE FIELD x ON ${PREFIX}_users TYPE string;",
		"DEFINE FIELD f ON t TYPE int VALUE $value - 1;\nDEFINE FIELD g ON t TYPE int VALUE 4 / 2;",
	];

	fn keys_and_hashes(entities: &[CatalogEntity]) -> Vec<(EntityKey, String)> {
		entities.iter().map(|e| (e.key(), e.statement_hash.clone())).collect()
	}

	#[test]
	fn entity_keys_and_hashes_match_the_old_pipeline() {
		// A changed key reads as a removed entity and a new one, and sync prunes the
		// "removed" one: REMOVE TABLE. So on everything the old splitter read
		// correctly, the new one must agree byte for byte.
		let mut corpus = repo_corpus();
		corpus.extend(
			LEGACY_VECTORS
				.iter()
				.enumerate()
				.map(|(i, sql)| (format!("vector {i}"), sql.to_string())),
		);
		for (path, sql) in corpus {
			let file = SchemaFile {
				path: path.clone(),
				hash: "h".to_string(),
				sql: sql.clone(),
			};
			let (entities, _) = parse_schema_statements(&file, true)
				.unwrap_or_else(|err| panic!("{path} no longer parses: {err:#}"));
			assert_eq!(
				keys_and_hashes(&entities),
				keys_and_hashes(&legacy::entities(&sql)),
				"entity keys or hashes changed for {path}"
			);
		}
	}

	/// Valid SurrealDB 3.3 schema that the old splitter could not read.
	const TRICKY_SCHEMA: &str = "-- a file that exercises every lexical trap
DEFINE PARAM OVERWRITE $PRE_FILTER VALUE /[ \\-_().,\\\\\\/$&+,:;=?@#|'<>^*%!]/;
DEFINE FUNCTION OVERWRITE fn::preFilter($str: option<string>) {
	RETURN IF $str {
		string::replace($str, $PRE_FILTER, '')
	};
};
DEFINE TABLE person SCHEMAFULL; # it's a comment
DEFINE FIELD name ON person TYPE string ASSERT $value = /^[a-z#';{]+$/;
DEFINE FIELD half ON person TYPE option<number> VALUE 10 / 2;
DEFINE FIELD note ON person TYPE option<string> DEFAULT 'it\\'s -- not a comment # nor this; or this';
DEFINE FIELD tag ON person TYPE option<string> DEFAULT ALWAYS \"a;b\";
DEFINE FUNCTION fn::braces($s: string) {
	LET $x = { a: 1 };
	IF $s = /}/ { RETURN '{'; };
	RETURN $s;
};
DEFINE TABLE `odd;name#` SCHEMALESS;
DEFINE TABLE ⟨other;name⟩ SCHEMALESS;
DEFINE PARAM $rid VALUE person:⟨a;b⟩;
DEFINE PARAM $list VALUE [/a'/, /b#/, { re: /c;/ }];
DEFINE PARAM $re2 VALUE <regex> \"a;b#c\";
DEFINE PARAM $half VALUE 10 / 2;
DEFINE SEQUENCE seq BATCH 1 START 1;
DEFINE INDEX by_name ON person FIELDS name;
DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase;
DEFINE EVENT ev ON person WHEN $event = 'CREATE' THEN {
	LET $re = /x;y'/;
	RETURN $re;
};
DEFINE ACCESS acc ON DATABASE TYPE RECORD
	SIGNIN (SELECT * FROM person WHERE name = $name)
	WITH JWT ALGORITHM HS512 KEY 'secret;#--';
/* a closing comment with a ' quote */
";

	async fn mem_db() -> surrealdb::Surreal<surrealdb::engine::any::Any> {
		use surrealdb::engine::any::connect;
		use surrealdb::opt::Config;
		use surrealdb::opt::capabilities::Capabilities;
		let db =
			connect(("mem://", Config::new().capabilities(Capabilities::all()))).await.unwrap();
		db.use_ns("scan").use_db("scan").await.unwrap();
		db
	}

	async fn schema_info(db: &surrealdb::Surreal<surrealdb::engine::any::Any>) -> String {
		use surrealdb_types::SurrealValue;
		let mut res = db.query("INFO FOR DB").await.unwrap().check().unwrap();
		let info: surrealdb_types::Value = res.take(0).unwrap();
		let info = serde_json::Value::from_value(info).unwrap();
		let mut out = vec![info.to_string()];
		let tables: Vec<String> =
			info["tables"].as_object().map(|t| t.keys().cloned().collect()).unwrap_or_default();
		for table in tables {
			let mut res = db
				.query("INFO FOR TABLE $t")
				.bind(("t", table.clone()))
				.await
				.unwrap()
				.check()
				.unwrap();
			let info: surrealdb_types::Value = res.take(0).unwrap();
			out.push(format!("{table}: {}", serde_json::Value::from_value(info).unwrap()));
		}
		out.join("\n")
	}

	/// SurrealDB is the oracle. For each file: its own statement count matches
	/// ours, applying our statements one at a time defines exactly what applying
	/// the file does, and the prepared form applies twice without error and
	/// defines the same schema again.
	#[tokio::test]
	async fn splitting_and_preparing_agree_with_surrealdb() {
		let mut corpus: Vec<(String, String)> = repo_corpus()
			.into_iter()
			.filter(|(path, _)| {
				path.contains("/schema/")
					|| path.contains("embed_schema")
					|| path.contains("embed_modules")
			})
			.collect();
		corpus.push(("tricky".to_string(), TRICKY_SCHEMA.to_string()));
		corpus.push(("crlf".to_string(), TRICKY_SCHEMA.replace('\n', "\r\n")));
		for (path, sql) in corpus {
			let statements =
				crate::surql_scan::scan(&sql).unwrap_or_else(|err| panic!("{path}: {err}"));

			let whole = mem_db().await;
			let response =
				whole.query(sql.as_str()).await.unwrap_or_else(|err| panic!("{path}: {err}"));
			assert_eq!(
				response.num_statements(),
				statements.len(),
				"{path}: statement count differs from SurrealDB's"
			);
			response.check().unwrap_or_else(|err| panic!("{path}: {err}"));

			let split = mem_db().await;
			for stmt in &statements {
				split.query(stmt.text(&sql)).await.and_then(|r| r.check()).unwrap_or_else(|err| {
					panic!("{path} line {}: {err}\n{}", stmt.line, stmt.text(&sql))
				});
			}

			let prepared = prepare_schema_sql(&sql).unwrap();
			assert_eq!(prepared.statements, statements.len());
			let reapplied = mem_db().await;
			for round in 0..2 {
				reapplied
					.query(prepared.sql.as_str())
					.await
					.and_then(|r| r.check())
					.unwrap_or_else(|err| {
						panic!("{path}: prepared apply {round} failed: {err}\n{}", prepared.sql)
					});
			}

			let expected = schema_info(&whole).await;
			assert_eq!(
				schema_info(&split).await,
				expected,
				"{path}: split statements define a different schema"
			);
			assert_eq!(
				schema_info(&reapplied).await,
				expected,
				"{path}: prepared statements define a different schema"
			);
		}
	}

	#[test]
	fn issue_92_file_parses_into_its_two_entities() {
		let sql = TRICKY_SCHEMA.split("DEFINE TABLE person").next().unwrap();
		let file = SchemaFile {
			path: "schema/utils.surql".to_string(),
			hash: "h".to_string(),
			sql: sql.to_string(),
		};
		let (entities, _) = parse_schema_statements(&file, false).expect("the #92 file must parse");
		let names: Vec<&str> = entities.iter().map(|e| e.name.as_str()).collect();
		assert_eq!(names, vec!["$PRE_FILTER", "fn::preFilter"]);
	}

	#[test]
	fn the_whole_tricky_schema_is_catalogued() {
		let file = SchemaFile {
			path: "schema/tricky.surql".to_string(),
			hash: "h".to_string(),
			sql: TRICKY_SCHEMA.to_string(),
		};
		let (entities, _) = parse_schema_statements(&file, false).unwrap();
		let keys: Vec<String> = entities
			.iter()
			.map(|e| format!("{}:{}:{}", e.kind, e.scope.as_deref().unwrap_or("-"), e.name))
			.collect();
		assert_eq!(
			keys,
			vec![
				"param:-:$PRE_FILTER",
				"function:-:fn::preFilter",
				"table:-:person",
				"field:person:name",
				"field:person:half",
				"field:person:note",
				"field:person:tag",
				"function:-:fn::braces",
				"table:-:`odd;name#`",
				"table:-:⟨other;name⟩",
				"param:-:$rid",
				"param:-:$list",
				"param:-:$re2",
				"param:-:$half",
				"sequence:-:seq",
				"index:person:by_name",
				"analyzer:-:simple",
				"event:person:ev",
				"access:DATABASE:acc",
			]
		);
	}

	#[test_case("DEFINE\n  TABLE\tperson;", "person" ; "newline and tab after define")]
	#[test_case("DEFINE /* c */ TABLE person;", "person" ; "comment after define")]
	#[test_case("DEFINE TABLE IF  NOT\n EXISTS person;", "person" ; "spaced if not exists")]
	#[test_case("DEFINE TABLE overwrite_log;", "overwrite_log" ; "name starting with overwrite")]
	#[test_case("DEFINE TABLE `my table`;", "`my table`" ; "backtick name with a space")]
	#[test_case("DEFINE TABLE ⟨my table⟩;", "⟨my table⟩" ; "angle name with a space")]
	fn names_the_old_whitespace_split_got_wrong(sql: &str, name: &str) {
		let file = SchemaFile {
			path: "schema/x.surql".to_string(),
			hash: "h".to_string(),
			sql: sql.to_string(),
		};
		let (entities, _) = parse_schema_statements(&file, false).unwrap();
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].name, name);
	}

	#[test]
	fn a_lost_statement_boundary_is_an_error_naming_the_file_and_line() {
		let file = SchemaFile {
			path: "schema/glued.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE TABLE a;\nDEFINE PARAM $x VALUE 'oops;\nDEFINE TABLE b;".to_string(),
		};
		let err = parse_schema_statements(&file, false).unwrap_err().to_string();
		assert!(err.contains("schema file 'schema/glued.surql'"), "{err}");
		assert!(err.contains("line 2, column 23"), "{err}");
		assert!(err.contains("string is never closed"), "{err}");

		let file = SchemaFile {
			sql: "DEFINE TABLE a\nDEFINE TABLE b;".to_string(),
			..file
		};
		let err = parse_schema_statements(&file, false).unwrap_err().to_string();
		assert!(err.contains("line 2, column 1"), "{err}");
		assert!(err.contains("began on line 1"), "{err}");
	}

	#[test]
	fn a_non_define_statement_error_names_its_line() {
		let file = SchemaFile {
			path: "schema/x.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE TABLE a;\n\nCREATE a;".to_string(),
		};
		let err = parse_schema_statements(&file, false).unwrap_err().to_string();
		assert!(err.contains("non-DEFINE statement at line 3"), "{err}");
	}

	#[test]
	fn truncation_respects_character_boundaries() {
		let long = format!("{}⟨x⟩ and more text after it", "a".repeat(95));
		assert!(truncate_stmt(&long).ends_with("..."));
		assert_eq!(truncate_stmt("short"), "short");
	}

	// ---- prepare_schema_sql ---------------------------------------------------

	fn prepare(sql: &str) -> PreparedSql {
		prepare_schema_sql(sql).expect("prepare")
	}

	#[test_case("DEFINE SEQUENCE s BATCH 1 START 1;", "DEFINE SEQUENCE IF NOT EXISTS s BATCH 1 START 1;" ; "plain gets if not exists")]
	#[test_case("DEFINE SEQUENCE IF NOT EXISTS s;", "DEFINE SEQUENCE IF NOT EXISTS s;" ; "if not exists is kept")]
	#[test_case("define sequence s;", "define sequence IF NOT EXISTS s;" ; "lowercase")]
	#[test_case("DEFINE SEQUENCE overwrite_seq;", "DEFINE SEQUENCE IF NOT EXISTS overwrite_seq;" ; "name starting with overwrite")]
	#[test_case("DEFINE\n\tSEQUENCE s;", "DEFINE\n\tSEQUENCE IF NOT EXISTS s;" ; "whitespace kept")]
	fn sequences_are_never_overwritten_implicitly(sql: &str, expected: &str) {
		let prepared = prepare(sql);
		assert_eq!(prepared.sql.trim(), expected);
		assert!(prepared.warnings.is_empty(), "{:?}", prepared.warnings);
	}

	#[test]
	fn an_explicit_sequence_overwrite_is_kept_and_warned_about() {
		let prepared = prepare("DEFINE TABLE t;\nDEFINE SEQUENCE Overwrite s START 500;");
		assert!(
			prepared.sql.contains("DEFINE SEQUENCE Overwrite s START 500;"),
			"{}",
			prepared.sql
		);
		assert_eq!(
			prepared.warnings,
			vec![PrepareWarning::SequenceOverwrite {
				name: "s".to_string(),
				line: 2,
			}]
		);
		let message = prepared.warnings[0].to_string();
		assert!(message.contains("sequence `s`") && message.contains("line 2"), "{message}");
	}

	#[test_case("DEFINE TABLE overwrite_log;", "DEFINE TABLE OVERWRITE overwrite_log;" ; "name starting with overwrite")]
	#[test_case("DEFINE TABLE IF  NOT\n EXISTS t;", "DEFINE TABLE OVERWRITE t;" ; "spaced if not exists")]
	#[test_case("DEFINE\n  TABLE t;", "DEFINE\n  TABLE OVERWRITE t;" ; "newline before kind")]
	#[test_case("DEFINE /* c */ TABLE t;", "DEFINE /* c */ TABLE OVERWRITE t;" ; "comment before kind")]
	#[test_case("define table if not exists t;", "define table OVERWRITE t;" ; "lowercase")]
	fn other_kinds_get_overwrite_however_they_are_written(sql: &str, expected: &str) {
		assert_eq!(prepare(sql).sql.trim(), expected);
	}

	#[test]
	fn content_reaches_the_server_exactly_as_written() {
		let prepared = prepare(TRICKY_SCHEMA);
		assert!(
			prepared.sql.contains("VALUE /[ \\-_().,\\\\\\/$&+,:;=?@#|'<>^*%!]/;"),
			"{}",
			prepared.sql
		);
		assert!(prepared.sql.contains("DEFINE TABLE OVERWRITE person SCHEMAFULL;"));
		assert!(prepared.sql.contains("ASSERT $value = /^[a-z#';{]+$/;"));
		assert!(prepared.sql.contains("DEFINE SEQUENCE IF NOT EXISTS seq BATCH 1 START 1;"));
		assert!(prepared.sql.contains("LET $re = /x;y'/;"));
	}

	#[test]
	fn inner_comments_are_kept_and_outer_ones_dropped() {
		let prepared = prepare("-- lead\nDEFINE TABLE t -- why\n  SCHEMAFULL; # after\n");
		assert_eq!(prepared.sql, "DEFINE TABLE OVERWRITE t -- why\n  SCHEMAFULL;\n");
	}

	#[test]
	fn preparing_is_idempotent() {
		for sql in [TRICKY_SCHEMA, "DEFINE SEQUENCE s; DEFINE TABLE t; CREATE t;"] {
			let once = prepare(sql);
			assert_eq!(prepare(&once.sql).sql, once.sql);
		}
	}

	#[test]
	fn non_define_statements_pass_through_and_are_counted() {
		let prepared = prepare("LET $x = 1; CREATE t SET a = /x;/; DEFINE TABLE t;");
		assert_eq!(prepared.statements, 3);
		assert_eq!(
			prepared.sql,
			"LET $x = 1;\nCREATE t SET a = /x;/;\nDEFINE TABLE OVERWRITE t;\n"
		);
	}

	#[test]
	fn unreadable_sql_is_an_error_with_a_line() {
		let err = prepare_schema_sql("DEFINE TABLE a;\nDEFINE TABLE b);").unwrap_err().to_string();
		assert!(err.contains("line 2"), "{err}");
	}

	#[test_case("DEFINE ACCESS a ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM u);", true ; "keyless record")]
	#[test_case("DEFINE ACCESS IF NOT EXISTS a ON DATABASE TYPE RECORD;", true ; "keyless record if not exists")]
	#[test_case("DEFINE ACCESS a ON DATABASE TYPE BEARER FOR RECORD;", true ; "keyless bearer for record")]
	#[test_case("DEFINE ACCESS a ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'k';", false ; "keyed record")]
	#[test_case("DEFINE ACCESS a ON DATABASE TYPE RECORD WITH JWT URL 'https://x/jwks.json';", false ; "jwks record")]
	#[test_case("DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'k';", false ; "jwt access")]
	#[test_case("DEFINE TABLE record;", false ; "not an access")]
	fn keyless_record_access_is_warned_about(sql: &str, warns: bool) {
		let warnings = prepare(sql).warnings;
		assert_eq!(
			warnings.iter().any(
				|w| matches!(w, PrepareWarning::KeylessRecordAccess { name, .. } if name == "a")
			),
			warns,
			"{warnings:?}"
		);
	}

	#[test]
	fn overwrite_sequences_finds_only_explicit_overwrites() {
		let files = vec![SchemaFile {
			path: "schema/s.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE SEQUENCE a; DEFINE SEQUENCE OVERWRITE b; DEFINE SEQUENCE IF NOT EXISTS c; DEFINE TABLE OVERWRITE d;".to_string(),
		}];
		assert_eq!(overwrite_sequences(&files), BTreeSet::from(["b".to_string()]));
	}

	#[test]
	#[expect(deprecated)]
	fn ensure_overwrite_still_works_and_passes_unreadable_sql_through() {
		assert_eq!(ensure_overwrite("DEFINE TABLE t;"), "DEFINE TABLE OVERWRITE t;\n");
		assert_eq!(ensure_overwrite("DEFINE TABLE t);"), "DEFINE TABLE t);");
	}
}
