use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::core::sha256_hex;

pub const SCHEMA_DIR: &str = "database/schema";
pub const ROLLOUTS_DIR: &str = "database/rollouts";
pub const STATE_DIR: &str = "database/snapshots";
pub const SCHEMA_SNAPSHOT_PATH: &str = "database/snapshots/schema_snapshot.json";
pub const CATALOG_SNAPSHOT_PATH: &str = "database/snapshots/catalog_snapshot.json";

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityKey {
	pub kind: String,
	pub scope: Option<String>,
	pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogEntity {
	pub kind: String,
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

pub fn ensure_local_state_dirs() -> Result<()> {
	fs::create_dir_all(SCHEMA_DIR).with_context(|| format!("creating {}", SCHEMA_DIR))?;
	fs::create_dir_all(ROLLOUTS_DIR).with_context(|| format!("creating {}", ROLLOUTS_DIR))?;
	fs::create_dir_all(STATE_DIR).with_context(|| format!("creating {}", STATE_DIR))?;
	Ok(())
}

pub fn collect_schema_files() -> Result<Vec<SchemaFile>> {
	let mut files: Vec<PathBuf> = WalkDir::new(SCHEMA_DIR)
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

pub fn load_schema_snapshot() -> Result<SchemaSnapshot> {
	load_json_or_default(
		SCHEMA_SNAPSHOT_PATH,
		SchemaSnapshot {
			version: 1,
			files: Vec::new(),
		},
	)
}

pub fn save_schema_snapshot(snapshot: &SchemaSnapshot) -> Result<()> {
	save_json_pretty(SCHEMA_SNAPSHOT_PATH, snapshot)
}

pub fn load_catalog_snapshot() -> Result<CatalogSnapshot> {
	load_json_or_default(
		CATALOG_SNAPSHOT_PATH,
		CatalogSnapshot {
			version: 2,
			entities: Vec::new(),
		},
	)
}

pub fn save_catalog_snapshot(snapshot: &CatalogSnapshot) -> Result<()> {
	save_json_pretty(CATALOG_SNAPSHOT_PATH, snapshot)
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

pub fn build_catalog_snapshot(files: &[SchemaFile]) -> Result<CatalogSnapshot> {
	let mut entities = BTreeSet::new();
	for file in files {
		let statements = parse_schema_statements(file)?;
		for entity in statements {
			entities.insert(entity);
		}
	}

	Ok(CatalogSnapshot {
		version: 2,
		entities: entities.into_iter().collect(),
	})
}

pub fn parse_schema_statements(file: &SchemaFile) -> Result<Vec<CatalogEntity>> {
	let mut entities = Vec::new();
	for stmt in split_statements(&strip_line_comments(&file.sql)) {
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
			bail!(
				"schema file '{}' contains a non-DEFINE statement: '{}'",
				file.path,
				truncate_stmt(normalized)
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
	Ok(entities)
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

	let mut out = Vec::new();
	for entity in ordered {
		let stmt = match entity.kind.as_str() {
			"field" => {
				format!("REMOVE FIELD {} ON {};", entity.name, scope_or_err(&entity, "FIELD")?)
			}
			"event" => {
				format!("REMOVE EVENT {} ON {};", entity.name, scope_or_err(&entity, "EVENT")?)
			}
			"index" => {
				format!("REMOVE INDEX {} ON {};", entity.name, scope_or_err(&entity, "INDEX")?)
			}
			"table" => format!("REMOVE TABLE {};", entity.name),
			"function" => format!("REMOVE FUNCTION {};", entity.name),
			"param" => format!("REMOVE PARAM {};", entity.name),
			"access" => match &entity.scope {
				Some(scope) => format!("REMOVE ACCESS {} ON {};", entity.name, scope),
				None => format!("REMOVE ACCESS {};", entity.name),
			},
			"analyzer" => format!("REMOVE ANALYZER {};", entity.name),
			"user" => match &entity.scope {
				Some(scope) => format!("REMOVE USER {} ON {};", entity.name, scope),
				None => format!("REMOVE USER {};", entity.name),
			},
			"api" => {
				if api_supported {
					format!("REMOVE API {};", entity.name)
				} else {
					bail!(
						"API removal requested for '{}' but this SurrealDB server does not support `REMOVE API`. \
Use a manual migration or upgrade server support.",
						entity.name
					);
				}
			}
			_ => continue,
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
	let weight = match entity.kind.as_str() {
		"index" => 0,
		"event" => 1,
		"field" => 2,
		"access" => 3,
		"user" => 4,
		"function" => 5,
		"param" => 6,
		"api" => 7,
		"analyzer" => 8,
		"table" => 9,
		_ => 10,
	};
	(weight, entity.scope.clone(), entity.kind.clone(), entity.name.clone())
}

fn normalize_path(path: &Path) -> Result<String> {
	let cwd = std::env::current_dir().context("resolving current directory")?;
	let rel = path.strip_prefix(&cwd).or_else(|_| path.strip_prefix(".")).unwrap_or(path);
	Ok(rel.to_string_lossy().replace('\\', "/"))
}

fn load_json_or_default<T>(path: &str, default: T) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	let p = Path::new(path);
	if !p.exists() {
		return Ok(default);
	}

	let raw = fs::read_to_string(p).with_context(|| format!("reading {}", path))?;
	let parsed = serde_json::from_str(&raw).with_context(|| format!("parsing {}", path))?;
	Ok(parsed)
}

fn save_json_pretty<T>(path: &str, value: &T) -> Result<()>
where
	T: Serialize,
{
	ensure_local_state_dirs()?;
	let raw = serde_json::to_string_pretty(value).context("serializing json")?;
	fs::write(path, format!("{raw}\n")).with_context(|| format!("writing {}", path))?;
	Ok(())
}

/// Ensures every `DEFINE` statement includes the `OVERWRITE` modifier so that
/// sync can re-apply schemas idempotently against an existing database.
pub fn ensure_overwrite(sql: &str) -> String {
	let stmts = split_statements(&strip_line_comments(sql));
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

fn strip_line_comments(sql: &str) -> String {
	sql.lines()
		.filter(|line| {
			let t = line.trim_start();
			!(t.starts_with("--") || t.starts_with("//"))
		})
		.collect::<Vec<_>>()
		.join("\n")
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

	let kind = tokens[1].to_ascii_lowercase();
	let mut idx = 2;
	idx = skip_modifiers(&tokens, idx);
	if idx >= tokens.len() {
		return None;
	}

	let (scope, name) = match kind.as_str() {
		"table" => (None, clean_ident(tokens[idx])),
		"field" | "event" | "index" => {
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
		"function" | "param" | "analyzer" | "api" => (None, clean_ident(tokens[idx])),
		"access" | "user" => {
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
	use super::*;

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
			"#
			.to_string(),
		}];

		let catalog = build_catalog_snapshot(&files).expect("catalog build");
		assert!(catalog.entities.contains(&CatalogEntity {
			kind: "table".to_string(),
			scope: None,
			name: "person".to_string(),
			source_path: "database/schema/root.surql".to_string(),
			statement_hash: sha256_hex("DEFINE TABLE OVERWRITE person SCHEMAFULL".as_bytes()),
			file_hash: "x".to_string(),
		}));
		assert!(catalog.entities.iter().any(|entity| {
			entity.kind == "field"
				&& entity.scope.as_deref() == Some("person")
				&& entity.name == "name"
				&& entity.source_path == "database/schema/root.surql"
		}));
		assert!(catalog.entities.iter().any(|entity| entity.kind == "api" && entity.name == "v1"));
	}

	#[test]
	fn render_remove_sql_respects_api_support() {
		let entities = vec![
			EntityKey {
				kind: "table".to_string(),
				scope: None,
				name: "person".to_string(),
			},
			EntityKey {
				kind: "field".to_string(),
				scope: Some("person".to_string()),
				name: "nickname".to_string(),
			},
			EntityKey {
				kind: "api".to_string(),
				scope: None,
				name: "v1".to_string(),
			},
		];

		let supported = render_remove_sql(&entities, true).expect("api should be supported");
		assert_eq!(supported[0], "REMOVE FIELD nickname ON person;");
		assert!(supported.iter().any(|line| line == "REMOVE API v1;"));
		assert_eq!(supported.last().expect("table removal"), "REMOVE TABLE person;");

		let unsupported = render_remove_sql(&entities, false);
		assert!(unsupported.is_err());
	}

	#[test]
	fn schema_rejects_non_define_sql() {
		let file = SchemaFile {
			path: "database/schema/root.surql".to_string(),
			hash: "x".to_string(),
			sql: "CREATE person SET name = 'a';".to_string(),
		};

		let err = parse_schema_statements(&file).expect_err("must reject create");
		assert!(err.to_string().contains("non-DEFINE"));
	}

	#[test]
	fn schema_allows_let_variables() {
		let file = SchemaFile {
			path: "database/schema/storage.surql".to_string(),
			hash: "x".to_string(),
			sql: "LET $types = ['image/png', 'image/jpeg'];\nDEFINE TABLE OVERWRITE storage SCHEMAFULL;".to_string(),
		};

		let entities = parse_schema_statements(&file).expect("LET should be allowed");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "table");
	}

	#[test]
	fn catalog_diff_detects_statement_changes() {
		let old = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: "field".to_string(),
				scope: Some("person".to_string()),
				name: "nickname".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: "a".to_string(),
				file_hash: "file-a".to_string(),
			}],
		};
		let new = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: "field".to_string(),
				scope: Some("person".to_string()),
				name: "nickname".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: "b".to_string(),
				file_hash: "file-b".to_string(),
			}],
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
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS table");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "table");
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
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS field");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "field");
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
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS event");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "event");
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
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS index");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "index");
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
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS function");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "function");
		assert_eq!(entities[0].name, "fn::greet");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_param() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE PARAM IF NOT EXISTS $env VALUE 'dev';".to_string(),
		};
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS param");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "param");
		assert_eq!(entities[0].name, "$env");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_analyzer() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE ANALYZER IF NOT EXISTS english TOKENIZERS blank, class;".to_string(),
		};
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS analyzer");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "analyzer");
		assert_eq!(entities[0].name, "english");
	}

	#[test]
	fn parse_schema_statements_accepts_if_not_exists_access() {
		let file = SchemaFile {
			path: "database/schema/test.surql".to_string(),
			hash: "h".to_string(),
			sql: "DEFINE ACCESS IF NOT EXISTS admin ON DATABASE TYPE RECORD;".to_string(),
		};
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS access");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "access");
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
		let entities = parse_schema_statements(&file).expect("should parse IF NOT EXISTS user");
		assert_eq!(entities.len(), 1);
		assert_eq!(entities[0].kind, "user");
		assert_eq!(entities[0].name, "app");
		assert_eq!(entities[0].scope.as_deref(), Some("DATABASE"));
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

		let catalog = build_catalog_snapshot(&files).expect("catalog should handle IF NOT EXISTS");
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
				kind: "table".to_string(),
				scope: None,
				name: "person".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: ine_hash,
				file_hash: "f1".to_string(),
			}],
		};
		let new = CatalogSnapshot {
			version: 2,
			entities: vec![CatalogEntity {
				kind: "table".to_string(),
				scope: None,
				name: "person".to_string(),
				source_path: "database/schema/a.surql".to_string(),
				statement_hash: ow_hash,
				file_hash: "f2".to_string(),
			}],
		};

		let diff = diff_catalog(&old, &new);
		assert_eq!(diff.modified.len(), 1, "modifier change should be a modification");
	}
}
