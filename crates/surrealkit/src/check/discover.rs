//! The files a check reads: the schema of the selected modules, and every other `.surql` file
//! under the project root.
//!
//! The project root is the directory the check runs from and the database folder is `folder`
//! under it, the way `surrealkit compile` resolves them. Nothing here reads a connection
//! setting.

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

use super::config::AnalyzeConfig;
use crate::module::Module;
use crate::project::{CONFIG_FILE_NAME, ProjectConfig};

/// Directories no check reads, whatever `[analyze] ignore` says.
const BUILTIN_IGNORE: &[&str] = &["target", "node_modules", ".git"];

/// What a source file is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
	/// A schema file of a selected module: applied, in order, to build the schema.
	Schema,
	/// Any other `.surql` file: checked against the schema.
	Query,
}

/// One source file, as read from disk.
#[derive(Debug, Clone)]
pub struct Source {
	/// Absolute.
	pub path: PathBuf,
	pub text: String,
	pub role: Role,
	/// The schema module a [`Role::Schema`] file belongs to.
	pub module: Option<String>,
}

/// Everything a check reads.
#[derive(Debug, Clone)]
pub struct Sources {
	/// The project root.
	pub root: PathBuf,
	/// Schema files, in the order they apply: modules in dependency order, files sorted.
	pub schema: Vec<Source>,
	/// Query files, sorted by path.
	pub queries: Vec<Source>,
	/// The schema directory of each selected module, absolute.
	pub schema_dirs: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
enum Pattern {
	/// Matches any path component.
	Name(String),
	/// Matches a root-relative path and everything under it, as components.
	Path(Vec<String>),
}

#[derive(Debug, Clone)]
struct Entry {
	/// As the user wrote it, or the built-in name.
	written: String,
	pattern: Pattern,
	builtin: bool,
}

impl Entry {
	/// Whether `components` (a root-relative path) is this entry or under it.
	fn covers(&self, components: &[String]) -> bool {
		match &self.pattern {
			Pattern::Name(name) => components.iter().any(|part| part == name),
			Pattern::Path(path) => components.starts_with(path),
		}
	}
}

/// The paths a check does not read: the built-in ignores and `[analyze] ignore`.
#[derive(Debug, Clone)]
pub struct Ignore {
	entries: Vec<Entry>,
}

impl Ignore {
	/// The built-in ignores and `config`'s `ignore`. An entry is a name, matching any path
	/// component, or (when it holds a `/`) a path relative to the project root; a trailing
	/// `/**` or `/` is dropped. An entry with a glob character is an error: there are no globs,
	/// and one that silently matched nothing would hide the files it was meant to skip.
	pub fn from_config(config: &AnalyzeConfig) -> Result<Self> {
		let mut entries: Vec<Entry> = BUILTIN_IGNORE
			.iter()
			.map(|name| Entry {
				written: (*name).to_string(),
				pattern: Pattern::Name((*name).to_string()),
				builtin: true,
			})
			.collect();
		for written in &config.ignore {
			let trimmed = written.trim();
			let trimmed = trimmed.strip_suffix("/**").unwrap_or(trimmed);
			let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
			if trimmed.contains(['*', '?', '[']) {
				bail!(
					"[analyze] ignore entry {written:?} holds a glob character: ignore takes \
					 names (`dist`, any path component) and root-relative paths \
					 (`src/generated`), not globs"
				);
			}
			let parts: Vec<String> = trimmed
				.split('/')
				.filter(|part| !part.is_empty() && *part != ".")
				.map(str::to_string)
				.collect();
			if parts.is_empty() {
				bail!("[analyze] ignore entry {written:?} names nothing");
			}
			if parts.iter().any(|part| part == "..") {
				bail!("[analyze] ignore entry {written:?} leaves the project root");
			}
			let pattern = if trimmed.contains('/') {
				Pattern::Path(parts)
			} else {
				Pattern::Name(parts.into_iter().next().unwrap_or_default())
			};
			entries.push(Entry {
				written: written.clone(),
				pattern,
				builtin: false,
			});
		}
		Ok(Self {
			entries,
		})
	}

	/// Whether `relative`, a path under the project root, is ignored.
	pub fn matches(&self, relative: &Path) -> bool {
		let components = components(relative);
		self.entries.iter().any(|entry| entry.covers(&components))
	}
}

/// The `Normal` components of a path, as strings.
fn components(path: &Path) -> Vec<String> {
	path.components()
		.filter_map(|component| match component {
			Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
			_ => None,
		})
		.collect()
}

/// `path` with `.` dropped and `..` folded into the component before it, without touching the
/// filesystem: a directory that does not exist yet is a normal state. A `..` with nothing to
/// fold into is kept, so [`root_relative`] still rejects a path that escapes the root.
fn normalize(path: &Path) -> PathBuf {
	let mut out = PathBuf::new();
	for component in path.components() {
		match component {
			Component::CurDir => {}
			Component::ParentDir => {
				if matches!(out.components().next_back(), Some(Component::Normal(_))) {
					out.pop();
				} else {
					out.push("..");
				}
			}
			other => out.push(other.as_os_str()),
		}
	}
	out
}

/// `dir` relative to `root`, with `.` and `..` resolved first.
///
/// A directory outside `root` is an error: the check reads the project under its root, and
/// files outside it are not part of the project, so reading none would report every table as
/// missing.
fn root_relative(root: &Path, dir: &Path, module: &str) -> Result<PathBuf> {
	let absolute = if dir.is_absolute() {
		dir.to_path_buf()
	} else {
		root.join(dir)
	};
	let normalized = normalize(&absolute);
	let root = normalize(root);
	// One message rather than a `with_context` wrapper: it has to be read whole, and anyhow
	// would print a wrapper alone on the first line and bury the rest under "Caused by".
	let relative = normalized.strip_prefix(&root).map_err(|_| {
		anyhow::anyhow!(
			"schema module {module:?}: the schema directory `{}` is outside the project root \
			 `{}`; run from a directory that contains both, or keep the database folder under \
			 the one holding {CONFIG_FILE_NAME}",
			normalized.display(),
			root.display()
		)
	})?;
	Ok(relative.to_path_buf())
}

/// An ignore that covers a selected schema directory, rejected.
///
/// `ignore = ["schema"]` (or `["database"]`) would skip every module's directory, and the
/// check would then read no schema and report every table as missing: hundreds of findings,
/// none of them real, and nothing pointing at the line that caused them. The built-in ignores
/// are checked too, and get a message of their own, because the fix differs: there is no line
/// to delete, only a directory to move.
fn reject_ignored_schema_dir(ignore: &Ignore, dirs: &[(String, PathBuf)]) -> Result<()> {
	for entry in &ignore.entries {
		for (module, relative) in dirs {
			if !entry.covers(&components(relative)) {
				continue;
			}
			let shown = relative.display();
			if entry.builtin {
				bail!(
					"the built-in ignore `{}` covers schema module {module:?}'s directory \
					 `{shown}`, so the check would read no schema at all. `target`, \
					 `node_modules` and `.git` are always skipped; move the database folder out \
					 of `{}/`.",
					entry.written,
					entry.written
				);
			}
			bail!(
				"[analyze] ignore = [… {:?} …] in {CONFIG_FILE_NAME} covers the schema directory \
				 of module {module:?} (`{shown}`), so the check would read no schema at all. An \
				 entry without a `/` matches that name anywhere in the tree, so it may be \
				 broader than it looks.",
				entry.written
			);
		}
	}
	Ok(())
}

fn read(path: &Path) -> Result<String> {
	fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

/// `folder` as the absolute, normalized path the layout is built from: a relative `folder` is
/// under `root`.
fn folder_under(root: &Path, folder: &str) -> String {
	normalize(&if Path::new(folder).is_absolute() {
		PathBuf::from(folder)
	} else {
		root.join(folder)
	})
	.to_string_lossy()
	.into_owned()
}

/// The schema directory of each of `modules` under `folder`, normalized, in the same order.
fn schema_dirs(project: &ProjectConfig, folder: &str, modules: &[Module]) -> Vec<PathBuf> {
	modules
		.iter()
		.map(|module| normalize(&project.layout_for(folder, module).schema_dir()))
		.collect()
}

/// The schema files of `modules`, each read from its directory in `dirs` (the same order), in
/// the order they apply.
fn read_schema(folder: &str, modules: &[Module], dirs: &[PathBuf]) -> Result<Vec<Source>> {
	let mut schema = Vec::new();
	for (module, dir) in modules.iter().zip(dirs) {
		for path in crate::schema_state::schema_file_paths(folder, dir)? {
			schema.push(Source {
				text: read(&path)?,
				path: normalize(&path),
				role: Role::Schema,
				module: Some(module.name().to_string()),
			});
		}
	}
	Ok(schema)
}

/// The schema files of `modules` alone, in the order they apply: what [`discover`] reads as
/// [`Sources::schema`], without walking the project for queries. A relative `folder` is under
/// `root`; no path here is resolved against the process working directory.
pub fn discover_schema(
	project: &ProjectConfig,
	folder: &str,
	root: &Path,
	modules: &[Module],
) -> Result<Vec<Source>> {
	let folder = folder_under(&normalize(root), folder);
	read_schema(&folder, modules, &schema_dirs(project, &folder, modules))
}

/// Find the files a check reads.
///
/// `root` is the project root; a relative `folder` is under it. `modules` are the selected
/// schema modules in dependency order (see [`select_modules`](crate::selection::select_modules)).
///
/// Schema is, per module, the files `surrealkit compile` collects. Queries are every other
/// `.surql` file under `root`, except the built-in and configured ignores, every declared
/// module's schema directory (selected or not), every module's `rollouts` and `snapshots`,
/// `<folder>/tests` and `<folder>/setup.surql`. Seed files are queries. An ignore affects
/// queries only; one that would cover a selected schema directory is an error.
pub fn discover(
	project: &ProjectConfig,
	folder: &str,
	root: &Path,
	modules: &[Module],
	config: &AnalyzeConfig,
) -> Result<Sources> {
	let root = normalize(root);
	let folder_str = folder_under(&root, folder);
	let ignore = Ignore::from_config(config)?;

	let schema_dirs = schema_dirs(project, &folder_str, modules);
	let mut relative_dirs = Vec::new();
	for (module, dir) in modules.iter().zip(&schema_dirs) {
		relative_dirs.push((module.name().to_string(), root_relative(&root, dir, module.name())?));
	}
	reject_ignored_schema_dir(&ignore, &relative_dirs)?;
	let schema = read_schema(&folder_str, modules, &schema_dirs)?;

	// A declared module's directories are never queries, selected or not.
	let mut every: Vec<Module> = modules.to_vec();
	every.extend(project.schema.keys().filter_map(|name| Module::new(name.clone()).ok()));
	let mut excluded: HashSet<PathBuf> = HashSet::new();
	for module in &every {
		let layout = project.layout_for(&folder_str, module);
		excluded.insert(normalize(&layout.schema_dir()));
		excluded.insert(normalize(&layout.rollouts_dir()));
		excluded.insert(normalize(&layout.state_dir()));
	}
	excluded.insert(normalize(&crate::constants::tests_dir(&folder_str)));
	excluded.insert(normalize(&crate::constants::setup_surql_path(&folder_str)));
	let schema_paths: HashSet<&Path> = schema.iter().map(|source| source.path.as_path()).collect();

	let walker =
		WalkDir::new(&root).follow_links(false).sort_by_file_name().into_iter().filter_entry(
			|entry| {
				let Ok(relative) = entry.path().strip_prefix(&root) else {
					return true;
				};
				entry.depth() == 0 || !(ignore.matches(relative) || excluded.contains(entry.path()))
			},
		);
	let mut queries = Vec::new();
	for entry in walker {
		let entry = entry.with_context(|| format!("walking {}", root.display()))?;
		if !entry.file_type().is_file()
			|| entry.path().extension().and_then(OsStr::to_str) != Some("surql")
			|| schema_paths.contains(entry.path())
		{
			continue;
		}
		queries.push(Source {
			text: read(entry.path())?,
			path: entry.into_path(),
			role: Role::Query,
			module: None,
		});
	}
	queries.sort_by(|a, b| a.path.cmp(&b.path));

	Ok(Sources {
		root,
		schema,
		queries,
		schema_dirs,
	})
}

#[cfg(test)]
mod tests {
	use tempfile::TempDir;

	use super::*;
	use crate::selection::select_modules;

	fn project(raw: &str) -> ProjectConfig {
		ProjectConfig::parse(raw).expect("config parses")
	}

	fn ignore(entries: &[&str]) -> Result<Ignore> {
		Ignore::from_config(&AnalyzeConfig {
			ignore: entries.iter().map(|e| (*e).to_string()).collect(),
			..AnalyzeConfig::default()
		})
	}

	fn run(raw: &str, folder: &str, root: &Path, ignored: &[&str]) -> Result<Sources> {
		let project = project(raw);
		let modules = select_modules(&project, &[], false, false)?;
		let config = AnalyzeConfig {
			ignore: ignored.iter().map(|e| (*e).to_string()).collect(),
			..AnalyzeConfig::default()
		};
		discover(&project, folder, root, &modules, &config)
	}

	fn write(root: &Path, files: &[&str]) {
		for file in files {
			let path = root.join(file);
			fs::create_dir_all(path.parent().expect("parent")).expect("dir");
			fs::write(path, format!("-- {file}\n")).expect("file");
		}
	}

	fn names(sources: &[Source], root: &Path) -> Vec<String> {
		sources
			.iter()
			.map(|s| s.path.strip_prefix(root).expect("under root").to_string_lossy().into_owned())
			.collect()
	}

	#[test]
	fn a_bare_name_matches_any_component_and_a_path_matches_its_subtree() {
		let ignore = ignore(&["dist", "src/generated/", "docs/**", "./legacy"]).unwrap();
		for hit in [
			"dist/a.surql",
			"app/dist/a.surql",
			"app/dist",
			"src/generated/a.surql",
			"src/generated",
			"docs/x/y.surql",
			"legacy/q.surql",
			"target/q.surql",
			"a/docs/x.surql",
			"a/node_modules/q.surql",
			".git/q.surql",
		] {
			assert!(ignore.matches(Path::new(hit)), "{hit}");
		}
		for miss in [
			"src/a.surql",
			"app/src/generated/a.surql",
			"distance/a.surql",
			"src/generated.surql",
			"app/legacy/q.surql",
		] {
			assert!(!ignore.matches(Path::new(miss)), "{miss}");
		}
	}

	#[test]
	fn a_glob_in_an_ignore_entry_is_an_error_that_says_so() {
		for entry in ["*.gen.surql", "src/**/gen", "a?b", "x[12]"] {
			let message = format!("{:#}", ignore(&[entry]).expect_err(entry));
			assert!(message.contains("glob"), "{entry}: {message}");
			assert!(message.contains(entry), "{entry}: {message}");
		}
		assert!(ignore(&[""]).is_err());
		assert!(ignore(&["../x/y"]).is_err());
	}

	#[test]
	fn the_project_root_is_never_judged_by_the_directories_above_it() {
		// A project that lives under a directory called `target` is still checked.
		let dir = TempDir::new().expect("tempdir");
		let root = dir.path().join("target").join("app");
		write(&root, &["database/schema/a.surql", "q/b.surql"]);
		let sources = run("", "./database", &root, &[]).expect("discovers");
		assert_eq!(names(&sources.schema, &root), vec!["database/schema/a.surql"]);
		assert_eq!(names(&sources.queries, &root), vec!["q/b.surql"]);
	}

	#[test]
	fn schema_is_in_module_order_and_queries_are_everything_else() {
		let dir = TempDir::new().expect("tempdir");
		let root = dir.path();
		write(
			root,
			&[
				"database/modules/billing/schema/20_invoice.surql",
				"database/modules/billing/schema/10_plan.surql",
				"database/modules/core/schema/05_user.surql",
				"database/modules/other/schema/x.surql",
				"database/modules/core/rollouts/r1/schema/user.surql",
				"database/modules/core/snapshots/s.surql",
				"database/tests/t.surql",
				"database/setup.surql",
				"database/seed/001.surql",
				"queries/list.surql",
				"queries/target/skipped.surql",
				"node_modules/dep/x.surql",
				"dist/gen.surql",
				"top.surql",
			],
		);
		let raw = "[schema.core]\n[schema.billing]\ndepends_on = [\"core\"]\n[schema.other]\n";
		let project = project(raw);
		let modules = select_modules(&project, &["billing".to_string()], false, false).unwrap();
		let config = AnalyzeConfig {
			ignore: vec!["dist".to_string()],
			..AnalyzeConfig::default()
		};
		let sources = discover(&project, "./database", root, &modules, &config).expect("discovers");
		assert_eq!(
			names(&sources.schema, root),
			vec![
				"database/modules/core/schema/05_user.surql",
				"database/modules/billing/schema/10_plan.surql",
				"database/modules/billing/schema/20_invoice.surql",
			]
		);
		assert_eq!(sources.schema[0].module.as_deref(), Some("core"));
		assert!(sources.schema.iter().all(|s| s.role == Role::Schema));
		assert_eq!(
			names(&sources.queries, root),
			vec!["database/seed/001.surql", "queries/list.surql", "top.surql"],
			"an unselected module's schema, rollouts, snapshots, tests, setup, the built-in and \
			 configured ignores are not queries"
		);
		assert!(sources.queries.iter().all(|s| s.role == Role::Query && s.module.is_none()));
		assert!(sources.queries[0].text.contains("seed/001"));
		assert_eq!(sources.schema_dirs.len(), 2);
		assert!(sources.schema_dirs.iter().all(|dir| dir.is_absolute()));
	}

	#[test]
	fn the_default_module_layout_is_found_without_a_config() {
		let dir = TempDir::new().expect("tempdir");
		let root = dir.path();
		write(
			root,
			&[
				"database/schema/a.surql",
				"database/schema/sub/b.surql",
				"database/rollouts/x.surql",
			],
		);
		let sources = run("", "./database", root, &[]).expect("discovers");
		assert_eq!(
			names(&sources.schema, root),
			vec!["database/schema/a.surql", "database/schema/sub/b.surql"]
		);
		assert!(sources.queries.is_empty());
		// A folder given as `database` or as an absolute path finds the same files.
		let plain = run("", "database", root, &[]).expect("discovers");
		assert_eq!(names(&plain.schema, root), names(&sources.schema, root));
		let absolute = run("", root.join("database").to_str().unwrap(), root, &[]).unwrap();
		assert_eq!(names(&absolute.schema, root), names(&sources.schema, root));
	}

	#[test]
	fn a_module_with_no_files_contributes_nothing() {
		let dir = TempDir::new().expect("tempdir");
		let sources = run("[schema.core]\n", "./database", dir.path(), &[]).expect("not an error");
		assert!(sources.schema.is_empty() && sources.queries.is_empty());
	}

	#[test]
	fn a_schema_directory_outside_the_root_is_an_error_not_a_silent_miss() {
		// `--folder ../shared` puts the schema where the walk never goes. It must say so,
		// rather than check the project against an empty schema.
		let error = run("", "../shared", Path::new("/proj"), &[])
			.expect_err("a schema directory outside the root is an error");
		let message = format!("{error:#}");
		assert!(message.contains("outside the project root"), "{message}");
		assert!(message.contains("/shared/schema"), "{message}");
	}

	#[test]
	fn an_absolute_schema_directory_under_the_root_is_relative_to_it() {
		let dir = TempDir::new().expect("tempdir");
		let root = dir.path();
		write(root, &["database/schema/a.surql"]);
		let absolute = root.join("database");
		let sources = run("", absolute.to_str().unwrap(), root, &[]).expect("discovers");
		assert_eq!(names(&sources.schema, root), vec!["database/schema/a.surql"]);
	}

	#[test]
	fn an_ignore_that_would_cover_a_schema_dir_is_rejected() {
		// `ignore = ["schema"]` would skip every module's directory, and the check would then
		// report every table as missing.
		for pattern in ["schema", "schema/**", "database", "database/schema", "./database/"] {
			let error = run("", "./database", Path::new("/proj"), &[pattern])
				.expect_err("an ignore that blanks the schema is an error");
			let message = format!("{error:#}");
			assert!(message.contains("schema directory"), "{pattern}: {message}");
			assert!(message.contains(pattern), "{pattern}: {message}");
			assert!(message.contains("[analyze] ignore"), "{pattern}: {message}");
		}
		// A path that is not on the schema path stays allowed, as does one beside it.
		let dir = TempDir::new().expect("tempdir");
		run("", "./database", dir.path(), &["dist/**", "database/schemas"])
			.expect("an unrelated ignore is fine");
	}

	#[test]
	fn a_builtin_ignore_covering_a_schema_dir_says_so_in_its_own_words() {
		// `[schema.x] path` under `target/` is skipped just as thoroughly, but there is no
		// config line to delete, so the message points at the directory.
		let error =
			run("[schema.core]\npath = \"target/schema\"\n", "./database", Path::new("/proj"), &[])
				.expect_err("a schema directory under target/ is an error");
		let message = format!("{error:#}");
		assert!(message.contains("built-in ignore `target`"), "{message}");
		assert!(message.contains("\"core\""), "{message}");
		assert!(
			!message.contains("[analyze] ignore"),
			"there is no config line to blame: {message}"
		);
	}
}
