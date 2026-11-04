//! The `compile` command: what its flags and `surrealkit.toml` ask for ([`Plan`]), the files a
//! run reads and writes ([`WatchSet`], what `compile --watch-paths` prints), and `compile
//! --watch` ([`watch`]), which regenerates the outputs when one of those files changes.
//!
//! A watch keeps the engine of the last schema that compiled, so a change to a query types that
//! query against it and nothing else ([`QueryCache`] holds the answers for the others). A change
//! to the schema, `surrealkit.toml` or `.env*` starts again from a fresh engine. What it writes
//! is what a one-shot `compile` of the same files writes.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::Serialize;

use crate::compile::CompileOpts;
use crate::module::Module;
use crate::project::ProjectConfig;
use crate::selection::select_modules;
use crate::typegen::{SchemaVersion, TypegenOpts};
use crate::variables::{
	QueriesConfig, TemplateVars, TypegenConfig, build_project_vars, load_typegen_config,
};

/// What the command line says a compile reads and writes.
#[derive(Debug, Clone, Default)]
pub struct CompileArgs {
	/// The project folder, `--folder`.
	pub folder: String,
	/// `--schema`.
	pub schemas: Vec<String>,
	/// `--all`.
	pub all: bool,
	/// `--no-deps`.
	pub no_deps: bool,
	/// `--var KEY=VALUE`.
	pub vars: Vec<(String, String)>,
	/// `--out`.
	pub out: Option<PathBuf>,
	/// `--compact`.
	pub compact: bool,
	/// `--lenient`.
	pub lenient: bool,
	/// `--allow-experimental`.
	pub allow_experimental: Vec<String>,
}

/// A compile as configured: the project, the selected modules and the options of the run, with
/// no query collected yet.
pub struct Plan {
	pub project: ProjectConfig,
	pub modules: Vec<Module>,
	pub typegen: TypegenConfig,
	/// `[typegen.queries]`, when it is set.
	pub queries: Option<QueriesConfig>,
	pub compile: CompileOpts,
	pub outputs: TypegenOpts,
}

impl Plan {
	/// Read `surrealkit.toml` from the working directory (or the nearest one above it), select
	/// the modules, and build the options of a run. `stdout` and `check` are `--stdout` and
	/// `--check`. Collects no query.
	///
	/// # Errors
	///
	/// Fails when the configuration or the module selection is invalid, or when `[typegen]
	/// json` is a version 1 document, which compile does not write.
	pub fn load(args: &CompileArgs, stdout: bool, check: bool) -> Result<Self> {
		let project = ProjectConfig::load(None)?;
		let typegen = load_typegen_config(None)?;
		Self::from_config(project, typegen, None, args, stdout, check)
	}

	/// [`Plan::load`] for a `project` and its `typegen` section that are already read.
	/// `config_path` is the `surrealkit.toml` the `[variables]` come from (`None`: the one in
	/// the working directory).
	///
	/// # Errors
	///
	/// Fails as [`Plan::load`] does.
	pub fn from_config(
		project: ProjectConfig,
		typegen: TypegenConfig,
		config_path: Option<&Path>,
		args: &CompileArgs,
		stdout: bool,
		check: bool,
	) -> Result<Self> {
		let modules = select_modules(&project, &args.schemas, args.all, args.no_deps)?;
		// `sync` and `typegen` write the configured JSON in the configured version; compile
		// writing v2 there would make the commands overwrite one another.
		if args.out.is_none()
			&& !stdout
			&& typegen.json_path()?.is_some()
			&& typegen.schema_version()? != SchemaVersion::V2
		{
			bail!(
				"compile writes schema document v2, but [typegen] json is the v1 document \
				 `sync` and `typegen` write (schema_version is 1 unless set); set [typegen] \
				 schema_version = 2 so that every command writes v2 there, or pass --out or \
				 --stdout"
			);
		}
		let queries = typegen.queries_config()?;
		let mut experimental = typegen.experimental.clone();
		experimental.extend(args.allow_experimental.iter().cloned());
		let compile = CompileOpts {
			experimental,
			// Files and flags only: no SURREALKIT_VAR_* from the environment.
			vars: TemplateVars {
				vars: build_project_vars(&args.vars, config_path)?,
			},
			expected_engine: typegen.engine.clone(),
			queries: None,
		};
		let outputs = TypegenOpts {
			out: args.out.clone().or(typegen.json_path()?),
			stdout,
			pretty: !args.compact,
			ts_out: typegen.typescript_path()?,
			ts_format: typegen.format.clone(),
			strict: !args.lenient,
			check,
			omit_generated_at: true,
			schema_version: SchemaVersion::V2,
			emitters: typegen.emitters()?,
			acknowledge: typegen.acknowledgements()?,
			// --lenient tolerates diagnostics, never an emitter that cannot run.
			emitter_failures_fatal: true,
			queries: match &queries {
				Some(config) => Some(crate::typegen::QueriesOpts {
					collected: Default::default(),
					typescript: typegen.queries_typescript()?,
					render: config.render_options(),
				}),
				None => None,
			},
		};
		Ok(Self {
			project,
			modules,
			typegen,
			queries,
			compile,
			outputs,
		})
	}

	/// Collect the project's queries from disk into the options, as a one-shot compile types
	/// them.
	///
	/// # Errors
	///
	/// Fails as [`crate::codegen::section::collect`] does.
	pub fn collect(mut self) -> Result<Self> {
		if let Some(config) = &self.queries {
			let collected = crate::codegen::section::collect(".", config)?;
			if let Some(outputs) = &mut self.outputs.queries {
				outputs.collected = collected.clone();
			}
			self.compile.queries = Some(collected);
		}
		Ok(self)
	}

	/// The files this compile reads and writes.
	pub fn watch_set(&self, folder: &str) -> Result<WatchSet> {
		watch_set(&self.project, folder, &self.modules, &self.typegen, self.outputs.out.as_deref())
	}
}

// TEMPORARY. `surrealkit check`'s branch makes `host_queries::scan`'s rule public as
// `host_queries::is_source` and `pub` `SOURCE_EXTENSIONS` / `SKIPPED_DIRECTORIES`. Replace the
// next three items with those, and delete them. `is_source` takes the path relative to the
// scanned root (a root that is a file: its file name), the way the real one does.

/// The extensions of the host code `[typegen.queries] sources` reads.
const SOURCE_EXTENSIONS: &[&str] =
	&["ts", "mts", "cts", "tsx", "js", "mjs", "cjs", "jsx", "svelte", "vue", "astro"];

/// The directories host code is never read from, besides hidden ones.
const SKIPPED_DIRECTORIES: &[&str] = &[
	"node_modules",
	"dist",
	"build",
	"target",
	".svelte-kit",
	".next",
	".nuxt",
	".astro",
	".output",
];

/// Whether `relative` (below a scanned root) is a file of host code: an extension in
/// [`SOURCE_EXTENSIONS`], not a `.d.ts` declaration, and no directory in it hidden or in
/// [`SKIPPED_DIRECTORIES`].
#[cfg(feature = "watch")]
fn is_source(relative: &Path) -> bool {
	let Some(name) = relative.file_name().and_then(|name| name.to_str()) else {
		return false;
	};
	let lowered = name.to_ascii_lowercase();
	if lowered.ends_with(".d.ts") {
		return false;
	}
	let extension = lowered.rsplit_once('.').map_or("", |(_, extension)| extension);
	if !SOURCE_EXTENSIONS.contains(&extension) {
		return false;
	}
	let directories = relative.parent().into_iter().flat_map(Path::components);
	!directories.into_iter().any(|component| {
		let name = component.as_os_str().to_string_lossy();
		name.starts_with('.') || SKIPPED_DIRECTORIES.contains(&name.as_ref())
	})
}

/// The files a compile reads and writes, as configured: relative to the working directory
/// unless the configuration says otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchSet {
	/// The selected modules' schema directories, in dependency order.
	pub schema: Vec<PathBuf>,
	/// `surrealkit.toml`, `.env` and `.env.local`.
	pub config: Vec<PathBuf>,
	/// `[typegen.queries] files`: directories of query files.
	pub query_files: Vec<PathBuf>,
	/// `[typegen.queries] sources`: files and directories of host code.
	pub query_sources: Vec<PathBuf>,
	/// Everything a run writes: the JSON, the TypeScript, the queries module and each emitter's
	/// directory.
	pub outputs: Vec<PathBuf>,
}

/// The files `compile` for `modules` reads and writes. `out` is `--out`.
///
/// # Errors
///
/// Fails when an output in `typegen` is misconfigured.
pub fn watch_set(
	project: &ProjectConfig,
	folder: &str,
	modules: &[Module],
	typegen: &TypegenConfig,
	out: Option<&Path>,
) -> Result<WatchSet> {
	let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
	let schema = modules
		.iter()
		.map(|module| project.layout_for(folder, module).schema_dir())
		.map(|dir| relative(&dir, &cwd))
		.collect();
	let config_file = ProjectConfig::discover(&cwd)
		.map(|found| relative(&found, &cwd))
		.unwrap_or_else(|| PathBuf::from(crate::project::CONFIG_FILE_NAME));
	let config = vec![config_file, PathBuf::from(".env"), PathBuf::from(".env.local")];
	let queries = typegen.queries_config()?;
	let (query_files, query_sources) = match &queries {
		Some(queries) => (queries.files.clone(), queries.sources.clone()),
		None => (Vec::new(), Vec::new()),
	};
	let mut outputs = vec![
		out.map(Path::to_path_buf)
			.or(typegen.json_path()?)
			.unwrap_or_else(|| crate::constants::typegen_output_path(folder)),
	];
	outputs.extend(typegen.typescript_path()?.map(|path| crate::typegen::typescript_file(&path)));
	outputs.extend(typegen.queries_typescript()?);
	outputs.extend(typegen.emitters()?.into_iter().map(|emitter| emitter.out));
	Ok(WatchSet {
		schema,
		config,
		query_files,
		query_sources,
		outputs,
	})
}

/// `path` relative to `cwd` when it is below it, or above it by whole directories, else as it
/// is. A relative `path` is returned unchanged.
fn relative(path: &Path, cwd: &Path) -> PathBuf {
	if !path.is_absolute() {
		return path.to_path_buf();
	}
	if let Ok(below) = path.strip_prefix(cwd) {
		return below.to_path_buf();
	}
	// Not the root of the file system: a path there is better left as it is.
	for (up, ancestor) in cwd.ancestors().enumerate().skip(1).filter(|(_, a)| a.parent().is_some())
	{
		if let Ok(below) = path.strip_prefix(ancestor) {
			let mut out: PathBuf = std::iter::repeat_n("..", up).collect();
			out.push(below);
			return out;
		}
	}
	path.to_path_buf()
}

/// A path as `compile --watch-paths` and the watch log spell it: `/`-separated, without a
/// leading `./`.
fn slashed(path: &Path) -> String {
	let text = path.to_string_lossy().replace('\\', "/");
	text.strip_prefix("./").map_or(text.clone(), str::to_string)
}

/// The document `compile --watch-paths` prints.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WatchPaths<'a> {
	version: u32,
	schema: Vec<String>,
	config: Vec<String>,
	query_files: Vec<String>,
	query_sources: Vec<String>,
	query_extensions: &'a [&'a str],
	query_ignored_directories: &'a [&'a str],
	outputs: Vec<String>,
}

impl WatchSet {
	/// The JSON document `compile --watch-paths` prints: version 1, the five sets of paths
	/// relative to the working directory and `/`-separated, and the rule host code is read by
	/// (`queryExtensions`, `queryIgnoredDirectories`; hidden directories are always skipped
	/// too, and `.d.ts` files are not read).
	pub fn to_json(&self) -> String {
		let paths = |paths: &[PathBuf]| paths.iter().map(|path| slashed(path)).collect();
		let document = WatchPaths {
			version: 1,
			schema: paths(&self.schema),
			config: paths(&self.config),
			query_files: paths(&self.query_files),
			query_sources: paths(&self.query_sources),
			query_extensions: SOURCE_EXTENSIONS,
			query_ignored_directories: SKIPPED_DIRECTORIES,
			outputs: paths(&self.outputs),
		};
		serde_json::to_string_pretty(&document).expect("a document of strings serializes")
	}
}

#[cfg(feature = "watch")]
pub use watching::watch;

#[cfg(feature = "watch")]
mod watching {
	use std::path::{Path, PathBuf};
	use std::sync::Arc;

	use anyhow::{Result, anyhow};

	use super::{CompileArgs, Plan, WatchSet, is_source, slashed};
	use crate::codegen::engine;
	use crate::codegen::section::{self, Collected};
	use crate::codegen::watch_cache::QueryCache;
	use crate::compile::{self, Built, Compiled, collect_module};
	use crate::watch::{self, Next, Root, Watcher};

	/// What a changed file asks for.
	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub(super) enum Change {
		/// The schema, `surrealkit.toml` or `.env*`: start again from a fresh engine.
		Schema,
		/// A query: type it against the engine that is there.
		Query,
	}

	/// A [`WatchSet`] with every path resolved, to say what a changed file is.
	#[derive(Debug)]
	pub(super) struct Matcher {
		cwd: PathBuf,
		schema: Vec<PathBuf>,
		config: Vec<PathBuf>,
		query_files: Vec<PathBuf>,
		query_sources: Vec<PathBuf>,
		outputs: Vec<PathBuf>,
	}

	impl Matcher {
		pub(super) fn new(set: &WatchSet) -> Self {
			let all = |paths: &[PathBuf]| paths.iter().map(|path| watch::resolve(path)).collect();
			Self {
				cwd: watch::resolve(Path::new(".")),
				schema: all(&set.schema),
				config: all(&set.config),
				query_files: all(&set.query_files),
				query_sources: all(&set.query_sources),
				outputs: all(&set.outputs),
			}
		}

		/// The roots to watch: every input of a run. Outputs are not watched, and a file that is
		/// one is not a change wherever it is.
		fn roots(set: &WatchSet) -> Vec<Root> {
			let mut roots: Vec<Root> = set.config.iter().cloned().map(Root::File).collect();
			for path in set.schema.iter().chain(&set.query_files).chain(&set.query_sources) {
				roots.push(Root::Tree(path.clone()));
			}
			roots
		}

		/// What a change to `path` (resolved) asks for, or `None` when it is not an input: an
		/// output of the run, a temporary file an output is written through, or a file
		/// nothing reads.
		pub(super) fn classify(&self, path: &Path) -> Option<Change> {
			let temporary = path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name.contains(".surrealkit-tmp-"));
			if temporary || self.outputs.iter().any(|output| path.starts_with(output)) {
				return None;
			}
			let surql = path.extension().is_some_and(|extension| extension == "surql");
			if self.config.iter().any(|file| path == file)
				|| (surql && self.schema.iter().any(|dir| path.starts_with(dir)))
			{
				return Some(Change::Schema);
			}
			if surql && self.query_files.iter().any(|dir| path.starts_with(dir)) {
				return Some(Change::Query);
			}
			let host = self.query_sources.iter().any(|root| match path.strip_prefix(root) {
				Ok(below) if below.as_os_str().is_empty() => {
					path.file_name().is_some_and(|name| is_source(Path::new(name)))
				}
				Ok(below) => is_source(below),
				Err(_) => false,
			});
			host.then_some(Change::Query)
		}

		/// `path` as the log spells it: relative to the working directory.
		pub(super) fn shown(&self, path: &Path) -> String {
			slashed(path.strip_prefix(&self.cwd).unwrap_or(path))
		}
	}

	/// What started a regeneration.
	struct Trigger {
		change: Option<Change>,
		/// The first changed path, and how many others changed with it.
		first: Option<(String, usize)>,
	}

	impl Trigger {
		fn label(&self) -> String {
			let what = match self.change {
				None => return "initial run".to_string(),
				Some(Change::Schema) => "schema changed",
				Some(Change::Query) => "queries changed",
			};
			match &self.first {
				Some((path, 0)) => format!("{what} ({path})"),
				Some((path, more)) => format!("{what} ({path} and {more} more)"),
				None => what.to_string(),
			}
		}
	}

	/// The state a watch keeps between runs.
	struct Run<'a> {
		args: &'a CompileArgs,
		/// The configuration of the last load that parsed.
		plan: Option<Plan>,
		/// The engine of the last schema that compiled, and what it produced.
		built: Option<Built>,
		cache: QueryCache,
	}

	/// What a regeneration did, for the log.
	struct Done {
		/// How many queries the engine was asked about, and how many are typed in all, when
		/// `[typegen.queries]` is configured.
		typed: Option<(usize, usize)>,
		written: Vec<PathBuf>,
	}

	impl Run<'_> {
		/// Read the configuration again. A configuration that does not parse leaves the previous
		/// one in place, and is the error when there is none.
		fn reload(&mut self) -> Result<()> {
			match Plan::load(self.args, false, false) {
				Ok(plan) => {
					self.plan = Some(plan);
					Ok(())
				}
				Err(err) if self.plan.is_some() => {
					log::error!(
						"compile: the configuration did not reload; keeping the previous one: \
						 {err:#}"
					);
					Ok(())
				}
				Err(err) => Err(err),
			}
		}

		fn watch_set(&self) -> WatchSet {
			self.plan
				.as_ref()
				.and_then(|plan| plan.watch_set(&self.args.folder).ok())
				.unwrap_or_else(|| WatchSet {
					schema: Vec::new(),
					config: vec![
						PathBuf::from(crate::project::CONFIG_FILE_NAME),
						PathBuf::from(".env"),
						PathBuf::from(".env.local"),
					],
					query_files: Vec::new(),
					query_sources: Vec::new(),
					outputs: Vec::new(),
				})
		}

		/// Regenerate the outputs for `trigger` and log what happened, in one line. A failure
		/// is the line, and leaves the previous outputs as they are.
		async fn regenerate(&mut self, trigger: &Trigger) {
			let label = trigger.label();
			let done = match trigger.change {
				Some(Change::Query) => self.queries().await,
				_ => self.schema().await,
			};
			match done {
				Ok(done) => {
					let typed = match (done.typed, trigger.change) {
						(Some((asked, total)), Some(Change::Query)) => {
							format!("{asked} of {total} re-typed, ")
						}
						(Some((_, total)), _) => {
							format!(
								"{total} {} typed, ",
								if total == 1 {
									"query"
								} else {
									"queries"
								}
							)
						}
						(None, _) => String::new(),
					};
					let wrote = if done.written.is_empty() {
						"nothing changed on disk".to_string()
					} else {
						let names: Vec<String> = done.written.iter().map(|p| slashed(p)).collect();
						format!("wrote {}", names.join(", "))
					};
					log::info!("compile: {label}: {typed}{wrote}");
				}
				Err(err) => log::error!("compile: {label}: {err:#}"),
			}
		}

		/// The schema, the configuration or a template variable changed: read the configuration
		/// again, compile from a fresh engine, and type every query.
		async fn schema(&mut self) -> Result<Done> {
			self.reload()?;
			let plan = self.plan.as_ref().ok_or_else(|| anyhow!("no configuration"))?;
			let modules = plan
				.modules
				.iter()
				.map(|module| collect_module(&plan.project, &self.args.folder, module))
				.collect::<Result<Vec<_>>>()?;
			let built = compile::build(&modules, &plan.compile).await?;
			// The types depend on the schema: nothing typed before is kept.
			self.built = Some(built);
			self.cache.clear();
			self.finish().await
		}

		/// A query changed: type what has no answer yet against the engine of the last schema
		/// that compiled.
		async fn queries(&mut self) -> Result<Done> {
			if self.built.is_none() {
				return Err(anyhow!(
					"the schema has to compile first, because queries are typed against it; \
					 the previous outputs are kept"
				));
			}
			self.finish().await
		}

		/// Collect the queries, ask the engine about those without an answer, assemble the
		/// document, and write the outputs.
		async fn finish(&mut self) -> Result<Done> {
			let plan = self.plan.as_ref().ok_or_else(|| anyhow!("no configuration"))?;
			let built = self.built.as_ref().ok_or_else(|| anyhow!("no compiled schema"))?;
			let mut document = built.compiled.document.clone();
			let mut typed = None;
			if let Some(config) = &plan.queries {
				let collected: Collected = section::collect(".", config)?;
				self.cache.retain(&collected);
				let untyped = self.cache.untyped(&collected);
				let before = self.cache.len();
				if !untyped.is_empty()
					&& let Some(answers) = engine::types_document(&built.db, &untyped).await?
				{
					self.cache.insert(answers);
				}
				let asked = self.cache.len() - before;
				let types = if collected.inputs().is_empty() {
					None
				} else {
					self.cache.document(&collected)
				};
				typed = Some((asked, types.as_ref().map_or(0, |types| types.queries.len())));
				section::apply(&mut document, &collected, types.as_ref());
			}
			let compiled = Compiled {
				document,
				v1: built.compiled.v1.clone(),
				v1_diagnostics: built.compiled.v1_diagnostics.clone(),
			};
			let written = compile::emit(
				&compiled,
				&self.args.folder,
				&plan.outputs,
				Some(log::Level::Debug),
			)?;
			Ok(Done {
				typed,
				written,
			})
		}
	}

	/// `compile --watch`: compile, then regenerate after every change to a file a compile
	/// reads, until Ctrl-C. Returns the exit code, 0 after Ctrl-C.
	///
	/// A first compile that fails is reported and the watch starts anyway, so the file can be
	/// fixed. A later one that fails is reported and leaves the previous outputs. Only the file
	/// watcher failing ends the command early.
	///
	/// # Errors
	///
	/// Fails when the file watcher cannot start or stops.
	pub async fn watch(args: &CompileArgs) -> Result<i32> {
		let mut run = Run {
			args,
			plan: None,
			built: None,
			cache: QueryCache::new(),
		};
		if let Err(err) = run.reload() {
			log::error!("compile: {err:#}");
		}
		let mut set = run.watch_set();
		let mut matcher = Arc::new(Matcher::new(&set));
		let mut watcher = Watcher::start(&Matcher::roots(&set), {
			let matcher = Arc::clone(&matcher);
			move |path| matcher.classify(path).is_some()
		})?;
		log::info!("compile: watching for changes; Ctrl-C to stop");

		let mut trigger = Trigger {
			change: None,
			first: None,
		};
		loop {
			// Without a configuration there is nothing to compile until one parses.
			if (run.plan.is_some() || trigger.change.is_some())
				&& watcher.interruptible(run.regenerate(&trigger)).await.is_none()
			{
				return Ok(0);
			}
			let now = run.watch_set();
			if now != set {
				matcher = Arc::new(Matcher::new(&now));
				let for_filter = Arc::clone(&matcher);
				watcher.retarget(&Matcher::roots(&now), move |path| {
					for_filter.classify(path).is_some()
				})?;
				set = now;
			}
			let batch = loop {
				match watcher.next().await? {
					Next::Interrupted => return Ok(0),
					Next::Changed(batch) => {
						for error in &batch.errors {
							log::error!("compile: the file watcher reported: {error}");
						}
						if !batch.paths.is_empty() {
							break batch;
						}
					}
				}
			};
			let changes: Vec<(Change, &PathBuf)> = batch
				.paths
				.iter()
				.filter_map(|path| matcher.classify(path).map(|change| (change, path)))
				.collect();
			let change = if changes.iter().any(|(change, _)| *change == Change::Schema) {
				Change::Schema
			} else {
				Change::Query
			};
			let named: Vec<&PathBuf> =
				changes.iter().filter(|(of, _)| *of == change).map(|(_, path)| *path).collect();
			trigger = Trigger {
				change: Some(change),
				first: named.first().map(|first| (matcher.shown(first), named.len() - 1)),
			};
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[cfg(feature = "watch")]
	#[test]
	fn host_code_is_read_by_extension_without_declarations_or_build_output() {
		for yes in ["a.ts", "A.SVELTE", "lib/a.mjs", "src/routes/+page.svelte", ".hidden.ts"] {
			assert!(is_source(Path::new(yes)), "{yes}");
		}
		for no in [
			"types.d.ts",
			"readme.md",
			"noext",
			"node_modules/pkg/a.ts",
			"lib/dist/a.js",
			".svelte-kit/a.ts",
			"src/.cache/a.ts",
		] {
			assert!(!is_source(Path::new(no)), "{no}");
		}
	}

	#[test]
	fn a_path_is_relative_to_the_working_directory_or_above_it() {
		let cwd = Path::new("/work/app");
		assert_eq!(relative(Path::new("/work/app/db/schema"), cwd), PathBuf::from("db/schema"));
		assert_eq!(
			relative(Path::new("/work/surrealkit.toml"), cwd),
			PathBuf::from("../surrealkit.toml")
		);
		assert_eq!(relative(Path::new("db/schema"), cwd), PathBuf::from("db/schema"));
		assert_eq!(relative(Path::new("/elsewhere/x"), cwd), PathBuf::from("/elsewhere/x"));
		assert_eq!(slashed(Path::new("./src/a.ts")), "src/a.ts");
	}

	#[cfg(feature = "watch")]
	#[test]
	fn a_changed_file_is_a_schema_change_a_query_change_or_nothing() {
		use super::watching::{Change, Matcher};
		let dir = tempfile::TempDir::new().unwrap();
		let root = crate::watch::resolve(dir.path());
		let at = |relative: &str| root.join(relative);
		let set = WatchSet {
			schema: vec![at("database/schema"), at("database/modules/auth/schema")],
			config: vec![at("surrealkit.toml"), at(".env"), at(".env.local")],
			query_files: vec![at("database/queries")],
			query_sources: vec![at("src"), at("app/page.ts")],
			outputs: vec![
				at("generated/schema.json"),
				at("src/generated/queries.ts"),
				at("src/generated/surreal"),
			],
		};
		let matcher = Matcher::new(&set);
		let cases = [
			("database/schema/person.surql", Some(Change::Schema)),
			("database/modules/auth/schema/a/b.surql", Some(Change::Schema)),
			("database/schema/notes.md", None),
			("surrealkit.toml", Some(Change::Schema)),
			(".env", Some(Change::Schema)),
			(".env.local", Some(Change::Schema)),
			(".env.production", None),
			("database/queries/people.surql", Some(Change::Query)),
			("database/queries/people.sql", None),
			("src/routes/+page.svelte", Some(Change::Query)),
			("src/lib/types.d.ts", None),
			("src/node_modules/x/a.ts", None),
			("app/page.ts", Some(Change::Query)),
			("app/other.ts", None),
			// What a run writes never counts, even inside a watched directory.
			("generated/schema.json", None),
			("src/generated/queries.ts", None),
			("src/generated/surreal/index.ts", None),
			("src/generated/queries.surrealkit-tmp-1-0.ts", None),
			("database/schema/person.surrealkit-tmp-1-0.surql", None),
			("unrelated/a.ts", None),
		];
		for (path, expected) in cases {
			assert_eq!(matcher.classify(&at(path)), expected, "{path}");
		}
	}
}
