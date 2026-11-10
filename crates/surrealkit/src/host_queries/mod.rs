//! Finds the SurrealQL queries host code sends to the engine.
//!
//! A query is a **string or template literal** in one of three places:
//!
//! - the first argument of a method call such as `db.query("SELECT * FROM person")`. The callee is
//!   matched on its method name alone, so `db.query(…)`, `this.db.query(…)` and `tx.query(…)` all
//!   count, while a bare `query(…)` or a call whose first argument is not a literal does not;
//! - the argument of a call to a configured function such as `createQuery(…)`, `useQuery(…)` or
//!   `preload(db, …)`, bare or as a member (`svelte.createQuery(…)`). Each function says which
//!   argument carries the query ([`QueryFunction::arg`]), the first by default;
//! - the query attribute of a configured component in markup (`<Query q="…" />`), in JSX and in
//!   Svelte, Vue and Astro templates.
//!
//! [`Queries`] names what to look for and [`HostQuery::source`] records which form matched.
//!
//! Only a literal survives into the type system, so only a literal can key an entry of the
//! generated registry: [`HostQuery::text`] is the literal's JavaScript runtime value, which is the
//! string `tsc` compares against. Each `${…}` substitution of a template becomes a `$__hostN`
//! parameter, and the span map on [`HostQuery`] keeps every byte of the extracted text addressable
//! in the host file.
//!
//! Plain TypeScript, JavaScript, TSX and JSX files are parsed directly. Svelte, Vue and Astro files
//! are unwrapped to their script blocks first, with offsets shifted back into the whole file, and
//! their templates are read by a small markup scanner.

mod frameworks;
mod literal;
mod markup;
pub mod python;
mod typescript;

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use walkdir::WalkDir;

/// The prefix of the parameter names generated for `${…}` substitutions.
///
/// The leading underscores keep `__host0` from colliding with a parameter the user wrote
/// themselves.
pub const HOST_PARAM_PREFIX: &str = "__host";

/// The method names that carry a query when none are configured.
pub const DEFAULT_METHODS: &[&str] = &["query"];

/// The functions that carry a query when none are configured: the first argument of
/// `createQuery`, `createLive`, `useQuery` and `useLive`, and the second of `preload`, which takes
/// the client first.
pub const DEFAULT_FUNCTIONS: &[QueryFunction<'static>] = &[
	QueryFunction::first("createQuery"),
	QueryFunction::first("createLive"),
	QueryFunction::first("useQuery"),
	QueryFunction::first("useLive"),
	QueryFunction {
		name: "preload",
		arg: 1,
	},
];

/// The markup components whose attribute carries a query when none are configured.
pub const DEFAULT_COMPONENTS: &[&str] = &["Query", "LiveQuery"];

/// The attribute of a component that carries its query when none is configured.
pub const DEFAULT_ATTRIBUTE: &str = "q";

/// A function whose call carries a query, and which argument it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryFunction<'a> {
	/// The function's name, compared exactly.
	pub name: &'a str,
	/// The 0-based index of the argument that carries the query. Comments between arguments do
	/// not count. A call with fewer arguments, or a non-literal at this index, is not a query.
	pub arg: usize,
}

impl<'a> QueryFunction<'a> {
	/// A function whose first argument (index 0) is the query.
	pub const fn first(name: &'a str) -> Self {
		Self {
			name,
			arg: 0,
		}
	}
}

/// What to look for: the names that mark a literal as a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Queries<'a> {
	/// Method names: `receiver.method("…")` carries a query. A bare `method("…")` does not.
	pub methods: &'a [&'a str],
	/// Functions: `function(…)` and `receiver.function(…)` carry a query in the argument each
	/// names. A method of the same name wins on a member call, and its query is the first
	/// argument.
	pub functions: &'a [QueryFunction<'a>],
	/// Markup component names, compared exactly. A member name such as `<Kit.Query>` matches on
	/// its last segment.
	pub components: &'a [&'a str],
	/// The attribute of a component that carries the query.
	pub attribute: &'a str,
}

impl Queries<'static> {
	/// The defaults of `[typegen.queries]`.
	pub const DEFAULT: Self = Self {
		methods: DEFAULT_METHODS,
		functions: DEFAULT_FUNCTIONS,
		components: DEFAULT_COMPONENTS,
		attribute: DEFAULT_ATTRIBUTE,
	};
}

/// Extensions [`scan`] reads.
pub const SOURCE_EXTENSIONS: &[&str] =
	&["ts", "mts", "cts", "tsx", "js", "mjs", "cjs", "jsx", "svelte", "vue", "astro"];

/// Directories [`scan`] never descends into, besides hidden ones.
pub const SKIPPED_DIRECTORIES: &[&str] = &[
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

/// One query found in a host file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostQuery {
	/// The query as the engine sees it: the literal's runtime string value, with each `${…}`
	/// substitution replaced by `$__hostN`.
	pub text: String,
	/// Byte range of the literal's contents in the host file.
	pub host_range: Range<usize>,
	/// 1-based line of `host_range.start`.
	pub line: u32,
	/// 1-based column of `host_range.start`, counted in chars.
	pub column: u32,
	/// The name that matched: the client method (`query`, `run`, …), the function (`useQuery`) or
	/// the component (`Query`). [`Self::source`] says which, and carries the rest.
	pub method: String,
	/// Which form of [`Queries`] found the query.
	pub source: HostQuerySource,
	/// The `${…}` substitutions that became parameters, in order.
	pub substitutions: Vec<Substitution>,
	/// Map from `text` offsets to host offsets: one entry per copied run or decoded escape.
	segments: Vec<Segment>,
}

/// The place a query was found, and the name that matched there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostQuerySource {
	/// The first argument of `receiver.method("…")`, with the method name.
	Method(String),
	/// The query argument of `function(…)` or `receiver.function(…)`, with the function name.
	Function(String),
	/// The attribute of a markup element: `<Component attribute="…">`. `attribute` is the
	/// configured name, whatever form the host spells it in (`:q` in a Vue template).
	Attribute {
		component: String,
		attribute: String,
	},
}

impl HostQuerySource {
	/// The name that matched: the method, the function or the component.
	pub fn name(&self) -> &str {
		match self {
			Self::Method(name) | Self::Function(name) => name,
			Self::Attribute {
				component,
				..
			} => component,
		}
	}
}

/// A `${…}` substitution rewritten into a parameter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Substitution {
	/// The generated parameter name (`__hostN`), without the `$`.
	pub param: String,
	/// Byte range of the whole `${…}` expression in the host file.
	pub host_range: Range<usize>,
	/// Byte range of the generated `$__hostN` inside [`HostQuery::text`].
	pub text_range: Range<usize>,
}

/// A run of the query text and the host bytes it came from: a verbatim copy (equal lengths), a
/// decoded escape sequence, or a normalised line ending.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Segment {
	text_start: usize,
	text_len: usize,
	host_start: usize,
	host_len: usize,
}

impl Segment {
	fn is_verbatim(&self) -> bool {
		self.text_len == self.host_len
	}
}

impl HostQuery {
	/// A query read from `source[host_range]`, whose line and column are worked out here.
	fn new(
		source: &str,
		text: String,
		host_range: Range<usize>,
		origin: HostQuerySource,
		substitutions: Vec<Substitution>,
		segments: Vec<Segment>,
	) -> Self {
		let (line, column) = line_and_column(source, host_range.start);
		Self {
			text,
			host_range,
			line,
			column,
			method: origin.name().to_string(),
			source: origin,
			substitutions,
			segments,
		}
	}

	/// Whether `text` equals the literal's runtime string, so it can key the type registry.
	///
	/// A query with substitutions only has a runtime value once the host fills them in.
	pub fn keyable(&self) -> bool {
		self.substitutions.is_empty()
	}

	/// Maps a byte offset in `text` to the host byte offset it came from.
	///
	/// An offset inside a decoded escape maps to the start of the escape, and an offset inside a
	/// `$__hostN` parameter maps to the start of its `${…}` expression.
	pub fn host_offset(&self, text_offset: usize) -> usize {
		self.locate(text_offset).start
	}

	/// Maps a host byte offset back into `text`, the inverse of [`Self::host_offset`].
	///
	/// `None` when the offset is not inside copied or decoded query text: outside the literal, or
	/// inside a `${…}` substitution or a line continuation.
	pub fn embed_offset(&self, host_offset: usize) -> Option<usize> {
		self.segments.iter().find_map(|segment| {
			let into = host_offset.checked_sub(segment.host_start)?;
			if into >= segment.host_len {
				return None;
			}
			Some(match segment.is_verbatim() {
				true => segment.text_start + into,
				false => segment.text_start,
			})
		})
	}

	/// Maps a byte range of `text` to the smallest host range covering it.
	///
	/// A range that touches a `$__hostN` parameter widens to the whole `${…}` expression, and one
	/// that touches a decoded escape widens to the whole escape sequence.
	pub fn host_span(&self, range: Range<usize>) -> Range<usize> {
		let first = range.start;
		let last = range.end.max(range.start + 1) - 1;
		let start = self.locate(first).start;
		let mut end = self.locate(last).end;
		// `locate` covers one byte; a multi-byte char copied verbatim ends at its last byte.
		if let Some(segment) = self.segment_at(last).filter(|segment| segment.is_verbatim()) {
			let mut text_end = last + 1;
			while !self.text.is_char_boundary(text_end) {
				text_end += 1;
			}
			end = end.max(segment.host_start + (text_end - segment.text_start));
		}
		start..end.max(start + 1)
	}

	fn segment_at(&self, text_offset: usize) -> Option<&Segment> {
		let at = self.segments.partition_point(|segment| segment.text_start <= text_offset);
		let segment = self.segments.get(at.checked_sub(1)?)?;
		(text_offset < segment.text_start + segment.text_len).then_some(segment)
	}

	/// The host bytes behind the one byte of `text` at `text_offset`.
	fn locate(&self, text_offset: usize) -> Range<usize> {
		if let Some(substitution) = self
			.substitutions
			.iter()
			.find(|substitution| substitution.text_range.contains(&text_offset))
		{
			return substitution.host_range.clone();
		}
		if let Some(segment) = self.segment_at(text_offset) {
			return match segment.is_verbatim() {
				true => {
					let at = segment.host_start + (text_offset - segment.text_start);
					at..at + 1
				}
				false => segment.host_start..segment.host_start + segment.host_len,
			};
		}
		// Past the last byte: the end of the literal's contents.
		let end = self
			.segments
			.last()
			.map_or(self.host_range.start, |segment| segment.host_start + segment.host_len);
		end..end
	}
}

/// The regions of a host source that hold script: the whole file for TypeScript and JavaScript,
/// the script blocks of a Svelte, Vue or Astro file. Each is a byte range of `source` and whether
/// it needs the JSX grammar.
fn script_regions(file_name: &str, source: &str) -> Vec<(Range<usize>, bool)> {
	match extension_of(Path::new(file_name)).as_str() {
		"svelte" | "vue" => frameworks::script_blocks(source)
			.into_iter()
			.map(|block| (block.content, block.tsx))
			.collect(),
		"astro" => {
			let frontmatter = frameworks::astro_frontmatter(source);
			let scripts_from = frontmatter.as_ref().map_or(0, |content| content.end);
			let mut regions: Vec<(Range<usize>, bool)> =
				frontmatter.map(|content| (content, false)).into_iter().collect();
			for block in frameworks::script_blocks(&source[scripts_from..]) {
				let content = block.content.start + scripts_from..block.content.end + scripts_from;
				regions.push((content, block.tsx));
			}
			regions
		}
		"tsx" | "jsx" => vec![(0..source.len(), true)],
		_ => vec![(0..source.len(), false)],
	}
}

/// Which markup syntax a file's template is written in, `None` for a file with no template.
fn markup_flavor(file_name: &str) -> Option<markup::Flavor> {
	match extension_of(Path::new(file_name)).as_str() {
		"svelte" => Some(markup::Flavor::Svelte),
		"vue" => Some(markup::Flavor::Vue),
		"astro" => Some(markup::Flavor::Astro),
		_ => None,
	}
}

/// Extracts the queries passed to the method calls named by `methods`, normally
/// [`DEFAULT_METHODS`], from one host source, dispatching on the extension of `file_name`.
///
/// Bare function calls and markup attributes are not read; [`extract_with`] reads them. Results
/// are in document order.
pub fn extract(file_name: &str, source: &str, methods: &[&str]) -> Vec<HostQuery> {
	extract_with(
		file_name,
		source,
		&Queries {
			methods,
			functions: &[],
			components: &[],
			attribute: DEFAULT_ATTRIBUTE,
		},
	)
}

/// Extracts the queries `queries` names from one host source: method calls, function calls, and
/// component attributes in JSX and in Svelte, Vue and Astro templates. Results are in document
/// order.
pub fn extract_with(file_name: &str, source: &str, queries: &Queries<'_>) -> Vec<HostQuery> {
	let mut found: Vec<HostQuery> = script_regions(file_name, source)
		.into_iter()
		.flat_map(|(content, tsx)| typescript::extract(source, content, tsx, queries))
		.collect();
	if let Some(flavor) = markup_flavor(file_name)
		&& !queries.components.is_empty()
	{
		found.extend(markup::scan(flavor, source, queries).queries);
	}
	found.sort_by_key(|query| (query.host_range.start, query.host_range.end));
	found
}

/// The byte ranges of the comments in one host source, in document order, as the TypeScript grammar
/// finds them: `//` line comments and `/* */` blocks, but not what only looks like one inside a
/// string or template literal.
///
/// A Svelte, Vue or Astro file yields the comments of its script regions, with offsets shifted into
/// the whole file the way [`extract`] shifts query offsets. In a Svelte or Astro template it also
/// yields the JavaScript comments inside `{…}` expressions, in the text and in attribute values.
/// HTML comments (`<!-- -->`) are not included, and neither is anything in a Vue template.
pub fn comments(file_name: &str, source: &str) -> Vec<Range<usize>> {
	let mut found: Vec<Range<usize>> = script_regions(file_name, source)
		.into_iter()
		.flat_map(|(content, tsx)| typescript::comments(source, content, tsx))
		.collect();
	if let Some(flavor) = markup_flavor(file_name) {
		for expression in markup::scan(flavor, source, &Queries::DEFAULT).expressions {
			found.extend(typescript::comments(source, expression, true));
		}
	}
	found.sort_by_key(|range| (range.start, range.end));
	found.dedup();
	found
}

/// A query and the file it was found in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoundQuery {
	pub file: PathBuf,
	pub query: HostQuery,
}

/// Extracts the queries passed to the method calls named by `methods` from every source file under
/// `roots`, as [`extract`] does for one source.
///
/// A root is a file or a directory, walked recursively. Files with a TypeScript, JavaScript,
/// Svelte, Vue or Astro extension are read; `.d.ts` files, hidden directories and build output
/// (`node_modules`, `dist`, `build`, `target`, `.svelte-kit`, `.next`, `.nuxt`, `.astro`,
/// `.output`) are skipped, and so are symlinks. A file that is not UTF-8 is skipped with a
/// warning. Results are sorted by file, then position; a file reachable from several roots is
/// read once.
///
/// # Errors
///
/// Fails when a root does not exist, or a source file cannot be read.
pub fn scan(roots: &[PathBuf], methods: &[&str]) -> Result<Vec<FoundQuery>> {
	scan_with(
		roots,
		&Queries {
			methods,
			functions: &[],
			components: &[],
			attribute: DEFAULT_ATTRIBUTE,
		},
	)
}

/// Extracts the queries `queries` names from every source file under `roots`, with [`scan`]'s
/// rules for what is read and in what order.
///
/// # Errors
///
/// Fails when a root does not exist, or a source file cannot be read.
pub fn scan_with(roots: &[PathBuf], queries: &Queries<'_>) -> Result<Vec<FoundQuery>> {
	let mut files = BTreeSet::new();
	for root in roots {
		if !root.exists() {
			bail!("path does not exist: {}", root.display());
		}
		let walker = WalkDir::new(root)
			.follow_links(false)
			.sort_by_file_name()
			.into_iter()
			.filter_entry(|entry| entry.depth() == 0 || !is_skipped_directory(entry));
		for entry in walker {
			let entry = match entry {
				Ok(entry) => entry,
				Err(error) => {
					log::warn!("skipping unreadable path under {}: {error}", root.display());
					continue;
				}
			};
			// A root that is itself a file is judged by its own name.
			let relative = match entry.depth() {
				0 => Path::new(entry.file_name()),
				_ => entry.path().strip_prefix(root).unwrap_or(entry.path()),
			};
			if entry.file_type().is_file() && is_source(relative) {
				files.insert(entry.into_path());
			}
		}
	}

	let mut found = Vec::new();
	for file in files {
		let bytes =
			std::fs::read(&file).with_context(|| format!("failed to read {}", file.display()))?;
		let Ok(source) = String::from_utf8(bytes) else {
			log::warn!("skipping {}: not valid UTF-8", file.display());
			continue;
		};
		let name = file.to_string_lossy();
		for query in extract_with(&name, &source, queries) {
			found.push(FoundQuery {
				file: file.clone(),
				query,
			});
		}
	}
	// Files arrive sorted and `extract` yields document order, so this is already the order
	// `(file, host_range.start)` asks for; the sort keeps the guarantee local to this function.
	found.sort_by(|a, b| {
		(&a.file, a.query.host_range.start).cmp(&(&b.file, b.query.host_range.start))
	});
	Ok(found)
}

fn is_skipped_directory(entry: &walkdir::DirEntry) -> bool {
	if !entry.file_type().is_dir() {
		return false;
	}
	is_skipped_name(&entry.file_name().to_string_lossy())
}

/// Whether a directory of this name is one [`scan`] never descends into: hidden, or one of
/// [`SKIPPED_DIRECTORIES`].
fn is_skipped_name(name: &str) -> bool {
	name.starts_with('.') || SKIPPED_DIRECTORIES.contains(&name)
}

/// Whether [`scan`] reads the file at `path`, which is relative to the root being scanned: a
/// [`SOURCE_EXTENSIONS`] file that is not a `.d.ts` declaration, with no directory between the
/// root and the file that is hidden or one of [`SKIPPED_DIRECTORIES`]. A path given relative to
/// another directory is judged by every directory it names, so pass it relative to the root.
pub fn is_source(path: &Path) -> bool {
	let is_declaration = path
		.file_name()
		.and_then(OsStr::to_str)
		.is_some_and(|name| name.to_ascii_lowercase().ends_with(".d.ts"));
	let in_skipped_directory = path.parent().is_some_and(|parent| {
		parent.components().any(|component| match component {
			std::path::Component::Normal(name) => is_skipped_name(&name.to_string_lossy()),
			_ => false,
		})
	});
	!is_declaration
		&& !in_skipped_directory
		&& SOURCE_EXTENSIONS.contains(&extension_of(path).as_str())
}

/// The lowercased extension of `path`, empty when there is none.
fn extension_of(path: &Path) -> String {
	path.extension().and_then(OsStr::to_str).map(str::to_ascii_lowercase).unwrap_or_default()
}

/// The 1-based line and char-counted column of `offset` in `source`.
fn line_and_column(source: &str, offset: usize) -> (u32, u32) {
	let before = &source[..offset];
	let line = before.bytes().filter(|byte| *byte == b'\n').count() + 1;
	let line_start = before.rfind('\n').map_or(0, |at| at + 1);
	let column = before[line_start..].chars().count() + 1;
	(u32::try_from(line).unwrap_or(u32::MAX), u32::try_from(column).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests;
