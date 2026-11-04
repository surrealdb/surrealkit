//! `host_queries::scan` over the fixture hosts under `tests/fixtures/host_queries`.

#![cfg(feature = "host-queries")]

use std::fs;
use std::path::{Path, PathBuf};

use surrealkit::host_queries::{
	DEFAULT_METHODS, FoundQuery, HostQuerySource, Queries, scan, scan_with,
};

fn hosts() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/host_queries")
}

fn texts(found: &[FoundQuery]) -> Vec<(String, &str)> {
	found
		.iter()
		.map(|found| {
			let file = found.file.file_name().and_then(|name| name.to_str()).unwrap_or_default();
			(file.to_string(), found.query.text.as_str())
		})
		.collect()
}

#[test]
fn scan_reads_every_host_kind_in_file_order() {
	let found = scan(&[hosts()], DEFAULT_METHODS).expect("scan succeeds");
	let owned = texts(&found);
	let seen: Vec<(&str, &str)> = owned.iter().map(|(file, text)| (file.as_str(), *text)).collect();

	// Files sort by name (`Comp.vue` < `Page.svelte` < `app.ts` < `page.astro` < `widget.tsx`),
	// queries by position within a file. `types.d.ts` and `.hidden/` are skipped.
	assert_eq!(
		seen,
		[
			("Comp.vue", "SELECT * FROM comment"),
			("Page.svelte", "SELECT * FROM post"),
			("Page.svelte", "SELECT * FROM post:1"),
			("app.ts", "SELECT name FROM person"),
			("app.ts", "SELECT * FROM person WHERE age >= $age"),
			("app.ts", "SELECT * FROM person WHERE age > $__host0"),
			("page.astro", "SELECT * FROM comment"),
			("page.astro", "SELECT * FROM comment WHERE flagged"),
			("widget.tsx", "SELECT * FROM widget"),
		]
	);
}

#[test]
fn scan_with_the_defaults_reads_bare_calls_and_component_attributes() {
	let found = scan_with(&[hosts()], &Queries::DEFAULT).expect("scan succeeds");
	let owned = texts(&found);
	let seen: Vec<(&str, &str)> = owned.iter().map(|(file, text)| (file.as_str(), *text)).collect();

	assert_eq!(
		seen,
		[
			("Bindings.svelte", "SELECT * FROM person"),
			("Bindings.svelte", "SELECT * FROM person WHERE age > $__host0"),
			("Bindings.svelte", "SELECT * FROM post"),
			("Bindings.svelte", "SELECT * FROM tag"),
			("Bindings.svelte", "SELECT * FROM note WHERE body = 'a > b'"),
			("Bindings.vue", "SELECT * FROM person WHERE age > 18"),
			("Bindings.vue", "SELECT * FROM post WHERE author = $__host0"),
			("Bindings.vue", "SELECT * FROM tag"),
			("Bindings.vue", "SELECT * FROM note WHERE body = 'a > b'"),
			("Bindings.vue", "SELECT * FROM comment"),
			("Comp.vue", "SELECT * FROM comment"),
			("Page.svelte", "SELECT * FROM post"),
			("Page.svelte", "SELECT * FROM post:1"),
			("app.ts", "SELECT name FROM person"),
			("app.ts", "SELECT * FROM person WHERE age >= $age"),
			("app.ts", "SELECT * FROM person WHERE age > $__host0"),
			("bindings.astro", "SELECT * FROM person"),
			("bindings.astro", "SELECT * FROM post WHERE title = \"a\""),
			("bindings.astro", "SELECT * FROM tag"),
			("bindings.astro", "SELECT * FROM note WHERE owner = $__host0"),
			("bindings.astro", "SELECT * FROM log WHERE body = 'a > b'"),
			("bindings.jsx", "SELECT * FROM person"),
			("bindings.jsx", "SELECT * FROM post"),
			("bindings.jsx", "SELECT * FROM tag WHERE id = $__host0"),
			("bindings.jsx", "SELECT * FROM log WHERE body = 'a > b'"),
			("bindings.tsx", "SELECT * FROM person"),
			("bindings.tsx", "SELECT * FROM post WHERE age > 18"),
			("bindings.tsx", "SELECT * FROM tag"),
			("bindings.tsx", "SELECT * FROM note"),
			("bindings.tsx", "SELECT * FROM log WHERE body = 'a > b'"),
			("page.astro", "SELECT * FROM comment"),
			("page.astro", "SELECT * FROM comment WHERE flagged"),
			("widget.tsx", "SELECT * FROM widget"),
		]
	);
}

#[test]
fn scan_with_records_where_each_query_was_found() {
	let found = scan_with(&[hosts().join("Bindings.svelte")], &Queries::DEFAULT).expect("scan");
	let sources: Vec<HostQuerySource> =
		found.iter().map(|found| found.query.source.clone()).collect();
	let attribute = |component: &str| HostQuerySource::Attribute {
		component: component.to_string(),
		attribute: "q".to_string(),
	};
	assert_eq!(
		sources,
		[
			HostQuerySource::Function("createQuery".to_string()),
			attribute("Query"),
			attribute("LiveQuery"),
			attribute("Query"),
			attribute("Query"),
		]
	);
}

#[test]
fn scan_with_positions_index_the_host_file() {
	let found = scan_with(&[hosts()], &Queries::DEFAULT).expect("scan succeeds");
	for found in &found {
		let source = fs::read_to_string(&found.file).expect("fixture is UTF-8");
		let query = &found.query;
		let contents = &source[query.host_range.clone()];
		let line = source.lines().nth(query.line as usize - 1).expect("line exists");
		assert_eq!(
			line.chars().nth(query.column as usize - 1),
			contents.chars().next(),
			"{}:{}:{}",
			found.file.display(),
			query.line,
			query.column,
		);
		for substitution in &query.substitutions {
			let hole = &source[substitution.host_range.clone()];
			assert!(hole.starts_with(['{', '$']) && hole.ends_with('}'), "{hole}");
		}
	}
}

#[test]
fn scan_positions_index_the_host_file() {
	let found = scan(&[hosts()], DEFAULT_METHODS).expect("scan succeeds");
	for found in &found {
		let source = fs::read_to_string(&found.file).expect("fixture is UTF-8");
		let query = &found.query;
		// Every query without substitutions is a verbatim copy of its host bytes.
		if query.keyable() {
			assert_eq!(&source[query.host_range.clone()], query.text, "{}", found.file.display());
		}
		// The line and column name the first char of the contents.
		let line = source.lines().nth(query.line as usize - 1).expect("line exists");
		assert_eq!(
			line.chars().nth(query.column as usize - 1),
			query.text.chars().next(),
			"{}:{}:{}",
			found.file.display(),
			query.line,
			query.column,
		);
	}

	let svelte = found.iter().find(|found| found.file.ends_with("Page.svelte")).expect("svelte");
	assert_eq!((svelte.query.line, svelte.query.column), (2, 43));
}

#[test]
fn scan_marks_substitution_queries_unkeyable() {
	let found = scan(&[hosts().join("app.ts")], DEFAULT_METHODS).expect("scan succeeds");
	let keyable: Vec<bool> = found.iter().map(|found| found.query.keyable()).collect();
	assert_eq!(keyable, [true, true, false]);
}

#[test]
fn scan_applies_a_custom_method_list() {
	let dir = tempfile::tempdir().expect("tempdir");
	fs::write(dir.path().join("a.ts"), "db.run(\"R\"); db.query(\"Q\");").expect("write");

	let default = scan(&[dir.path().to_path_buf()], DEFAULT_METHODS).expect("scan");
	assert_eq!(texts(&default).len(), 1);
	let both = scan(&[dir.path().to_path_buf()], &["run", "query"]).expect("scan");
	assert_eq!(both.iter().map(|found| found.query.text.as_str()).collect::<Vec<_>>(), ["R", "Q"]);
}

#[test]
fn scan_skips_build_output_hidden_directories_and_declarations() {
	let dir = tempfile::tempdir().expect("tempdir");
	let root = dir.path();
	let query = "db.query(\"SELECT 1\");\n";
	for skipped in [
		"node_modules/pkg/index.ts",
		"dist/a.js",
		"build/a.ts",
		"target/a.ts",
		".svelte-kit/a.ts",
		".next/a.ts",
		".nuxt/a.ts",
		".astro/a.ts",
		".output/a.ts",
		".git/a.ts",
		"src/types.d.ts",
		"src/notes.md",
	] {
		let path = root.join(skipped);
		fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
		fs::write(path, query).expect("write");
	}
	fs::create_dir_all(root.join("src/deep")).expect("mkdir");
	fs::write(root.join("src/deep/z.mjs"), query).expect("write");
	fs::write(root.join("src/b.ts"), query).expect("write");
	fs::write(root.join("src/a.cts"), query).expect("write");

	let found = scan(&[root.to_path_buf()], DEFAULT_METHODS).expect("scan");
	let files: Vec<PathBuf> = found
		.iter()
		.map(|found| found.file.strip_prefix(root).expect("under root").into())
		.collect();
	assert_eq!(
		files,
		[PathBuf::from("src/a.cts"), PathBuf::from("src/b.ts"), PathBuf::from("src/deep/z.mjs"),]
	);
}

#[test]
fn scan_accepts_files_and_dedupes_overlapping_roots() {
	let file = hosts().join("widget.tsx");
	let found = scan(&[hosts(), file.clone(), file], DEFAULT_METHODS).expect("scan");
	let widgets = found.iter().filter(|found| found.file.ends_with("widget.tsx")).count();
	assert_eq!(widgets, 1);
}

#[test]
fn scan_skips_files_that_are_not_utf8() {
	let dir = tempfile::tempdir().expect("tempdir");
	fs::write(dir.path().join("bad.ts"), [0xff, 0xfe, b'd', b'b']).expect("write");
	fs::write(dir.path().join("good.ts"), "db.query(\"SELECT 1\");").expect("write");

	let found = scan(&[dir.path().to_path_buf()], DEFAULT_METHODS).expect("scan");
	assert_eq!(texts(&found), [("good.ts".to_string(), "SELECT 1")]);
}

#[test]
fn scan_reads_crlf_files() {
	let dir = tempfile::tempdir().expect("tempdir");
	fs::write(
		dir.path().join("crlf.ts"),
		"const a = 1;\r\nconst q = db.query(`\r\nSELECT 1\r\n`);\r\n",
	)
	.expect("write");

	let found = scan(&[dir.path().to_path_buf()], DEFAULT_METHODS).expect("scan");
	assert_eq!(found.len(), 1);
	assert_eq!(found[0].query.text, "\nSELECT 1\n");
	assert_eq!(found[0].query.line, 2);
}

#[test]
fn scan_names_a_missing_root() {
	let missing = hosts().join("does-not-exist");
	let error = scan(&[missing], DEFAULT_METHODS).expect_err("missing root fails");
	assert!(error.to_string().contains("does-not-exist"), "{error}");
}
