//! Golden tests for the per-query TypeScript renderer, over hand-built `QueryEntry` fixtures, and
//! the conversion of the engine's `TypesDocument` into those entries. No engine, no network.

use std::path::{Path, PathBuf};
use std::process::Command;

use surrealkit::codegen::document::TypesDocument;
use surrealkit::codegen::queries::{QueryEntry, from_types_document};
use surrealkit::codegen::query_types::{Report, Request, report};
use surrealkit::codegen::typescript::{DateStyle, HintStyle, RenderOptions, render};
use surrealkit::typegen::document::TypeDef;

const QUERIES: &str = include_str!("fixtures/codegen/queries.json");
const KINDS: &str = include_str!("fixtures/codegen/queries_kinds.json");
/// The `types` the `named` kinds of `KINDS` refer to.
const KIND_TYPES: &str = include_str!("fixtures/codegen/queries_kinds_types.json");
const EMPTY: &str = include_str!("fixtures/codegen/queries_empty.json");
const BLOG: &str = include_str!("fixtures/codegen/blog.json");
const OPEN: &str = include_str!("fixtures/codegen/open.json");
/// Cases of `query_types`: a request and the engine's document for it.
const QUERY_TYPES: &str = include_str!("fixtures/codegen/query_types.json");

fn entries(text: &str) -> Vec<QueryEntry> {
	serde_json::from_str(text).expect("fixture entries read")
}

fn types(text: &str) -> Vec<TypeDef> {
	serde_json::from_str(text).expect("fixture types read")
}

fn render_default(text: &str) -> String {
	render(&entries(text), &[], &RenderOptions::default())
}

fn render_kinds() -> String {
	render(&entries(KINDS), &types(KIND_TYPES), &RenderOptions::default())
}

fn native_unknown() -> RenderOptions {
	RenderOptions {
		dates: DateStyle::Native,
		hints: HintStyle::Unknown,
		..RenderOptions::default()
	}
}

#[test]
fn queries() {
	insta::assert_snapshot!("queries", render_default(QUERIES));
}

#[test]
fn queries_with_native_dates_and_distrusted_hints() {
	insta::assert_snapshot!(
		"queries_native_unknown",
		render(&entries(QUERIES), &[], &native_unknown())
	);
}

#[test]
fn every_v2_kind() {
	insta::assert_snapshot!("kinds", render_kinds());
}

#[test]
fn empty_list() {
	insta::assert_snapshot!("empty", render_default(EMPTY));
}

#[test]
fn fixtures_round_trip_through_the_entry_model() {
	for text in [QUERIES, KINDS, EMPTY] {
		let parsed = entries(text);
		let again = serde_json::to_value(&parsed).expect("serialises");
		let original: serde_json::Value = serde_json::from_str(text).expect("json");
		assert_eq!(again, original);
	}
}

#[test]
fn the_engines_types_document_converts_to_entries() {
	let mut all = Vec::new();
	for (name, text) in [("blog", BLOG), ("open", OPEN)] {
		let doc = TypesDocument::from_json(text).expect("fixture document reads");
		let converted = from_types_document(&doc);
		assert!(converted.unrepresented.is_empty(), "{name}: {:?}", converted.unrepresented);
		all.push((name, converted.queries));
	}
	insta::assert_json_snapshot!("engine_entries", all);
}

/// The `query_types` cases, each reported against its document.
fn query_type_reports() -> Vec<(String, Report)> {
	let cases: Vec<serde_json::Value> = serde_json::from_str(QUERY_TYPES).expect("cases read");
	cases
		.into_iter()
		.map(|case| {
			let name = case["name"].as_str().expect("case name").to_string();
			let request: Request =
				serde_json::from_value(case["request"].clone()).expect("request reads");
			request.validate().expect("request is valid");
			let document =
				TypesDocument::from_json(&case["document"].to_string()).expect("document reads");
			(name, report(&request, &document, &RenderOptions::default()))
		})
		.collect()
}

#[test]
fn query_types_reports() {
	for (name, report) in query_type_reports() {
		insta::assert_json_snapshot!(format!("query_types_{name}_report"), report);
		insta::assert_snapshot!(format!("query_types_{name}_snippet"), report.snippet);
	}
}

#[test]
fn query_types_hints_never_override_the_engine() {
	let reports: std::collections::BTreeMap<_, _> = query_type_reports().into_iter().collect();
	let params = |name: &str| {
		reports[name]
			.params
			.iter()
			.map(|p| (p.name.as_str(), p.kind.as_deref(), p.source))
			.collect::<Vec<_>>()
	};
	use surrealkit::codegen::query_types::ParamSource::{Declared, Engine, Example};
	assert_eq!(
		params("declared_hint"),
		[("who", Some("record<person>"), Declared), ("since", Some("datetime"), Engine)]
	);
	assert_eq!(params("example_hint")[0], ("who", Some("string"), Example));
	assert_eq!(params("disagreeing_hint"), [("min", Some("int"), Engine)]);
	assert_eq!(reports["unused_hint"].notes.len(), 1);
	assert_eq!(reports["disagreeing_hint"].notes.len(), 1);
	assert!(reports["projection"].notes.is_empty());
}

#[test]
fn query_types_report_omits_what_is_absent() {
	let reports: std::collections::BTreeMap<_, _> = query_type_reports().into_iter().collect();
	let json = serde_json::to_value(&reports["untyped"]).expect("serialises");
	assert!(json.get("notes").is_none() && json.get("findings").is_none(), "{json}");
	assert!(json["statements"][0].get("kind").is_none(), "{json}");
	assert_eq!(json["statements"][0]["typescript"], "unknown");
	assert!(!json.to_string().contains("null"), "{json}");
	let host = serde_json::to_value(&reports["transaction"]).expect("serialises");
	assert_eq!(host["params"], serde_json::json!([]));
	assert_eq!(host["statements"].as_array().expect("statements").len(), 5);
}

/// Typechecks the goldens with `strict` against the real SDK. Needs `SURREALKIT_TSC` (a `tsc`
/// binary) and `SURREALKIT_TS_SDK` (the JS SDK package directory, which has `dist/`); skipped
/// without them.
#[test]
#[ignore = "needs SURREALKIT_TSC and SURREALKIT_TS_SDK"]
fn typecheck_golden_with_tsc() {
	let (Some(tsc), Some(sdk)) =
		(std::env::var_os("SURREALKIT_TSC"), std::env::var_os("SURREALKIT_TS_SDK"))
	else {
		return;
	};
	let dir = tempfile::tempdir().expect("tempdir");
	let root = dir.path();
	let modules = root.join("node_modules");
	std::fs::create_dir_all(&modules).expect("node_modules");
	symlink(Path::new(&sdk), &modules.join("surrealdb"));

	for (name, text) in [("queries", QUERIES), ("empty", EMPTY)] {
		write(&root.join(format!("{name}.ts")), &render_default(text));
	}
	write(&root.join("kinds.ts"), &render_kinds());
	write(&root.join("native.ts"), &render(&entries(QUERIES), &[], &native_unknown()));
	let mut snippet_files = Vec::new();
	for (name, report) in query_type_reports() {
		let file = format!("qt_{name}.ts");
		write(&root.join(&file), &report.snippet);
		snippet_files.push(file);
	}
	write(&root.join("use.ts"), USE_TS);
	let files = ["use.ts", "queries.ts", "kinds.ts", "native.ts", "empty.ts"]
		.map(String::from)
		.into_iter()
		.chain(snippet_files)
		.map(|file| format!("{file:?}"))
		.collect::<Vec<_>>()
		.join(", ");
	write(
		&root.join("tsconfig.json"),
		&format!(
			r#"{{
  "compilerOptions": {{
    "strict": true,
    "noEmit": true,
    "target": "es2022",
    "module": "esnext",
    "moduleResolution": "bundler",
    "skipLibCheck": true,
    "types": []
  }},
  "files": [{files}]
}}
"#
		),
	);

	let output = Command::new(&tsc)
		.args(["--project", "tsconfig.json"])
		.current_dir(root)
		.output()
		.expect("runs tsc");
	assert!(
		output.status.success(),
		"tsc rejected the golden:\n{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
}

fn write(path: &PathBuf, text: &str) {
	std::fs::write(path, text).expect("writes");
}

#[cfg(unix)]
fn symlink(target: &Path, link: &Path) {
	std::os::unix::fs::symlink(target, link).expect("symlinks the SDK");
}

#[cfg(not(unix))]
fn symlink(_target: &Path, _link: &Path) {
	panic!("the tsc check needs a unix symlink");
}

/// What a consumer writes against the generated module, including what it must reject.
const USE_TS: &str = r#"
import { Surreal } from "surrealdb";
import type { Queries, QueryNames, Typed } from "./queries";

const db = new Surreal() as Typed<Surreal>;

export async function main() {
  const [rows] = await db.query("SELECT name, email FROM user WHERE name = $name LIMIT $limit", {
    name: "ada",
  });
  const first: { name: string; email: string } = rows[0];
  void first;

  // A known text must be given its parameters' types; it never falls through to the SDK's
  // untyped overload.
  // @ts-expect-error a parameter of the wrong type
  await db.query("SELECT name, email FROM user WHERE name = $name LIMIT $limit", { name: 1 });
  // @ts-expect-error a required parameter left out
  await db.query("SELECT name, email FROM user WHERE name = $name LIMIT $limit");
  // @ts-expect-error a binding for a query that reads no parameter
  await db.query("SELECT * FROM event", { nope: 1 });
  // Other text, and text only known at run time, keep the SDK's own types.
  const [elsewhere] = await db.query<[number[]]>("SELECT * FROM elsewhere", { x: 1 });
  const counts: number[] = elsewhere;
  const dynamic: string = "SELECT 1";
  await db.query(dynamic, { x: 1 });
  void counts;

  const [row] = await db.query("SELECT * FROM ONLY user:alice");
  if (row) {
    const bio: string | undefined = row.bio;
    void bio;
  }

  // One slot per statement: BEGIN and LET first, COMMIT last.
  const [begun, , counted, , committed] = await db.query(
    "BEGIN; LET $n = 1; SELECT count() FROM post GROUP ALL; SELECT * FROM $__host0; COMMIT;",
  );
  const n: number = counted[0].count;
  const done: undefined = committed;
  void begun;
  void n;
  void done;

  const [created, updated, deleted, gone, diffs] = await db.query(
    "CREATE ONLY user:ada SET name = 'Ada', age = 36; UPDATE ONLY user:bob SET age += 1; DELETE user:ada; DELETE user:bob RETURN AFTER; UPDATE user SET age += 1 RETURN DIFF;",
  );
  const age: number = created.age;
  const maybe: number | undefined = updated?.age;
  const empty: [] = deleted;
  const after: undefined = gone[0];
  const op: "add" | "change" | "remove" | "replace" = diffs[0][0].op;
  void age;
  void maybe;
  void empty;
  void after;
  void op;

  const [events] = await db.query("SELECT * FROM event");
  const extra: unknown = events[0].anything;
  void extra;

  await db.query("SELECT * FROM post WHERE published = $published AND author = $author", {
    published: true,
    author: undefined,
  });

  // @ts-expect-error The result is typed, not the SDK's `unknown`.
  const bad: number = rows[0].name;
  void bad;

  // Text the registry does not know falls through to the SDK's own overloads.
  await db.query("SELECT 1");
}

const named: Queries[QueryNames["usersByName"]]["params"] = { name: "ada" };
void named;
"#;
