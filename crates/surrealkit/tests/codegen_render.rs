//! Golden tests for the per-query TypeScript renderer, over hand-built `QueryEntry` fixtures, and
//! the conversion of the engine's `TypesDocument` into those entries. No engine, no network.

use std::path::{Path, PathBuf};
use std::process::Command;

use surrealkit::codegen::document::TypesDocument;
use surrealkit::codegen::queries::{QueryEntry, from_types_document};
use surrealkit::codegen::typescript::{DateStyle, HintStyle, RenderOptions, render};
use surrealkit::typegen::document::TypeDef;

const QUERIES: &str = include_str!("fixtures/codegen/queries.json");
const KINDS: &str = include_str!("fixtures/codegen/queries_kinds.json");
/// The `types` the `named` kinds of `KINDS` refer to.
const KIND_TYPES: &str = include_str!("fixtures/codegen/queries_kinds_types.json");
const EMPTY: &str = include_str!("fixtures/codegen/queries_empty.json");
const BLOG: &str = include_str!("fixtures/codegen/blog.json");
const OPEN: &str = include_str!("fixtures/codegen/open.json");

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
	write(&root.join("use.ts"), USE_TS);
	write(
		&root.join("tsconfig.json"),
		r#"{
  "compilerOptions": {
    "strict": true,
    "noEmit": true,
    "target": "es2022",
    "module": "esnext",
    "moduleResolution": "bundler",
    "skipLibCheck": true,
    "types": []
  },
  "files": ["use.ts", "queries.ts", "kinds.ts", "native.ts", "empty.ts"]
}
"#,
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
