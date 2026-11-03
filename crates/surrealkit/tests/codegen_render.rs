//! Golden tests for the TypeScript renderer, over hand-built `TypesDocument` fixtures. No engine,
//! no network.

use std::path::{Path, PathBuf};
use std::process::Command;

use surrealkit::codegen::document::{FORMAT, TypesDocument, VERSION};
use surrealkit::codegen::typescript::{DateStyle, HintStyle, RenderOptions, render};

const BLOG: &str = include_str!("fixtures/codegen/blog.json");
const EMPTY: &str = include_str!("fixtures/codegen/empty.json");
const OPEN: &str = include_str!("fixtures/codegen/open.json");
const KINDS: &str = include_str!("fixtures/codegen/kinds.json");

fn doc(text: &str) -> TypesDocument {
	TypesDocument::from_json(text).expect("fixture document reads")
}

fn render_default(text: &str) -> String {
	render(&doc(text), &RenderOptions::default())
}

#[test]
fn blog_schema() {
	insta::assert_snapshot!("blog", render_default(BLOG));
}

#[test]
fn blog_schema_with_native_dates_and_distrusted_hints() {
	let opts = RenderOptions {
		dates: DateStyle::Native,
		hints: HintStyle::Unknown,
		..RenderOptions::default()
	};
	insta::assert_snapshot!("blog_native_unknown", render(&doc(BLOG), &opts));
}

#[test]
fn empty_document() {
	insta::assert_snapshot!("empty", render_default(EMPTY));
}

#[test]
fn open_rows_and_nested_open_fields() {
	insta::assert_snapshot!("open", render_default(OPEN));
}

#[test]
fn every_engine_kind() {
	insta::assert_snapshot!("kinds", render_default(KINDS));
}

#[test]
fn refuses_a_foreign_format_and_a_newer_version() {
	let foreign = TypesDocument::from_json(r#"{"format":"other.types","version":1}"#)
		.expect_err("a foreign format is refused");
	let foreign = format!("{foreign:#}");
	assert!(foreign.contains("other.types") && foreign.contains(FORMAT), "{foreign}");

	let newer = format!(r#"{{"format":"{FORMAT}","version":{}}}"#, VERSION + 1);
	let newer = TypesDocument::from_json(&newer).expect_err("a newer version is refused");
	let newer = format!("{newer:#}");
	assert!(newer.contains(&(VERSION + 1).to_string()) && newer.contains(FORMAT), "{newer}");
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

	for (name, text) in [("blog", BLOG), ("open", OPEN), ("kinds", KINDS)] {
		write(&root.join(format!("{name}.ts")), &render_default(text));
	}
	let native = RenderOptions {
		dates: DateStyle::Native,
		hints: HintStyle::Unknown,
		..RenderOptions::default()
	};
	write(&root.join("native.ts"), &render(&doc(BLOG), &native));
	write(&root.join("empty.ts"), &render_default(EMPTY));
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
  "files": ["use.ts", "blog.ts", "open.ts", "kinds.ts", "native.ts", "empty.ts"]
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
import type { Queries, QueryNames, Tables, Typed, User, Post } from "./blog";

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

  const [, counted] = await db.query(
    "BEGIN; LET $n = 1; SELECT count() FROM post GROUP ALL; SELECT * FROM $__host0; COMMIT;",
  );
  const n: number = counted[0].count;
  void n;

  // @ts-expect-error The result is typed, not the SDK's `unknown`.
  const bad: number = rows[0].name;
  void bad;

  // Text the registry does not know falls through to the SDK's own overloads.
  await db.query("SELECT 1");
}

const named: Queries[QueryNames["usersByName"]]["params"] = { name: "ada" };
void named;

const user = {} as User;
const joined: import("surrealdb").DateTime = user.joined;
void joined;
const id: import("surrealdb").RecordId<"user"> = user.id;
void id;
const tables: Tables = {} as Tables;
const post: Post = tables.post;
void post;
"#;
