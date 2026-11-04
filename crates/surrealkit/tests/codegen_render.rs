//! Golden tests for the per-query TypeScript renderer, over hand-built `QueryEntry` fixtures, and
//! the conversion of the engine's `TypesDocument` into those entries. No engine, no network.

use std::path::Path;
use std::process::Command;

use surrealkit::codegen::document::TypesDocument;
use surrealkit::codegen::queries::{QueryEntry, from_types_document};
use surrealkit::codegen::query_types::{Report, Request, report};
use surrealkit::codegen::section::{Collected, assemble};
use surrealkit::codegen::typescript::{DateStyle, HintStyle, RenderOptions, render};
use surrealkit::typegen::document::TypeDef;

const QUERIES: &str = include_str!("fixtures/codegen/queries.json");
const KINDS: &str = include_str!("fixtures/codegen/queries_kinds.json");
/// The `types` the `named` kinds of `KINDS` refer to.
const KIND_TYPES: &str = include_str!("fixtures/codegen/queries_kinds_types.json");
const EMPTY: &str = include_str!("fixtures/codegen/queries_empty.json");
/// The engine's document for the live and failed queries at the end of `QUERIES`.
const LIVE_ENGINE: &str = include_str!("fixtures/codegen/queries_live_engine.json");
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

/// The queries of `QUERIES` the engine's `LIVE_ENGINE` document describes: the last seven.
fn live_entries() -> Vec<QueryEntry> {
	let all = entries(QUERIES);
	all[all.len() - 7..].to_vec()
}

#[test]
fn the_engines_live_document_converts_to_the_v2_entries() {
	let doc = TypesDocument::from_json(LIVE_ENGINE).expect("fixture document reads");
	let converted = from_types_document(&doc);
	assert!(converted.unrepresented.is_empty(), "{:?}", converted.unrepresented);
	assert_eq!(converted.queries, live_entries());
	// The snake_case engine members reach the v2 camelCase ones.
	let json = serde_json::to_value(&converted.queries).expect("serialises");
	assert_eq!(json[1]["statements"][0]["liveRefusal"]["windowable"], true);
	assert_eq!(json[2]["statements"][0]["live"]["watch"], "LIVE SELECT id FROM user");
	assert_eq!(json[2]["statements"][0]["live"]["exit"], "silent");
	assert_eq!(json[0]["statements"][0]["live"]["notifications"][3]["action"], "KILLED");
	assert!(json[6].get("params").is_none() && json[6]["statements"] == serde_json::json!([]));
}

#[test]
fn a_query_the_engine_could_not_type_is_one_diagnostic_and_touches_no_other() {
	let doc = TypesDocument::from_json(LIVE_ENGINE).expect("fixture document reads");
	let (assembled, diagnostics) = assemble(&Collected::default(), Some(&doc));
	assert_eq!(assembled.len(), 7);
	assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
	let diagnostic = &diagnostics[0];
	assert_eq!(diagnostic.code, "query-invalid");
	assert_eq!(diagnostic.query.as_deref(), Some("SELEC * FROM user"));
	assert_eq!(diagnostic.source, "SELEC * FROM user");
	assert_eq!(diagnostic.span.map(|span| (span.start, span.end)), Some((0, 5)));
	assert!(diagnostic.reason.contains("S0001") && diagnostic.reason.contains("Parse error"));
	let failed: Vec<&str> =
		assembled.iter().filter(|e| e.error.is_some()).map(|e| e.key.as_str()).collect();
	assert_eq!(failed, ["SELEC * FROM user"]);
	let rest: Vec<&QueryEntry> = assembled.iter().filter(|e| e.error.is_none()).collect();
	let expected = live_entries();
	let expected: Vec<&QueryEntry> = expected.iter().filter(|e| e.error.is_none()).collect();
	for entry in expected {
		assert!(rest.contains(&entry), "{} changed", entry.key);
	}
}

#[test]
fn the_registries_leave_out_a_failed_query() {
	let text = render_default(QUERIES);
	assert!(!text.contains("SELEC * FROM user"), "the failed query is in the module");
	assert!(text.contains("export const liveQueries = {"));
	assert!(!text.contains("declare module"));
	let registered = render(
		&entries(QUERIES),
		&[],
		&RenderOptions {
			register: Some("surrealkit-query".to_string()),
			..RenderOptions::default()
		},
	);
	assert!(registered.ends_with(
		"declare module \"surrealkit-query\" {\n  interface Register {\n    queries: Queries;\n    \
		 liveQueries: LiveQueries;\n  }\n}\n"
	));
}

#[test]
fn live_queries_are_keyed_like_queries_and_a_multi_statement_query_has_none() {
	let text = render_default(QUERIES);
	let live = text.split("export const liveQueries = {").nth(1).expect("liveQueries");
	let live = live.split("} as const;").next().expect("end");
	for key in [
		"\"SELECT * FROM user\"",
		"\"SELECT * FROM user ORDER BY name LIMIT $lim\"",
		"\"SELECT * FROM user WHERE team = $team\"",
		"\"SELECT * FROM ONLY user:ada\"",
		"\"LIVE SELECT DIFF FROM user\"",
	] {
		assert!(live.contains(&format!("  {key}: {{")), "{key}");
	}
	assert!(!live.contains("LET $n = 1"), "a real LIVE SELECT among other statements is not keyed");
	assert!(!live.contains("S0001") && !live.contains("E4009"), "no code or message");
}

#[test]
fn an_empty_list_has_an_empty_live_registry() {
	let text = render_default(EMPTY);
	assert!(text.contains("export const liveQueries = {} as const;"));
	assert!(text.contains("type LiveNotifications = {};"));
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
fn query_types_reports_the_live_subscription() {
	let reports: std::collections::BTreeMap<_, _> = query_type_reports().into_iter().collect();
	let live = &reports["live"];
	let json = serde_json::to_value(live).expect("serialises");
	let statement = &json["statements"][0];
	assert_eq!(statement["live"]["query"], "LIVE SELECT * FROM user");
	assert_eq!(statement["live"]["notifications"][0]["action"], "CREATE");
	assert_eq!(statement["liveRefusal"]["clauses"], serde_json::json!(["ORDER BY", "LIMIT"]));
	assert_eq!(statement["liveRefusal"]["windowable"], true);
	assert!(
		live.snippet.contains("export type ListUsersLive = { queryId: Uuid; action: \"CREATE\"")
	);
	assert!(live.snippet.contains("Uuid"), "the snippet imports what its types use");
	let plain = serde_json::to_value(&reports["projection"]).expect("serialises");
	assert!(plain["statements"][0].get("live").is_none(), "{plain}");
	assert!(plain["statements"][0].get("liveRefusal").is_none(), "{plain}");
	assert!(!reports["projection"].snippet.contains("Live"));
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
		.collect::<Vec<_>>();
	write_tsconfig(root, &files);
	run_tsc(&tsc, root);
}

/// The same module with `register` set, against a stub of the package it names.
#[test]
#[ignore = "needs SURREALKIT_TSC and SURREALKIT_TS_SDK"]
fn typecheck_golden_with_register_with_tsc() {
	let (Some(tsc), Some(sdk)) =
		(std::env::var_os("SURREALKIT_TSC"), std::env::var_os("SURREALKIT_TS_SDK"))
	else {
		return;
	};
	let dir = tempfile::tempdir().expect("tempdir");
	let root = dir.path();
	let modules = root.join("node_modules");
	std::fs::create_dir_all(modules.join("surrealkit-query")).expect("node_modules");
	symlink(Path::new(&sdk), &modules.join("surrealdb"));
	write(
		&modules.join("surrealkit-query").join("package.json"),
		r#"{ "name": "surrealkit-query", "version": "0.0.0", "types": "index.d.ts" }"#,
	);
	write(&modules.join("surrealkit-query").join("index.d.ts"), "export interface Register {}\n");
	let opts = RenderOptions {
		register: Some("surrealkit-query".to_string()),
		..RenderOptions::default()
	};
	write(&root.join("queries.ts"), &render(&entries(QUERIES), &[], &opts));
	write(&root.join("use.ts"), REGISTER_TS);
	write_tsconfig(root, &["use.ts".to_string(), "queries.ts".to_string()]);
	run_tsc(&tsc, root);
}

fn write_tsconfig(root: &Path, files: &[String]) {
	let files = files.iter().map(|file| format!("{file:?}")).collect::<Vec<_>>().join(", ");
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
}

fn run_tsc(tsc: &std::ffi::OsStr, root: &Path) {
	let output = Command::new(tsc)
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

fn write(path: &Path, text: &str) {
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
import type { RecordId, Uuid } from "surrealdb";
import { liveQueries } from "./queries";
import type { LiveNotification, LiveQueryText, Queries, QueryNames, Typed } from "./queries";

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

export function live(message: LiveNotification<"SELECT * FROM user">) {
  const queryId: Uuid = message.queryId;
  void queryId;
  if (message.action === "CREATE") {
    const id: RecordId<"user"> = message.recordId;
    const name: string = message.value.name;
    // @ts-expect-error The value is typed, not `unknown`.
    const bad: number = message.value.name;
    void id;
    void name;
    void bad;
  } else if (message.action === "KILLED") {
    // The SDK's KILLED message carries neither a record nor a value.
    const id: undefined = message.recordId;
    const value: undefined = message.value;
    void id;
    void value;
  } else {
    const action: "UPDATE" | "DELETE" = message.action;
    void action;
  }
}

export function diff(message: LiveNotification<"LIVE SELECT DIFF FROM user">) {
  if (message.action === "UPDATE") {
    const op: "add" | "change" | "remove" | "replace" = message.value[0].op;
    void op;
  }
}

export function hinted(message: LiveNotification<"SELECT * FROM user WHERE team = $team">) {
  if (message.action === "CREATE") {
    // An open result admits undeclared keys.
    const extra: unknown = message.value.anything;
    void extra;
  }
}

const refusedForGood: false = liveQueries["SELECT * FROM ONLY user:ada"].refusal.windowable;
const refusedButWindowed: true =
  liveQueries["SELECT * FROM user ORDER BY name LIMIT $lim"].refusal.windowable;
const windowedQuery: "LIVE SELECT * FROM user" =
  liveQueries["SELECT * FROM user ORDER BY name LIMIT $lim"].query;
const exits: "silent" = liveQueries["SELECT * FROM user WHERE team = $team"].exit;
const watched: "LIVE SELECT id FROM user" = liveQueries["SELECT * FROM user WHERE team = $team"].watch;
void [refusedForGood, refusedButWindowed, windowedQuery, exits, watched];

const servable: LiveQueryText = "SELECT * FROM user";
const windowable: LiveQueryText = "SELECT * FROM user ORDER BY name LIMIT $lim";
// @ts-expect-error FROM ONLY cannot be windowed, so it is not a text a subscription can serve.
const refused: LiveQueryText = "SELECT * FROM ONLY user:ada";
void [servable, windowable, refused];
// @ts-expect-error A refusal LIVE cannot window has no notification.
type OnlyNotification = LiveNotification<"SELECT * FROM ONLY user:ada">;

// @ts-expect-error The query the engine could not type is not in the registry.
type Failed = Queries["SELEC * FROM user"];
// A real LIVE SELECT among other statements has no live entry.
// @ts-expect-error
liveQueries["LET $n = 1; LIVE SELECT * FROM user;"];
"#;

/// What a consumer reads through the stub package the module augments.
const REGISTER_TS: &str = r#"
import type { Register } from "surrealkit-query";
import type { LiveQueries, Queries } from "./queries";

type Q = Register["queries"];
type L = Register["liveQueries"];
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
export const queries: Same<Q, Queries> = true;
export const live: Same<L, LiveQueries> = true;
const text: keyof Q = "SELECT * FROM user";
void text;
"#;
