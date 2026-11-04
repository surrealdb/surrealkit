//! `surrealkit compile --watch`, through the binary: it compiles, then regenerates on a change
//! to an input and on nothing else.
//!
//! The environment is cleared: compile reads no connection setting. Timeouts are generous and
//! every wait polls, because the file system's change notification is neither instant nor
//! (on macOS) exact about when it starts.
#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const CONFIG: &str = "[typegen]\nschema_version = 2\njson = \"generated/schema.json\"\n\n\
                      [typegen.queries]\nsources = [\"src\"]\ntypescript = \"src/generated/queries.ts\"\n";
const BASE: &str = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a ON t TYPE string;\n";
/// 4.0 refuses a nested field whose parent is not declared.
const BAD: &str = "DEFINE FIELD x.y ON t TYPE string;\n";
const PAGE: &str = "export const rows = (db) => db.query(\"SELECT * FROM t\");\n";

fn write(path: &Path, text: &str) {
	fs::create_dir_all(path.parent().expect("parent")).expect("dir");
	fs::write(path, text).expect("write");
}

fn project(schema: &str) -> TempDir {
	let dir = TempDir::new().expect("tempdir");
	write(&dir.path().join("surrealkit.toml"), CONFIG);
	write(&dir.path().join("database/schema/001_t.surql"), schema);
	write(&dir.path().join("src/page.ts"), PAGE);
	dir
}

fn command(root: &Path, args: &[&str]) -> Command {
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command
		.current_dir(root)
		.args(args)
		.env_clear()
		.env("NO_COLOR", "1")
		.env("RUST_MIN_STACK", "8388608");
	command
}

/// A running `compile --watch`, with what it wrote to both streams gathered on threads.
struct Watching {
	child: Child,
	seen: Arc<Mutex<String>>,
}

impl Watching {
	fn start(root: &Path) -> Self {
		let mut child = command(root, &["compile", "--watch"])
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.spawn()
			.expect("spawn compile --watch");
		let seen = Arc::new(Mutex::new(String::new()));
		let stdout = child.stdout.take().expect("piped stdout");
		let stderr = child.stderr.take().expect("piped stderr");
		gather(stdout, &seen);
		gather(stderr, &seen);
		Self {
			child,
			seen,
		}
	}

	fn text(&self) -> String {
		self.seen.lock().expect("lock").clone()
	}

	/// The lines of the output that contain `needle`.
	fn lines(&self, needle: &str) -> Vec<String> {
		self.text().lines().filter(|line| line.contains(needle)).map(str::to_string).collect()
	}

	/// Poll until `done` accepts what was written, or `limit` passes; `tick` runs on each poll.
	fn wait(&self, limit: Duration, done: impl Fn(&str) -> bool, mut tick: impl FnMut()) -> bool {
		let start = Instant::now();
		while start.elapsed() < limit {
			if done(&self.text()) {
				return true;
			}
			tick();
			std::thread::sleep(Duration::from_millis(250));
		}
		done(&self.text())
	}

	/// Wait for the first run to finish, then give the file system watcher a moment to be
	/// ready: it may not be the instant the banner prints.
	fn ready(&self, first: &str) {
		assert!(
			self.wait(Duration::from_secs(120), |text| text.contains(first), || {}),
			"the first run never reported {first:?}: {}",
			self.text()
		);
		std::thread::sleep(Duration::from_secs(2));
	}

	/// Write `text` to `path` until `done` accepts the output, rewriting every few seconds
	/// (with a counter in `comment`'s place) in case the watcher missed one. Returns how many
	/// writes it took.
	fn edit_until(
		&self,
		path: &Path,
		text: &str,
		comment: &str,
		done: impl Fn(&str) -> bool,
	) -> usize {
		let mut writes = 0;
		let mut last = Instant::now() - Duration::from_secs(60);
		let seen = self.wait(Duration::from_secs(90), &done, || {
			if last.elapsed() >= Duration::from_secs(8) {
				writes += 1;
				write(path, &format!("{text}{comment} {writes}\n"));
				last = Instant::now();
			}
		});
		assert!(seen, "the edit to {} was never seen: {}", path.display(), self.text());
		writes
	}
}

fn gather(stream: impl Read + Send + 'static, sink: &Arc<Mutex<String>>) {
	let sink = Arc::clone(sink);
	std::thread::spawn(move || {
		for line in BufReader::new(stream).lines().map_while(Result::ok) {
			let mut seen = sink.lock().expect("lock");
			seen.push_str(&line);
			seen.push('\n');
		}
	});
}

impl Drop for Watching {
	fn drop(&mut self) {
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}

fn interrupt(child: &Child) {
	let status =
		Command::new("kill").args(["-INT", &child.id().to_string()]).status().expect("run kill");
	assert!(status.success());
}

#[test]
fn it_regenerates_on_an_input_and_on_nothing_else() {
	let dir = project(BASE);
	let root = dir.path();
	let json = root.join("generated/schema.json");
	let queries = root.join("src/generated/queries.ts");
	let watching = Watching::start(root);
	watching.ready("compile: initial run:");
	let first = watching.lines("compile: initial run:");
	assert_eq!(first.len(), 1, "{}", watching.text());
	assert!(first[0].contains("wrote generated/schema.json"), "{}", first[0]);
	assert!(first[0].contains("src/generated/queries.ts"), "{}", first[0]);
	let before = fs::read_to_string(&json).expect("the first run wrote the JSON");
	assert!(!before.contains("\"b\""), "{before}");

	// A schema file: one regeneration, and the JSON is rewritten.
	let schema = root.join("database/schema/001_t.surql");
	let more = format!("{BASE}DEFINE FIELD b ON t TYPE int;\n");
	write(&schema, &more);
	let mut writes = 1;
	let seen = watching.wait(
		Duration::from_secs(10),
		|text| text.contains("compile: schema changed (database/schema/001_t.surql)"),
		|| {},
	);
	if !seen {
		writes += watching.edit_until(&schema, &more, "--", |text| {
			text.contains("compile: schema changed (database/schema/001_t.surql)")
		});
	}
	std::thread::sleep(Duration::from_secs(3));
	let lines = watching.lines("compile: schema changed");
	assert!(!lines.is_empty() && lines.len() <= writes, "{} for {writes} writes", lines.len());
	assert!(lines[0].contains("wrote generated/schema.json"), "{}", lines[0]);
	assert!(fs::read_to_string(&json).expect("json").contains("\"b\""), "the JSON has the field");

	// A host source: a queries-changed line.
	let page = root.join("src/page.ts");
	let edited = "export const rows = (db) => db.query(\"SELECT a FROM t\");\n";
	write(&page, edited);
	let seen = watching.wait(
		Duration::from_secs(10),
		|text| text.contains("compile: queries changed (src/page.ts)"),
		|| {},
	);
	if !seen {
		watching.edit_until(&page, edited, "//", |text| {
			text.contains("compile: queries changed (src/page.ts)")
		});
	}
	let lines = watching.lines("compile: queries changed (src/page.ts)");
	assert!(lines[0].contains("re-typed"), "{}", lines[0]);
	std::thread::sleep(Duration::from_secs(3));

	// What the run writes is not an input: writing it is no change.
	let settled = watching.text();
	write(&json, "{}\n");
	write(&queries, "// edited by hand\n");
	write(&root.join("generated/schema.surrealkit-tmp-1-1.json"), "{}\n");
	std::thread::sleep(Duration::from_secs(4));
	assert_eq!(watching.text(), settled, "an output changed and the run regenerated");

	// Ctrl-C ends it, successfully.
	let mut watching = watching;
	interrupt(&watching.child);
	let start = Instant::now();
	let status = loop {
		if let Some(status) = watching.child.try_wait().expect("wait") {
			break status;
		}
		assert!(start.elapsed() < Duration::from_secs(20), "Ctrl-C did not end it");
		std::thread::sleep(Duration::from_millis(100));
	};
	assert_eq!(status.code(), Some(0), "{}", watching.text());
}

#[test]
fn a_schema_that_does_not_compile_is_reported_and_watched_until_it_does() {
	let dir = project(&format!("{BASE}{BAD}"));
	let root = dir.path();
	let watching = Watching::start(root);
	watching.ready("compile: initial run:");
	let first = watching.lines("compile: initial run:");
	assert!(first[0].contains("001_t.surql"), "the error names the file: {}", first[0]);
	assert!(!root.join("generated/schema.json").exists(), "a failed run writes nothing");

	// A query cannot be typed before the schema compiles; the outputs stay as they are.
	let page = root.join("src/page.ts");
	let edited = "export const rows = (db) => db.query(\"SELECT a FROM t\");\n";
	write(&page, edited);
	let seen = |text: &str| text.contains("compile: queries changed (src/page.ts)");
	if !watching.wait(Duration::from_secs(10), seen, || {}) {
		watching.edit_until(&page, edited, "//", seen);
	}
	let line = &watching.lines("compile: queries changed")[0];
	assert!(line.contains("schema has to compile first"), "{line}");
	assert!(!root.join("generated/schema.json").exists());

	// Fixing the schema regenerates everything.
	let schema = root.join("database/schema/001_t.surql");
	write(&schema, BASE);
	let seen = |text: &str| text.contains("compile: schema changed");
	if !watching.wait(Duration::from_secs(10), seen, || {}) {
		watching.edit_until(&schema, BASE, "--", seen);
	}
	assert!(
		watching.wait(
			Duration::from_secs(30),
			|_| root.join("generated/schema.json").exists(),
			|| {}
		),
		"{}",
		watching.text()
	);
	let line = &watching.lines("compile: schema changed")[0];
	assert!(line.contains("wrote generated/schema.json"), "{line}");
}

#[test]
fn watch_does_not_combine_with_check_stdout_or_watch_paths() {
	let dir = project(BASE);
	for other in ["--check", "--stdout", "--watch-paths"] {
		let output =
			command(dir.path(), &["compile", "--watch", other]).output().expect("run surrealkit");
		assert!(!output.status.success(), "--watch {other}");
		let text = String::from_utf8_lossy(&output.stderr);
		assert!(text.contains("cannot be used with"), "{text}");
	}
}
