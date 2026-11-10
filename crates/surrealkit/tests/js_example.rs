//! `examples/js-typed`: plain JavaScript typed through a declarations file. Needs `SURREALKIT_TSC`
//! (a `tsc` binary) and `SURREALKIT_TS_SDK` (the JS SDK package directory, which has `dist/`);
//! ignored without them, and nothing here runs `npm install`.

#![expect(
	clippy::print_stdout,
	clippy::print_stderr,
	reason = "a skipped check says why, and a run shows tsc's report"
)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn example() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/js-typed")
}

/// Copies the example's `jsconfig.json` and `src` into `root`, with the SDK linked into
/// `node_modules`, as an `npm install` would leave it.
fn stage(root: &Path, sdk: &Path) {
	let modules = root.join("node_modules");
	std::fs::create_dir_all(&modules).expect("node_modules");
	#[cfg(unix)]
	std::os::unix::fs::symlink(sdk, modules.join("surrealdb")).expect("symlinks the SDK");
	#[cfg(not(unix))]
	panic!("the tsc check needs a unix symlink: {}", sdk.display());
	std::fs::copy(example().join("jsconfig.json"), root.join("jsconfig.json")).expect("jsconfig");
	std::fs::create_dir_all(root.join("src")).expect("src");
	for entry in std::fs::read_dir(example().join("src")).expect("example src") {
		let entry = entry.expect("entry");
		std::fs::copy(entry.path(), root.join("src").join(entry.file_name())).expect("copy");
	}
}

fn tsc(tsc: &std::ffi::OsStr, root: &Path) -> Output {
	Command::new(tsc)
		.args(["-p", "jsconfig.json", "--noEmit", "--pretty", "false"])
		.current_dir(root)
		.output()
		.expect("runs tsc")
}

fn text(output: &Output) -> String {
	format!(
		"{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	)
}

#[test]
#[ignore = "needs SURREALKIT_TSC and SURREALKIT_TS_SDK"]
fn the_js_example_typechecks_and_its_bad_file_does_not() {
	let (Some(tsc_bin), Some(sdk)) =
		(std::env::var_os("SURREALKIT_TSC"), std::env::var_os("SURREALKIT_TS_SDK"))
	else {
		eprintln!("skipped: set SURREALKIT_TSC and SURREALKIT_TS_SDK to run the tsc check");
		return;
	};
	if !example().join("src/queries.d.ts").exists() {
		eprintln!(
			"skipped: examples/js-typed/src/queries.d.ts is not generated; run `surrealkit compile` there"
		);
		return;
	}
	let dir = tempfile::tempdir().expect("tempdir");
	let root = dir.path();
	stage(root, Path::new(&sdk));

	let both = tsc(&tsc_bin, root);
	let report = text(&both);
	assert!(!both.status.success(), "bad.js should not typecheck:\n{report}");
	assert!(
		report.contains("src/bad.js") && report.contains("'nick'"),
		"expected the wrong field in bad.js:\n{report}"
	);
	assert!(!report.contains("src/app.js"), "app.js should be clean:\n{report}");
	assert!(!report.contains("src/queries"), "the generated files should be clean:\n{report}");
	println!("{report}");

	std::fs::remove_file(root.join("src/bad.js")).expect("remove bad.js");
	let clean = tsc(&tsc_bin, root);
	assert!(clean.status.success(), "without bad.js the project typechecks:\n{}", text(&clean));
}
