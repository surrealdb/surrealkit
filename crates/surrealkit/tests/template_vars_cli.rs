//! Template variables read from the project's `.env` / `.env.local`, through the binary.
//!
//! Each run uses the embedded in-memory engine (`--host mem://`), so a fresh database per
//! invocation. The substituted value is read back from the table comment in the JSON schema
//! document `sync` writes.

use std::fs;
use std::path::Path;
use std::process::Command;

fn write(path: &Path, contents: &str) {
	if let Some(parent) = path.parent() {
		fs::create_dir_all(parent).expect("mkdir");
	}
	fs::write(path, contents).expect("write");
}

/// A project whose schema puts `${JWT_SECRET}` in a table comment.
fn project(toml_extra: &str) -> tempfile::TempDir {
	let tmp = tempfile::TempDir::new().expect("tempdir");
	write(
		&tmp.path().join("database/schema/001_app.surql"),
		"DEFINE TABLE t SCHEMAFULL COMMENT '${JWT_SECRET}';\n",
	);
	write(
		&tmp.path().join("surrealkit.toml"),
		&format!("{toml_extra}\n[typegen]\njson = \"out/schema.json\"\n"),
	);
	tmp
}

/// Run `sync` against a fresh in-memory engine and return its exit code and output.
fn sync(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> (Option<i32>, String) {
	let mut command = Command::new(env!("CARGO_BIN_EXE_surrealkit"));
	command.current_dir(dir).env_remove("SURREALKIT_VAR_JWT_SECRET").env_remove("JWT_SECRET");
	for (k, v) in env {
		command.env(k, v);
	}
	let output = command.args(["--host", "mem://", "sync"]).args(args).output().expect("run");
	let text = format!(
		"{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
	(output.status.code(), text)
}

/// The table comment `sync` stored, read from the `DEFINE TABLE` in the JSON document it wrote.
fn comment(dir: &Path) -> String {
	let json = fs::read_to_string(dir.join("out/schema.json")).expect("schema.json");
	let doc: serde_json::Value = serde_json::from_str(&json).expect("json");
	let define = doc["tables"]
		.as_array()
		.expect("tables")
		.iter()
		.find(|t| t["name"] == "t")
		.and_then(|t| t["define"].as_str())
		.expect("a DEFINE TABLE for t")
		.to_string();
	let (_, rest) = define.split_once("COMMENT '").expect("a comment on t");
	let (comment, _) = rest.split_once('\'').expect("a closing quote");
	comment.to_string()
}

fn synced_comment(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> String {
	let (code, text) = sync(dir, args, env);
	assert_eq!(code, Some(0), "{text}");
	comment(dir)
}

#[test]
fn a_variable_resolves_from_dotenv_under_its_own_name() {
	let tmp = project("");
	write(&tmp.path().join(".env"), "SURREALDB_NAME=app\nJWT_SECRET=from_dotenv\n");
	assert_eq!(synced_comment(tmp.path(), &[], &[]), "from_dotenv");
}

#[test]
fn dotenv_local_is_read_in_place_of_dotenv() {
	// The same file the connection settings come from: `.env.local` when present.
	let tmp = project("");
	write(&tmp.path().join(".env"), "JWT_SECRET=from_dotenv\n");
	write(&tmp.path().join(".env.local"), "JWT_SECRET=from_dotenv_local\n");
	assert_eq!(synced_comment(tmp.path(), &[], &[]), "from_dotenv_local");
}

#[test]
fn flags_and_prefixed_env_beat_dotenv_and_toml_is_the_fallback() {
	let tmp = project("[variables]\njwt_secret = \"from_toml\"\n");
	assert_eq!(synced_comment(tmp.path(), &[], &[]), "from_toml");

	write(&tmp.path().join(".env"), "JWT_SECRET=from_dotenv\n");
	assert_eq!(synced_comment(tmp.path(), &[], &[]), "from_dotenv");
	assert_eq!(
		synced_comment(tmp.path(), &[], &[("SURREALKIT_VAR_JWT_SECRET", "from_env")]),
		"from_env"
	);
	assert_eq!(
		synced_comment(
			tmp.path(),
			&["--var", "JWT_SECRET=from_flag"],
			&[("SURREALKIT_VAR_JWT_SECRET", "from_env")]
		),
		"from_flag"
	);
}

#[test]
fn a_plain_process_environment_variable_is_not_a_source() {
	let tmp = project("");
	let (code, text) = sync(tmp.path(), &[], &[("JWT_SECRET", "from_shell")]);
	assert_ne!(code, Some(0), "{text}");
	assert!(text.contains("template variable 'JWT_SECRET' is not defined"), "{text}");
	assert!(text.contains("JWT_SECRET in .env / .env.local"), "{text}");
}

#[test]
fn connection_settings_in_dotenv_still_apply() {
	// SURREALDB_HOST from .env is still honoured: an unreachable host fails the run even
	// though the variable resolves. (`--host` is not passed here.)
	let tmp = project("");
	write(
		&tmp.path().join(".env"),
		"SURREALDB_HOST=http://127.0.0.1:1\nSURREALDB_CONNECT_TIMEOUT_SECS=5\nJWT_SECRET=x\n",
	);
	let output = Command::new(env!("CARGO_BIN_EXE_surrealkit"))
		.current_dir(tmp.path())
		.env_remove("SURREALDB_HOST")
		.arg("sync")
		.output()
		.expect("run");
	let text = String::from_utf8_lossy(&output.stderr);
	assert!(!output.status.success(), "{text}");
	assert!(text.contains("127.0.0.1:1"), "the .env host should be used: {text}");
}
