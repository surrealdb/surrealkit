use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Result, anyhow, bail};
use regex::Regex;
use serde::Deserialize;

static VAR_REGEX: OnceLock<Regex> = OnceLock::new();

fn var_regex() -> &'static Regex {
	VAR_REGEX
		.get_or_init(|| Regex::new(r"\$\$\{[^}]+\}|\$\{([^}]+)\}").expect("invalid variable regex"))
}

/// Replace `${VAR_NAME}` tokens in `content` with values from `vars`.
///
/// Keys in `vars` must be `UPPER_CASE`. Lookup is case-insensitive: `${foo}`, `${Foo}`,
/// and `${FOO}` all match key `FOO`. The escape `$${VAR}` yields a literal `${VAR}`.
/// Errors on the first undefined variable. Skips the regex when `content` has no `${`.
pub fn apply(content: &str, vars: &HashMap<String, String>) -> Result<String> {
	// Fast path: no possible token in content. Skip regex entirely.
	if !content.contains("${") {
		return Ok(content.to_string());
	}

	let mut result = String::with_capacity(content.len());
	let mut last_end = 0;

	for cap in var_regex().captures_iter(content) {
		let full = cap.get(0).expect("regex match always has group 0");
		result.push_str(&content[last_end..full.start()]);

		let matched = full.as_str();
		if matched.starts_with("$$") {
			// $${NAME} -> literal ${NAME}. Drop the leading '$'.
			result.push_str(&matched[1..]);
		} else {
			let name = cap.get(1).expect("single-dollar branch has capture group 1").as_str();
			let key = name.to_ascii_uppercase();
			match vars.get(&key) {
				Some(value) => result.push_str(value),
				None => {
					return Err(anyhow!(
						"template variable '{}' is not defined \
                         (set via --var {}=VALUE, SURREALKIT_VAR_{} env var, or surrealkit.toml [variables])",
						key,
						key,
						key,
					));
				}
			}
		}

		last_end = full.end();
	}

	result.push_str(&content[last_end..]);
	Ok(result)
}

/// Merge variables from all three sources. Highest priority wins:
/// `cli_vars` > `SURREALKIT_VAR_*` env vars > `surrealkit.toml [variables]`.
/// Keys are uppercased. `toml_path` defaults to `./surrealkit.toml` when `None`.
#[doc(hidden)]
pub fn build_vars(
	cli_vars: &[(String, String)],
	toml_path: Option<&Path>,
) -> Result<HashMap<String, String>> {
	let mut map: HashMap<String, String> = HashMap::new();

	// Lowest priority: surrealkit.toml [variables]
	let cfg_path = toml_path.unwrap_or_else(|| Path::new("surrealkit.toml"));
	if cfg_path.exists() {
		let raw = std::fs::read_to_string(cfg_path)?;
		let cfg: ProjectConfig = toml::from_str(&raw)?;
		for (k, v) in cfg.variables {
			map.insert(k.to_ascii_uppercase(), v);
		}
	}

	// Middle priority: SURREALKIT_VAR_* environment variables
	for (key, value) in std::env::vars() {
		if let Some(stripped) = key.strip_prefix("SURREALKIT_VAR_")
			&& !stripped.is_empty()
		{
			map.insert(stripped.to_ascii_uppercase(), value);
		}
	}

	// Highest priority: --var KEY=VALUE CLI flags
	for (k, v) in cli_vars {
		if k.is_empty() {
			bail!("--var flag has an empty key");
		}
		map.insert(k.to_ascii_uppercase(), v.clone());
	}

	Ok(map)
}

/// Parse a `KEY=VALUE` string from a `--var` CLI flag. Splits at the first `=`.
/// Missing `=` is an error. Empty value is allowed.
#[doc(hidden)]
pub fn parse_var_flag(raw: &str) -> Result<(String, String)> {
	match raw.find('=') {
		None => bail!("--var '{}' is missing '=' (expected KEY=VALUE)", raw),
		Some(pos) => {
			let key = &raw[..pos];
			if key.is_empty() {
				bail!("--var '={}' has an empty key (expected KEY=VALUE)", &raw[pos + 1..]);
			}
			Ok((key.to_string(), raw[pos + 1..].to_string()))
		}
	}
}

#[derive(Debug, Clone, Default)]
pub struct TemplateVars {
	pub vars: HashMap<String, String>,
}

impl TemplateVars {
	/// Apply `${VAR}` substitution to `content`. Undefined variables are an error.
	pub fn apply(&self, content: &str) -> Result<String> {
		apply(content, &self.vars)
	}

	pub fn is_empty(&self) -> bool {
		self.vars.is_empty()
	}
}

#[derive(Debug, Default, Deserialize)]
struct ProjectConfig {
	#[serde(default)]
	variables: HashMap<String, String>,
	#[serde(default)]
	typegen: TypegenConfig,
}

/// The `[typegen]` section of `surrealkit.toml`.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct TypegenConfig {
	/// Where generated TypeScript types go. When set, TS generation is enabled:
	/// `surrealkit typegen` and `sync --watch` write them here. A path ending in
	/// `.ts`, `.mts` or `.cts` is the file itself; anything else is a directory,
	/// and the file in it is `filename`.
	pub typescript: Option<PathBuf>,
	/// The file name to write inside the `typescript` directory. Defaults to
	/// `index.ts`, which a project may want for its own barrel file.
	pub filename: Option<String>,
	/// Optional formatter command run on the generated file after writing
	/// (e.g. `"biome check --write"`, `"prettier --write"`, `"eslint --fix"`).
	/// The generated file path is appended as the final argument. Failures are
	/// non-fatal warnings.
	pub format: Option<String>,
}

impl TypegenConfig {
	/// The TypeScript file to write, if TS generation is configured.
	pub fn typescript_path(&self) -> Result<Option<PathBuf>> {
		let Some(typescript) = &self.typescript else {
			if self.filename.is_some() {
				bail!(
					"[typegen] filename is set but typescript is not; set typescript to the directory to write it in"
				);
			}
			return Ok(None);
		};
		if is_typescript_file(typescript) {
			if let Some(filename) = &self.filename {
				bail!(
					"[typegen] typescript = {:?} already names a file, so filename = {filename:?} has \
					 nothing to apply to; set one or the other",
					typescript.display().to_string()
				);
			}
			return Ok(Some(typescript.clone()));
		}
		let filename = self.filename.as_deref().unwrap_or(DEFAULT_TYPESCRIPT_FILE);
		let bare = Path::new(filename).file_name().is_some_and(|name| name == filename);
		if filename.trim().is_empty() || !bare || filename.contains('\\') {
			bail!("[typegen] filename = {filename:?} must be a file name, without a directory");
		}
		Ok(Some(typescript.join(filename)))
	}
}

/// The TypeScript file written when the configured path is a directory.
pub const DEFAULT_TYPESCRIPT_FILE: &str = "index.ts";

/// Whether `path` names a TypeScript file rather than a directory.
pub fn is_typescript_file(path: &Path) -> bool {
	path.extension().is_some_and(|ext| ext == "ts" || ext == "mts" || ext == "cts")
}

/// Load the `[typegen]` section from `surrealkit.toml`. `toml_path` defaults to
/// `./surrealkit.toml` when `None`. A missing file (or missing section) yields
/// [`TypegenConfig::default`].
pub fn load_typegen_config(toml_path: Option<&Path>) -> Result<TypegenConfig> {
	let cfg_path = toml_path.unwrap_or_else(|| Path::new("surrealkit.toml"));
	if !cfg_path.exists() {
		return Ok(TypegenConfig::default());
	}
	let raw = std::fs::read_to_string(cfg_path)?;
	let cfg: ProjectConfig = toml::from_str(&raw)?;
	Ok(cfg.typegen)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use tempfile::TempDir;

	use super::*;

	fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
		pairs.iter().map(|(k, v)| (k.to_ascii_uppercase(), v.to_string())).collect()
	}

	#[test]
	fn apply_substitutes_single_var() {
		let v = vars(&[("FOO", "bar")]);
		assert_eq!(apply("hello ${FOO} world", &v).unwrap(), "hello bar world");
	}

	#[test]
	fn apply_substitutes_multiple_vars() {
		let v = vars(&[("A", "alpha"), ("B", "beta")]);
		assert_eq!(apply("${A} and ${B}", &v).unwrap(), "alpha and beta");
	}

	#[test]
	fn apply_substitutes_repeated_var() {
		let v = vars(&[("X", "42")]);
		assert_eq!(apply("${X} + ${X}", &v).unwrap(), "42 + 42");
	}

	#[test]
	fn apply_substitutes_at_string_boundaries() {
		let v = vars(&[("X", "42")]);
		assert_eq!(apply("${X}", &v).unwrap(), "42");
		assert_eq!(apply("${X}suffix", &v).unwrap(), "42suffix");
		assert_eq!(apply("prefix${X}", &v).unwrap(), "prefix42");
		assert_eq!(apply("${X}${X}", &v).unwrap(), "4242");
	}

	#[test]
	fn apply_undefined_var_returns_error() {
		let v = vars(&[]);
		let err = apply("${MISSING}", &v).unwrap_err();
		let msg = err.to_string();
		assert!(msg.contains("MISSING"), "error should name the variable: {err}");
		assert!(msg.contains("not defined"), "error should explain the cause: {err}");
		assert!(msg.contains("--var"), "error should hint at remediation: {err}");
	}

	#[test]
	fn apply_undefined_var_error_normalizes_case() {
		// Even when the user wrote ${foo} (lowercase), the error names the canonical key.
		let v = vars(&[]);
		let err = apply("${foo}", &v).unwrap_err();
		assert!(err.to_string().contains("FOO"), "error should use canonical UPPER name: {err}");
	}

	#[test]
	fn apply_returns_first_undefined_variable() {
		// When multiple tokens are undefined, the error names the first one encountered.
		let v = vars(&[]);
		let err = apply("${FIRST} ${SECOND}", &v).unwrap_err();
		assert!(
			err.to_string().contains("FIRST"),
			"error should name the first undefined var: {err}"
		);
	}

	#[test]
	fn apply_escape_sequence_is_literal() {
		let v = vars(&[]);
		assert_eq!(apply("$${NOOP}", &v).unwrap(), "${NOOP}");
	}

	#[test]
	fn apply_escape_coexists_with_substitution() {
		let v = vars(&[("FOO", "bar")]);
		assert_eq!(apply("$${NOOP} ${FOO}", &v).unwrap(), "${NOOP} bar");
	}

	#[test]
	fn apply_escape_does_not_consume_undefined_var() {
		// The escape `$${X}` must not trigger an undefined-variable error.
		let v = vars(&[]);
		assert_eq!(apply("$${X}", &v).unwrap(), "${X}");
	}

	#[test]
	fn apply_case_insensitive_key() {
		let v = vars(&[("FOO", "val")]);
		assert_eq!(apply("${foo}", &v).unwrap(), "val");
		assert_eq!(apply("${Foo}", &v).unwrap(), "val");
		assert_eq!(apply("${FOO}", &v).unwrap(), "val");
	}

	#[test]
	fn apply_empty_vars_plain_sql_is_noop() {
		let v = vars(&[]);
		let sql = "DEFINE TABLE user SCHEMAFULL;";
		assert_eq!(apply(sql, &v).unwrap(), sql);
	}

	#[test]
	fn apply_empty_vars_with_token_errors() {
		let v = vars(&[]);
		assert!(apply("${X}", &v).is_err());
	}

	#[test]
	fn apply_empty_string_is_noop() {
		let v = vars(&[("FOO", "bar")]);
		assert_eq!(apply("", &v).unwrap(), "");
	}

	#[test]
	fn apply_empty_var_name_not_matched() {
		// ${} has no captured group (regex requires [^}]+), so it passes through literally.
		let v = vars(&[]);
		assert_eq!(apply("${}", &v).unwrap(), "${}");
	}

	#[test]
	fn apply_unterminated_token_passes_through() {
		// ${FOO without closing } is not a token; pass through.
		let v = vars(&[("FOO", "bar")]);
		assert_eq!(apply("${FOO no close", &v).unwrap(), "${FOO no close");
	}

	#[test]
	fn apply_empty_dollar_at_end_passes_through() {
		let v = vars(&[]);
		assert_eq!(apply("trailing $", &v).unwrap(), "trailing $");
	}

	#[test]
	fn apply_preserves_surrounding_text() {
		let v = vars(&[("TABLE", "users")]);
		let input = "DEFINE TABLE ${TABLE} SCHEMAFULL;\nDEFINE FIELD id ON ${TABLE} TYPE string;";
		let expected = "DEFINE TABLE users SCHEMAFULL;\nDEFINE FIELD id ON users TYPE string;";
		assert_eq!(apply(input, &v).unwrap(), expected);
	}

	#[test]
	fn apply_value_containing_dollar_brace_is_not_re_expanded() {
		// Substitution result is not re-scanned, so values cannot reference other vars.
		let v = vars(&[("OUTER", "${INNER}"), ("INNER", "should-not-leak")]);
		assert_eq!(apply("${OUTER}", &v).unwrap(), "${INNER}");
	}

	#[test]
	fn parse_var_flag_valid() {
		assert_eq!(parse_var_flag("KEY=VALUE").unwrap(), ("KEY".to_string(), "VALUE".to_string()));
	}

	#[test]
	fn parse_var_flag_value_contains_equals() {
		// Splits at first '=' so values may contain '=' (e.g., base64, JSON).
		assert_eq!(parse_var_flag("KEY=a=b").unwrap(), ("KEY".to_string(), "a=b".to_string()));
	}

	#[test]
	fn parse_var_flag_empty_value_is_valid() {
		assert_eq!(parse_var_flag("KEY=").unwrap(), ("KEY".to_string(), String::new()));
	}

	#[test]
	fn parse_var_flag_preserves_key_case_for_caller_normalization() {
		// parse_var_flag itself does not uppercase; that's build_vars' job.
		let (k, _) = parse_var_flag("mykey=v").unwrap();
		assert_eq!(k, "mykey");
	}

	#[test]
	fn parse_var_flag_no_equals_errors() {
		let err = parse_var_flag("KEYONLY").unwrap_err();
		assert!(err.to_string().contains("KEY=VALUE"), "error should show expected format: {err}");
	}

	#[test]
	fn parse_var_flag_empty_key_errors() {
		assert!(parse_var_flag("=value").is_err());
	}

	// build_vars tests pass `Some(<path>)` so they don't pick up a `surrealkit.toml`
	// from the cwd, and use unique key prefixes so a developer's `SURREALKIT_VAR_*`
	// shell env can't influence assertions.

	#[test]
	fn build_vars_toml_fallback() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(&cfg, "[variables]\nbuild_vars_test_only_a = \"from_toml\"\n").unwrap();
		let map = build_vars(&[], Some(&cfg)).unwrap();
		assert_eq!(map.get("BUILD_VARS_TEST_ONLY_A").map(String::as_str), Some("from_toml"));
	}

	#[test]
	fn build_vars_cli_beats_toml() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(&cfg, "[variables]\nbuild_vars_test_only_b = \"from_toml\"\n").unwrap();
		let map = build_vars(
			&[("BUILD_VARS_TEST_ONLY_B".to_string(), "from_cli".to_string())],
			Some(&cfg),
		)
		.unwrap();
		assert_eq!(map.get("BUILD_VARS_TEST_ONLY_B").map(String::as_str), Some("from_cli"));
	}

	#[test]
	fn build_vars_normalizes_cli_key_to_uppercase() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		let map =
			build_vars(&[("build_vars_test_only_c".to_string(), "v".to_string())], Some(&cfg))
				.unwrap();
		assert!(map.contains_key("BUILD_VARS_TEST_ONLY_C"), "CLI key should be uppercased");
	}

	#[test]
	fn build_vars_missing_toml_is_ok() {
		let tmp = TempDir::new().unwrap();
		let nonexistent = tmp.path().join("does_not_exist.toml");
		// No CLI vars + no env-var prefix collisions expected with a unique key check below.
		let map = build_vars(&[], Some(&nonexistent)).unwrap();
		assert!(
			!map.contains_key("BUILD_VARS_TEST_ONLY_D"),
			"no spurious key should appear from a missing TOML"
		);
	}

	#[test]
	fn build_vars_empty_toml_variables_section_is_ok() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		// A surrealkit.toml that exists but has no [variables] section must not error.
		std::fs::write(&cfg, "# no variables section\n").unwrap();
		let map = build_vars(&[], Some(&cfg)).unwrap();
		assert!(!map.contains_key("BUILD_VARS_TEST_ONLY_E"));
	}

	#[test]
	fn build_vars_invalid_toml_returns_error() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(&cfg, "this is = not = valid = toml [[[").unwrap();
		let err = build_vars(&[], Some(&cfg)).unwrap_err();
		let _ = err;
	}

	#[test]
	fn load_typegen_config_reads_typescript_path() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(&cfg, "[typegen]\ntypescript = \"../src/types\"\n").unwrap();
		let parsed = load_typegen_config(Some(&cfg)).unwrap();
		assert_eq!(parsed.typescript.as_deref(), Some(Path::new("../src/types")));
	}

	#[test]
	fn load_typegen_config_reads_format_command() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(
			&cfg,
			"[typegen]\ntypescript = \"./types\"\nformat = \"biome check --write\"\n",
		)
		.unwrap();
		let parsed = load_typegen_config(Some(&cfg)).unwrap();
		assert_eq!(parsed.format.as_deref(), Some("biome check --write"));
	}

	fn typegen(typescript: Option<&str>, filename: Option<&str>) -> TypegenConfig {
		TypegenConfig {
			typescript: typescript.map(PathBuf::from),
			filename: filename.map(str::to_string),
			format: None,
		}
	}

	#[test_case::test_case(Some("libs/db/types"), None, Some("libs/db/types/index.ts") ; "directory keeps index ts")]
	#[test_case::test_case(Some("libs/db/types"), Some("schema.generated.ts"), Some("libs/db/types/schema.generated.ts") ; "directory and filename")]
	#[test_case::test_case(Some("libs/db/types/database.ts"), None, Some("libs/db/types/database.ts") ; "ts file")]
	#[test_case::test_case(Some("libs/db/types/database.mts"), None, Some("libs/db/types/database.mts") ; "mts file")]
	#[test_case::test_case(Some("libs/db/types.d"), None, Some("libs/db/types.d/index.ts") ; "dotted directory")]
	#[test_case::test_case(None, None, None ; "not configured")]
	fn typescript_path_resolves(
		typescript: Option<&str>,
		filename: Option<&str>,
		expected: Option<&str>,
	) {
		assert_eq!(
			typegen(typescript, filename).typescript_path().unwrap(),
			expected.map(PathBuf::from)
		);
	}

	#[test_case::test_case(Some("types/database.ts"), Some("other.ts"), "already names a file" ; "file and filename")]
	#[test_case::test_case(None, Some("x.ts"), "typescript is not" ; "filename alone")]
	#[test_case::test_case(Some("types"), Some("sub/x.ts"), "without a directory" ; "filename with a directory")]
	#[test_case::test_case(Some("types"), Some(".."), "without a directory" ; "filename dot dot")]
	#[test_case::test_case(Some("types"), Some(" "), "without a directory" ; "blank filename")]
	fn typescript_path_rejects(typescript: Option<&str>, filename: Option<&str>, message: &str) {
		let err = typegen(typescript, filename).typescript_path().unwrap_err().to_string();
		assert!(err.contains(message), "{err}");
	}

	#[test]
	fn load_typegen_config_reads_filename() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(
			&cfg,
			"[typegen]\ntypescript = \"types\"\nfilename = \"schema.generated.ts\"\n",
		)
		.unwrap();
		let parsed = load_typegen_config(Some(&cfg)).unwrap();
		assert_eq!(
			parsed.typescript_path().unwrap(),
			Some(PathBuf::from("types/schema.generated.ts"))
		);
	}

	#[test]
	fn load_typegen_config_missing_section_is_none() {
		let tmp = TempDir::new().unwrap();
		let cfg = tmp.path().join("surrealkit.toml");
		std::fs::write(&cfg, "[variables]\nfoo = \"bar\"\n").unwrap();
		let parsed = load_typegen_config(Some(&cfg)).unwrap();
		assert!(parsed.typescript.is_none());
	}

	#[test]
	fn load_typegen_config_missing_file_is_default() {
		let tmp = TempDir::new().unwrap();
		let missing = tmp.path().join("nope.toml");
		let parsed = load_typegen_config(Some(&missing)).unwrap();
		assert!(parsed.typescript.is_none());
	}

	// `SURREALKIT_VAR_*` env var pickup is exercised by integration usage; testing it
	// deterministically here requires process-wide env mutation, which races other tests.
	// The logic is straightforward: filter `std::env::vars()` by the prefix, strip it,
	// uppercase the key.
}
