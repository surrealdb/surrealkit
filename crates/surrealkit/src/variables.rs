use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Result, anyhow, bail};
use regex::Regex;
use serde::Deserialize;

static VAR_REGEX: OnceLock<Regex> = OnceLock::new();

fn var_regex() -> &'static Regex {
    VAR_REGEX.get_or_init(|| {
        Regex::new(r"\$\$\{[^}]+\}|\$\{([^}]+)\}").expect("invalid variable regex")
    })
}

/// Substitutes all `${VAR_NAME}` tokens in `content` using `vars`.
///
/// - Keys in `vars` must already be normalized to `UPPER_CASE`.
/// - Variable name lookup is case-insensitive; `${foo}`, `${Foo}`, and `${FOO}` all match key `FOO`.
/// - `$${VAR_NAME}` is an escape sequence that yields the literal `${VAR_NAME}` (no substitution).
/// - Returns an error naming the first undefined variable encountered.
///
/// When `content` contains no `${`, this returns the original string without scanning further.
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
            // Escape: $${NAME} → literal ${NAME}. Strip the leading '$'.
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

/// Builds the merged variable map from all three sources in priority order.
///
/// Priority (highest wins): `cli_vars` > `SURREALKIT_VAR_*` env vars > `surrealkit.toml [variables]`.
/// All keys are normalized to `UPPER_CASE`.
///
/// `toml_path` defaults to `./surrealkit.toml` when `None`.
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

/// Parses a `KEY=VALUE` string from a `--var` CLI flag.
///
/// The split occurs at the first `=`. A missing `=` is an error; an empty value is allowed.
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
    /// Applies template variable substitution to `content`.
    ///
    /// When `vars` is empty and `content` has no `${` tokens, returns the original string
    /// without any allocation. Any undefined `${VAR}` token is an error.
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_ascii_uppercase(), v.to_string())).collect()
    }

    // --- apply ---

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
        assert!(err.to_string().contains("FIRST"), "error should name the first undefined var: {err}");
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
        // ${FOO without closing } is not a token — emit literally.
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
        // Substitution result is not re-scanned, preventing accidental recursion attacks.
        let v = vars(&[("OUTER", "${INNER}"), ("INNER", "should-not-leak")]);
        assert_eq!(apply("${OUTER}", &v).unwrap(), "${INNER}");
    }

    // --- parse_var_flag ---

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
        // parse_var_flag itself does not uppercase — that's build_vars' responsibility.
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

    // --- build_vars ---
    //
    // These tests pass `Some(<path>)` rather than `None` so they don't accidentally read
    // from a `surrealkit.toml` in the cwd. They also use unique key prefixes so a
    // developer-set `SURREALKIT_VAR_FOO` in their shell can't influence the result.

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
        let map = build_vars(
            &[("build_vars_test_only_c".to_string(), "v".to_string())],
            Some(&cfg),
        )
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
        let _ = err; // we don't assert on the message — just that it errors
    }

    // Note: `SURREALKIT_VAR_*` env var pickup is exercised by integration usage and is hard
    // to test deterministically here without process-wide env mutation (which races other
    // tests). The behavior is straightforward: `std::env::vars()` is filtered for the
    // prefix, the prefix is stripped, and the key is uppercased — see lines 86-91.
}
