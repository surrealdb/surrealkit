use anyhow::{Result, anyhow};
use regex::Regex;
use serde_json::Value;

use super::types::{AssertionReport, HeaderAssertionSpec, JsonAssertionSpec};

#[derive(Debug, Clone, Default)]
pub struct JsonAssertionContext {
	pub actor_auth: Option<Value>,
}

pub fn assert_json_value_with_context(
	actual: &Value,
	assertion: &JsonAssertionSpec,
	index: usize,
	ctx: &JsonAssertionContext,
) -> Result<AssertionReport> {
	let label = format!("json_assertion_{}", index + 1);
	let found = lookup_path(actual, &assertion.path);
	let exists = found.is_some();

	if let Some(expected_exists) = assertion.exists
		&& expected_exists != exists
	{
		return Ok(AssertionReport {
			name: label,
			passed: false,
			message: format!(
				"path '{}' existence mismatch: expected {} got {}",
				assertion.path, expected_exists, exists
			),
		});
	}

	if found.is_none() {
		return Ok(AssertionReport {
			name: label,
			passed: assertion.exists == Some(false),
			message: format!("path '{}' not found", assertion.path),
		});
	}

	let value = found.expect("checked above");

	if let Some(expected) = &assertion.equals
		&& value != expected
	{
		return Ok(AssertionReport {
			name: label,
			passed: false,
			message: format!("path '{}' expected {:?}, got {:?}", assertion.path, expected, value),
		});
	}

	if let Some(auth_ref) = &assertion.equals_auth {
		let Some(auth) = ctx.actor_auth.as_ref() else {
			return Ok(AssertionReport {
				name: label,
				passed: false,
				message: "actor auth is unavailable for this assertion".to_string(),
			});
		};
		let Some(expected) = lookup_auth_value(auth, auth_ref) else {
			return Ok(AssertionReport {
				name: label,
				passed: false,
				message: format!("auth reference '{}' could not be resolved", auth_ref),
			});
		};
		if value != expected {
			return Ok(AssertionReport {
				name: label,
				passed: false,
				message: format!(
					"path '{}' expected auth reference '{}' = {:?}, got {:?}",
					assertion.path, auth_ref, expected, value
				),
			});
		}
	}

	if let Some(substring) = &assertion.contains {
		let text = value_to_text(value);
		if !text.contains(substring) {
			return Ok(AssertionReport {
				name: label,
				passed: false,
				message: format!(
					"path '{}' missing substring '{}' in '{}'",
					assertion.path, substring, text
				),
			});
		}
	}

	if let Some(pattern) = &assertion.regex {
		let re = Regex::new(pattern).map_err(|e| {
			anyhow!("invalid regex '{}' for path '{}': {}", pattern, assertion.path, e)
		})?;
		let text = value_to_text(value);
		if !re.is_match(&text) {
			return Ok(AssertionReport {
				name: label,
				passed: false,
				message: format!(
					"path '{}' regex '{}' did not match '{}'",
					assertion.path, pattern, text
				),
			});
		}
	}

	Ok(AssertionReport {
		name: label,
		passed: true,
		message: format!("path '{}' assertion passed", assertion.path),
	})
}

pub fn assert_header_value(
	headers: &reqwest::header::HeaderMap,
	assertion: &HeaderAssertionSpec,
	index: usize,
) -> Result<AssertionReport> {
	let label = format!("header_assertion_{}", index + 1);
	let key = assertion.name.to_ascii_lowercase();
	let found = headers
		.iter()
		.find(|(name, _)| name.as_str().eq_ignore_ascii_case(&key))
		.map(|(_, value)| value.to_str().unwrap_or_default().to_string());
	let exists = found.is_some();

	if let Some(expected_exists) = assertion.exists
		&& exists != expected_exists
	{
		return Ok(AssertionReport {
			name: label,
			passed: false,
			message: format!(
				"header '{}' existence mismatch expected {} got {}",
				assertion.name, expected_exists, exists
			),
		});
	}

	if found.is_none() {
		return Ok(AssertionReport {
			name: label,
			passed: exists == assertion.exists.unwrap_or(false),
			message: format!("header '{}' not found", assertion.name),
		});
	}

	let value = found.expect("checked above");

	if let Some(expected) = &assertion.equals
		&& &value != expected
	{
		return Ok(AssertionReport {
			name: label,
			passed: false,
			message: format!("header '{}' expected '{}' got '{}'", assertion.name, expected, value),
		});
	}

	if let Some(part) = &assertion.contains
		&& !value.contains(part)
	{
		return Ok(AssertionReport {
			name: label,
			passed: false,
			message: format!(
				"header '{}' missing substring '{}' in '{}'",
				assertion.name, part, value
			),
		});
	}

	if let Some(pattern) = &assertion.regex {
		let re = Regex::new(pattern).map_err(|e| {
			anyhow!("invalid header regex '{}' for '{}': {}", pattern, assertion.name, e)
		})?;
		if !re.is_match(&value) {
			return Ok(AssertionReport {
				name: label,
				passed: false,
				message: format!(
					"header '{}' regex '{}' did not match '{}'",
					assertion.name, pattern, value
				),
			});
		}
	}

	Ok(AssertionReport {
		name: label,
		passed: true,
		message: format!("header '{}' assertion passed", assertion.name),
	})
}

fn value_to_text(value: &Value) -> String {
	match value {
		Value::String(v) => v.clone(),
		_ => value.to_string(),
	}
}

pub fn lookup_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
	if path.trim().is_empty() {
		return Some(value);
	}

	let mut cursor = value;
	for seg in path.split('.') {
		if seg.is_empty() {
			continue;
		}

		if let Ok(index) = seg.parse::<usize>() {
			let arr = cursor.as_array()?;
			cursor = arr.get(index)?;
			continue;
		}

		let obj = cursor.as_object()?;
		cursor = obj.get(seg)?;
	}

	Some(cursor)
}

fn lookup_auth_value<'a>(auth: &'a Value, auth_ref: &str) -> Option<&'a Value> {
	if auth_ref == "$auth" {
		return Some(auth);
	}
	let path = auth_ref.strip_prefix("$auth.")?;
	lookup_path(auth, path)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lookup_path_supports_objects_and_arrays() {
		let value: Value = serde_json::json!({
			"a": {
				"b": [
					{"c": 1},
					{"c": 2}
				]
			}
		});
		let got = lookup_path(&value, "a.b.1.c").expect("path should exist");
		assert_eq!(got, &serde_json::json!(2));
	}

	#[test]
	fn missing_json_path_fails_by_default() {
		let actual = serde_json::json!({"present": 1});
		let assertion = JsonAssertionSpec {
			path: "missing".to_string(),
			exists: None,
			equals: Some(serde_json::json!(1)),
			equals_auth: None,
			contains: None,
			regex: None,
		};

		let report = assert_json_value_with_context(
			&actual,
			&assertion,
			0,
			&JsonAssertionContext::default(),
		)
		.expect("assertion should evaluate");

		assert!(!report.passed);
		assert_eq!(report.message, "path 'missing' not found");
	}

	#[test]
	fn missing_json_path_can_be_asserted_explicitly() {
		let actual = serde_json::json!({"present": 1});
		let assertion = JsonAssertionSpec {
			path: "missing".to_string(),
			exists: Some(false),
			equals: None,
			equals_auth: None,
			contains: None,
			regex: None,
		};

		let report = assert_json_value_with_context(
			&actual,
			&assertion,
			0,
			&JsonAssertionContext::default(),
		)
		.expect("assertion should evaluate");

		assert!(report.passed, "{}", report.message);
	}

	#[test]
	fn present_json_path_match_and_mismatch_are_unchanged() {
		let actual = serde_json::json!({"present": 1});
		let matching = JsonAssertionSpec {
			path: "present".to_string(),
			exists: None,
			equals: Some(serde_json::json!(1)),
			equals_auth: None,
			contains: None,
			regex: None,
		};
		let mismatching = JsonAssertionSpec {
			equals: Some(serde_json::json!(2)),
			..matching.clone()
		};

		let matched =
			assert_json_value_with_context(&actual, &matching, 0, &JsonAssertionContext::default())
				.expect("matching assertion should evaluate");
		let mismatched = assert_json_value_with_context(
			&actual,
			&mismatching,
			1,
			&JsonAssertionContext::default(),
		)
		.expect("mismatching assertion should evaluate");

		assert!(matched.passed, "{}", matched.message);
		assert!(!mismatched.passed);
		assert!(mismatched.message.contains("expected Number(2), got Number(1)"));
	}

	#[test]
	fn assertion_can_compare_against_auth_reference() {
		let actual = serde_json::json!({
			"owner": "user:alice"
		});
		let assertion = JsonAssertionSpec {
			path: "owner".to_string(),
			exists: None,
			equals: None,
			equals_auth: Some("$auth.id".to_string()),
			contains: None,
			regex: None,
		};
		let ctx = JsonAssertionContext {
			actor_auth: Some(serde_json::json!({
				"id": "user:alice",
				"email": "alice@example.com"
			})),
		};

		let report =
			assert_json_value_with_context(&actual, &assertion, 0, &ctx).expect("assertion ok");
		assert!(report.passed, "{}", report.message);
	}

	#[test]
	fn assertion_can_compare_against_entire_auth_object() {
		let actual = serde_json::json!({
			"id": "user:alice",
			"email": "alice@example.com"
		});
		let assertion = JsonAssertionSpec {
			path: "".to_string(),
			exists: None,
			equals: None,
			equals_auth: Some("$auth".to_string()),
			contains: None,
			regex: None,
		};
		let ctx = JsonAssertionContext {
			actor_auth: Some(actual.clone()),
		};

		let report =
			assert_json_value_with_context(&actual, &assertion, 0, &ctx).expect("assertion ok");
		assert!(report.passed, "{}", report.message);
	}
}
