//! Canonical JSON and the schema document v2 `semanticDigest`.
//!
//! `canonical` is the JSON Canonicalization Scheme (RFC 8785) restricted to what a schema
//! document contains — objects with ASCII keys, arrays, strings, booleans and integers — which
//! reduces it to: keys sorted, no whitespace, integers in plain decimal, and strings escaped as
//! ECMAScript `JSON.stringify` does. A non-integer number is a bug in the document and fails.
//!
//! `semanticDigest = "sha256:" + hex(sha256(canonical(projection(document))))`, where the
//! projection removes the top-level `semanticDigest`, `provenance` and `diagnostics`, `define`
//! on every table and field, and the derived `value.readsInput` and `assert.recognized` on
//! every field (contract section 13).

use anyhow::{Result, bail};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Append the canonical JSON of `value` to `out`.
fn write_canonical(value: &Value, out: &mut String) -> Result<()> {
	match value {
		Value::Null => out.push_str("null"),
		Value::Bool(b) => out.push_str(if *b {
			"true"
		} else {
			"false"
		}),
		Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				out.push_str(&i.to_string());
			} else if let Some(u) = n.as_u64() {
				out.push_str(&u.to_string());
			} else {
				bail!(
					"canonical JSON: the non-integer number {n} has no place in a schema document"
				);
			}
		}
		Value::String(s) => write_string(s, out),
		Value::Array(items) => {
			out.push('[');
			for (i, item) in items.iter().enumerate() {
				if i > 0 {
					out.push(',');
				}
				write_canonical(item, out)?;
			}
			out.push(']');
		}
		Value::Object(map) => {
			// Sorted by UTF-16 code units per RFC 8785; for the ASCII keys a schema
			// document has, that is byte order. Sorting here does not depend on how the
			// map happens to be ordered.
			let mut keys: Vec<&String> = map.keys().collect();
			keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
			out.push('{');
			for (i, key) in keys.iter().enumerate() {
				if i > 0 {
					out.push(',');
				}
				write_string(key, out);
				out.push(':');
				write_canonical(&map[key.as_str()], out)?;
			}
			out.push('}');
		}
	}
	Ok(())
}

/// A JSON string literal escaped exactly as ECMAScript `JSON.stringify` escapes it.
fn write_string(s: &str, out: &mut String) {
	out.push('"');
	for c in s.chars() {
		match c {
			'"' => out.push_str("\\\""),
			'\\' => out.push_str("\\\\"),
			'\u{08}' => out.push_str("\\b"),
			'\u{0C}' => out.push_str("\\f"),
			'\n' => out.push_str("\\n"),
			'\r' => out.push_str("\\r"),
			'\t' => out.push_str("\\t"),
			c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
			c => out.push(c),
		}
	}
	out.push('"');
}

/// The canonical JSON of `value`.
pub fn canonical_json(value: &Value) -> Result<String> {
	let mut out = String::new();
	write_canonical(value, &mut out)?;
	Ok(out)
}

/// The canonical JSON of any serialisable value.
pub fn canonical_of<T: Serialize>(value: &T) -> Result<String> {
	canonical_json(&serde_json::to_value(value)?)
}

/// The semantic projection of a document already converted to JSON.
pub fn projection(document: &Value) -> Value {
	let mut doc = document.clone();
	if let Value::Object(map) = &mut doc {
		map.remove("semanticDigest");
		map.remove("provenance");
		map.remove("diagnostics");
		if let Some(Value::Array(tables)) = map.get_mut("tables") {
			for table in tables {
				let Value::Object(table) = table else {
					continue;
				};
				table.remove("define");
				let Some(Value::Array(fields)) = table.get_mut("fields") else {
					continue;
				};
				for field in fields {
					let Value::Object(field) = field else {
						continue;
					};
					field.remove("define");
					if let Some(Value::Object(value)) = field.get_mut("value") {
						value.remove("readsInput");
					}
					if let Some(Value::Object(assert)) = field.get_mut("assert") {
						assert.remove("recognized");
					}
				}
			}
		}
	}
	doc
}

/// `sha256:<hex>` over the canonical JSON of `document`'s semantic projection. `document` is
/// a schema document v2 as JSON; whatever `semanticDigest` it carries is ignored.
pub fn semantic_digest(document: &Value) -> Result<String> {
	let canonical = canonical_json(&projection(document))?;
	Ok(format!("sha256:{}", hex::encode(Sha256::digest(canonical.as_bytes()))))
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn keys_sort_and_strings_escape_like_json_stringify() {
		let value = json!({
			"b": [1, -2, true, null],
			"a": "quote \" backslash \\ nl \n tab \t ctrl \u{1} del \u{7f} é 😀 \u{2028}",
		});
		assert_eq!(
			canonical_json(&value).unwrap(),
			"{\"a\":\"quote \\\" backslash \\\\ nl \\n tab \\t ctrl \\u0001 del \u{7f} é 😀 \u{2028}\",\"b\":[1,-2,true,null]}"
		);
	}

	#[test]
	fn non_integer_numbers_are_rejected() {
		assert!(canonical_json(&json!({ "x": 1.5 })).is_err());
		assert!(canonical_json(&json!([0.0])).is_err());
		assert_eq!(canonical_json(&json!(u64::MAX)).unwrap(), u64::MAX.to_string());
	}

	#[test]
	fn the_projection_drops_exactly_the_non_semantic_parts() {
		let doc = json!({
			"version": 2,
			"semanticDigest": "sha256:00",
			"provenance": { "x": 1 },
			"diagnostics": [{ "code": "x" }],
			"tables": [{
				"name": "t",
				"define": "DEFINE TABLE t",
				"fields": [{
					"path": ["f"],
					"define": "DEFINE FIELD f ON t",
					"value": { "expr": "$value", "readsInput": true },
					"assert": { "expr": "true", "recognized": [] },
					"comment": "kept",
				}],
			}],
		});
		assert_eq!(
			projection(&doc),
			json!({
				"version": 2,
				"tables": [{
					"name": "t",
					"fields": [{
						"path": ["f"],
						"value": { "expr": "$value" },
						"assert": { "expr": "true" },
						"comment": "kept",
					}],
				}],
			})
		);
	}

	/// The contract's example document carries a digest that two independent implementations
	/// agree on: this one and effect-surrealkit's (TypeScript), whose suite pins the same value.
	#[test]
	fn the_example_document_digest_matches_the_independent_implementations() {
		let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("tests/fixtures/typegen_corpus/example.v2.json");
		let doc: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
		let recorded = doc["semanticDigest"].as_str().unwrap().to_string();
		assert_eq!(semantic_digest(&doc).unwrap(), recorded);
		assert_eq!(
			recorded,
			"sha256:9bb3a0a15eb772d737b5dd170653655b97e4833995b53d272207288b37ee0f37"
		);
	}
}
