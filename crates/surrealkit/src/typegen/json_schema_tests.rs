//! The published JSON Schema for schema document v2
//! (`schema/schema-document-v2.schema.json`, exported as
//! [`SCHEMA_DOCUMENT_V2_JSON_SCHEMA`](super::SCHEMA_DOCUMENT_V2_JSON_SCHEMA)) is enforced: the
//! contract example, every document generated from the corpus snapshots, the plain-`INFO`
//! fallback's documents and a hermetically compiled corpus all validate against it.
//!
//! SurrealKit takes no new dependency for this, so the check is a small JSON Schema
//! (draft 2020-12) validator covering exactly the keywords the published schema uses. Any
//! other keyword is a validation error, so the schema cannot grow a keyword this test would
//! silently skip.

use regex::Regex;
use serde_json::{Map, Value};

use super::corpus_tests::{corpus_dir, read_json};
use super::corpus_v2_tests::{fixture_document, fixture_snapshot};
use super::introspect_v2::assemble_from_info;

/// Keywords that only annotate.
const ANNOTATIONS: &[&str] = &["$schema", "$id", "title", "description", "$defs"];

struct Validator<'s> {
	root: &'s Value,
}

impl Validator<'_> {
	fn resolve(&self, reference: &str) -> Result<&Value, String> {
		let name = reference
			.strip_prefix("#/$defs/")
			.ok_or_else(|| format!("unsupported $ref {reference}"))?;
		self.root["$defs"].get(name).ok_or_else(|| format!("unresolved $ref {reference}"))
	}

	/// Every error validating `value` at `at` against `schema`.
	fn validate(&self, schema: &Value, value: &Value, at: &str, errors: &mut Vec<String>) {
		let Some(schema) = schema.as_object() else {
			errors.push(format!("{at}: schema is not an object"));
			return;
		};
		for (keyword, rule) in schema {
			self.keyword(schema, keyword, rule, value, at, errors);
		}
	}

	fn valid(&self, schema: &Value, value: &Value, at: &str) -> bool {
		let mut errors = Vec::new();
		self.validate(schema, value, at, &mut errors);
		errors.is_empty()
	}

	fn keyword(
		&self,
		schema: &Map<String, Value>,
		keyword: &str,
		rule: &Value,
		value: &Value,
		at: &str,
		errors: &mut Vec<String>,
	) {
		match keyword {
			k if ANNOTATIONS.contains(&k) => {}
			"$ref" => match rule.as_str().map(|r| self.resolve(r)) {
				Some(Ok(target)) => self.validate(target, value, at, errors),
				Some(Err(err)) => errors.push(format!("{at}: {err}")),
				None => errors.push(format!("{at}: $ref is not a string")),
			},
			"type" => {
				let types: Vec<&str> = match rule {
					Value::String(t) => vec![t.as_str()],
					Value::Array(ts) => ts.iter().filter_map(Value::as_str).collect(),
					_ => vec![],
				};
				if !types.iter().any(|t| type_matches(t, value)) {
					errors.push(format!("{at}: expected {types:?}, found {value}"));
				}
			}
			"const" => {
				if value != rule {
					errors.push(format!("{at}: expected {rule}, found {value}"));
				}
			}
			"enum" => {
				if !rule.as_array().is_some_and(|options| options.contains(value)) {
					errors.push(format!("{at}: {value} is not one of {rule}"));
				}
			}
			"required" => {
				if let (Some(object), Some(keys)) = (value.as_object(), rule.as_array()) {
					for key in keys.iter().filter_map(Value::as_str) {
						if !object.contains_key(key) {
							errors.push(format!("{at}: missing required property `{key}`"));
						}
					}
				}
			}
			"properties" => {
				if let (Some(object), Some(props)) = (value.as_object(), rule.as_object()) {
					for (key, sub) in props {
						if let Some(v) = object.get(key) {
							self.validate(sub, v, &format!("{at}.{key}"), errors);
						}
					}
				}
			}
			"additionalProperties" => {
				if let Some(object) = value.as_object() {
					let declared = schema.get("properties").and_then(Value::as_object);
					for (key, v) in object {
						if declared.is_some_and(|d| d.contains_key(key)) {
							continue;
						}
						match rule {
							Value::Bool(false) => {
								errors.push(format!("{at}: unexpected property `{key}`"))
							}
							Value::Bool(true) => {}
							sub => self.validate(sub, v, &format!("{at}.{key}"), errors),
						}
					}
				}
			}
			"items" => {
				if let Some(items) = value.as_array() {
					for (i, item) in items.iter().enumerate() {
						self.validate(rule, item, &format!("{at}[{i}]"), errors);
					}
				}
			}
			"minItems" => {
				if let (Some(items), Some(min)) = (value.as_array(), rule.as_u64())
					&& (items.len() as u64) < min
				{
					errors.push(format!("{at}: fewer than {min} items"));
				}
			}
			"uniqueItems" => {
				if let Some(items) = value.as_array()
					&& rule == &Value::Bool(true)
				{
					for (i, a) in items.iter().enumerate() {
						if items[i + 1..].contains(a) {
							errors.push(format!("{at}: duplicate item {a}"));
						}
					}
				}
			}
			"minLength" => {
				if let (Some(s), Some(min)) = (value.as_str(), rule.as_u64())
					&& (s.chars().count() as u64) < min
				{
					errors.push(format!("{at}: shorter than {min}"));
				}
			}
			"minimum" => {
				if let (Some(n), Some(min)) = (value.as_f64(), rule.as_f64())
					&& n < min
				{
					errors.push(format!("{at}: {n} is below {min}"));
				}
			}
			"pattern" => {
				if let (Some(s), Some(pattern)) = (value.as_str(), rule.as_str()) {
					match Regex::new(pattern) {
						Ok(re) if re.is_match(s) => {}
						Ok(_) => errors.push(format!("{at}: {s:?} does not match {pattern}")),
						Err(err) => errors.push(format!("{at}: bad pattern {pattern}: {err}")),
					}
				}
			}
			"oneOf" => {
				let matching = rule.as_array().map_or(0, |options| {
					options.iter().filter(|o| self.valid(o, value, at)).count()
				});
				if matching != 1 {
					errors.push(format!(
						"{at}: matches {matching} of oneOf, expected exactly 1: {value}"
					));
				}
			}
			"anyOf" => {
				if !rule
					.as_array()
					.is_some_and(|options| options.iter().any(|o| self.valid(o, value, at)))
				{
					errors.push(format!("{at}: matches none of anyOf: {value}"));
				}
			}
			"if" => {
				let branch = if self.valid(rule, value, at) {
					schema.get("then")
				} else {
					schema.get("else")
				};
				if let Some(branch) = branch {
					self.validate(branch, value, at, errors);
				}
			}
			"then" | "else" => {} // applied through "if"
			other => errors.push(format!("{at}: unsupported keyword `{other}`")),
		}
	}
}

fn type_matches(t: &str, value: &Value) -> bool {
	match t {
		"object" => value.is_object(),
		"array" => value.is_array(),
		"string" => value.is_string(),
		"boolean" => value.is_boolean(),
		"integer" => value.as_i64().is_some() || value.as_u64().is_some(),
		"number" => value.is_number(),
		"null" => value.is_null(),
		_ => false,
	}
}

fn schema() -> Value {
	serde_json::from_str(super::SCHEMA_DOCUMENT_V2_JSON_SCHEMA)
		.expect("the published schema is JSON")
}

/// Every validation error of `document`.
pub(crate) fn validation_errors(document: &Value) -> Vec<String> {
	let schema = schema();
	let validator = Validator {
		root: &schema,
	};
	let mut errors = Vec::new();
	validator.validate(&schema, document, "$", &mut errors);
	errors
}

pub(crate) fn assert_valid(label: &str, document: &Value) {
	let errors = validation_errors(document);
	assert!(errors.is_empty(), "{label} does not validate:\n{}", errors.join("\n"));
}

#[test]
fn the_published_schema_is_the_crate_file() {
	let on_disk = std::fs::read_to_string(
		std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("schema/schema-document-v2.schema.json"),
	)
	.unwrap();
	assert_eq!(on_disk, super::SCHEMA_DOCUMENT_V2_JSON_SCHEMA);
	assert_eq!(schema()["$schema"], "https://json-schema.org/draft/2020-12/schema");
}

#[test]
fn the_contract_example_validates() {
	assert_valid("example.v2.json", &read_json(&corpus_dir().join("example.v2.json")));
}

#[test]
fn every_generated_corpus_document_validates() {
	for version in ["3.2.4", "3.3.0"] {
		let doc = serde_json::to_value(fixture_document(version)).unwrap();
		assert_valid(&format!("the {version} document"), &doc);
		let fallback =
			serde_json::to_value(assemble_from_info(&fixture_snapshot(version)).unwrap()).unwrap();
		assert_valid(&format!("the {version} plain-INFO document"), &fallback);
	}
}

#[test]
fn legacy_access_documents_without_authenticate_round_trip_unchanged() {
	let value = serde_json::to_value(fixture_document("3.3.0")).unwrap();
	assert!(value["accesses"].as_array().unwrap().iter().all(|a| a.get("authenticate").is_none()));
	let decoded: super::document::SchemaDocument = serde_json::from_value(value.clone()).unwrap();
	assert!(decoded.accesses.iter().all(|a| a.authenticate.is_none()));
	let encoded = serde_json::to_value(decoded).unwrap();
	assert_eq!(encoded, value);
	assert_eq!(super::canonical::semantic_digest(&encoded).unwrap(), value["semanticDigest"]);
}

/// The validator is not vacuous: each of these breaks the contract and must be caught.
#[test]
fn contract_violations_are_rejected() {
	let base = serde_json::to_value(fixture_document("3.3.0")).unwrap();
	type Mutation = (&'static str, Box<dyn Fn(&mut Value)>);
	let mutations: Vec<Mutation> = vec![
		("unknown top-level key", Box::new(|d| d["generatedAt"] = Value::from("now"))),
		("bad digest", Box::new(|d| d["semanticDigest"] = Value::from("sha256:xyz"))),
		("version 1", Box::new(|d| d["version"] = Value::from(1))),
		(
			"missing collection",
			Box::new(|d| {
				d.as_object_mut().unwrap().remove("buckets");
			}),
		),
		(
			"unknown kind",
			Box::new(|d| {
				d["tables"][0]["fields"] = serde_json::json!([{
					"path": ["x"], "kind": {"kind": "frob"}, "optional": false, "flexible": false,
					"readonly": false, "inline": false,
					"permissions": {"select": true, "create": true, "update": true}
				}])
			}),
		),
		("null instead of omitted", Box::new(|d| d["tables"][0]["comment"] = Value::Null)),
		(
			"one-variant union",
			Box::new(|d| {
				d["functions"][0]["returns"] =
					serde_json::json!({"kind": "union", "variants": [{"kind": "int"}]})
			}),
		),
		(
			"non-string literal",
			Box::new(|d| {
				d["functions"][0]["returns"] =
					serde_json::json!({"kind": "literal", "type": "int", "value": 5})
			}),
		),
		("secret on an access", Box::new(|d| d["accesses"][0]["key"] = Value::from("[REDACTED]"))),
		("null authenticate", Box::new(|d| d["accesses"][0]["authenticate"] = Value::Null)),
		(
			"non-string authenticate",
			Box::new(|d| d["accesses"][0]["authenticate"] = Value::from(1)),
		),
		(
			"unknown adapter",
			Box::new(|d| d["provenance"]["adapter"] = Value::from("structure-9.9")),
		),
		("bad function name", Box::new(|d| d["functions"][0]["name"] = Value::from("greet"))),
	];
	assert_valid("the base document", &base);
	for (label, mutate) in mutations {
		let mut doc = base.clone();
		mutate(&mut doc);
		assert!(!validation_errors(&doc).is_empty(), "{label} was accepted");
	}
}

#[test]
fn an_unsupported_keyword_is_an_error_not_a_skip() {
	let validator_schema = serde_json::json!({ "type": "object", "maxProperties": 1 });
	let v = Validator {
		root: &validator_schema,
	};
	let mut errors = Vec::new();
	v.validate(&validator_schema, &serde_json::json!({}), "$", &mut errors);
	assert!(errors.iter().any(|e| e.contains("unsupported keyword `maxProperties`")), "{errors:?}");
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn a_compiled_document_validates() {
	use crate::compile::{CompileModule, CompileOpts, compile};
	use crate::schema_state::SchemaFile;
	let files = super::corpus_tests::corpus_modules()
		.into_iter()
		.filter(|m| m.engines.as_ref().is_none_or(|e| e.iter().any(|v| v == "3.3")))
		.map(|m| {
			let sql = std::fs::read_to_string(corpus_dir().join(&m.path)).unwrap();
			CompileModule {
				name: m.path.clone(),
				files: vec![SchemaFile {
					path: m.path,
					hash: String::new(),
					sql,
				}],
			}
		})
		.collect::<Vec<_>>();
	let opts = CompileOpts {
		experimental: vec!["files".into(), "surrealism".into()],
		..CompileOpts::default()
	};
	let doc = compile(&files, &opts).await.expect("compile").document;
	assert_valid("the compiled corpus", &serde_json::to_value(doc).unwrap());
}
