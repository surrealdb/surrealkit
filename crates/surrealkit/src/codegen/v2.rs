//! The engine's kinds (`kind_value`, the `surrealdb_types::Kind` serde form) as schema
//! document v2 kinds.
//!
//! A kind is lowered into the [`FieldType`] tree the v2 kind parser produces from kind
//! text, and then normalised by the same code (`typegen::kind_v2`), so a kind reaches the
//! document in one spelling whichever way it was read. The kinds that tree has no node for
//! (sized numbers, `date`, `time`, `timestamp`, `interval`, `bigdecimal`, `enum` and named
//! types) are lowered as their engine spelling, which `kind_v2` reads into their v2 nodes.
//!
//! A function's signature is dropped, because v2's `function` is bare. Variants this build
//! does not know become `{ kind: "unknown", source }`, and are listed in
//! [`V2Kind::unrepresented`] so the caller can report them.

use surrealdb_types::ToSql;

use super::document::{KindValue, LiteralValue};
use crate::typegen::document::Kind;
use crate::typegen::kind_v2;
use crate::typegen::{FieldType, LiteralType, ObjectField, PrimitiveType};

/// A kind in v2 form.
#[derive(Debug, Clone, PartialEq)]
pub struct V2Kind {
	pub kind: Kind,
	/// The kind admitted NONE, and NONE was folded into this flag (top positions only).
	pub optional: bool,
	/// The `source` of every `unknown` in `kind`, in the order met.
	pub unrepresented: Vec<String>,
}

/// `kind` in a top-level position (a field, a literal-object field, a function argument,
/// a statement's result): a `none` variant becomes `optional`.
pub fn top(kind: &KindValue) -> V2Kind {
	let (kind, optional) = kind_v2::top(&field_type(kind));
	V2Kind {
		unrepresented: unknown_sources(&kind),
		kind,
		optional,
	}
}

/// `kind` in a nested position (a tuple item, an array or set item, a `DEFINE TYPE`'s kind):
/// NONE becomes `{ kind: "option", item }`, and `optional` is always `false`.
pub fn nested(kind: &KindValue) -> V2Kind {
	let kind = kind_v2::nested(&field_type(kind));
	V2Kind {
		unrepresented: unknown_sources(&kind),
		kind,
		optional: false,
	}
}

fn unknown_sources(kind: &Kind) -> Vec<String> {
	fn walk(kind: &Kind, out: &mut Vec<String>) {
		match kind {
			Kind::Unknown {
				source,
			} => out.push(source.clone()),
			Kind::Array {
				item,
				..
			}
			| Kind::Set {
				item,
				..
			}
			| Kind::Option {
				item,
			} => walk(item, out),
			Kind::Tuple {
				items: kinds,
			}
			| Kind::Union {
				variants: kinds,
			} => kinds.iter().for_each(|k| walk(k, out)),
			Kind::ObjectLiteral {
				fields,
			} => fields.iter().for_each(|f| walk(&f.kind, out)),
			_ => {}
		}
	}
	let mut out = Vec::new();
	walk(kind, &mut out);
	out
}

fn primitive(name: PrimitiveType) -> FieldType {
	FieldType::Primitive {
		name,
	}
}

/// A kind as its SurrealQL spelling, which `kind_v2` lowers to its node, or to `unknown`.
fn spelled(source: impl Into<String>) -> FieldType {
	FieldType::Unknown {
		source: source.into(),
	}
}

/// `text` quoted with `quote`, escaped so the kind parser reads back exactly `text`.
fn quoted(text: &str, quote: char) -> String {
	let mut out = String::with_capacity(text.len() + 2);
	out.push(quote);
	for c in text.chars() {
		if c == '\\' || c == quote {
			out.push('\\');
		}
		out.push(c);
	}
	out.push(quote);
	out
}

fn literal(value: serde_json::Value, literal_type: Option<LiteralType>) -> FieldType {
	FieldType::Literal {
		value,
		literal_type,
	}
}

fn field_type(kind: &KindValue) -> FieldType {
	let many = |kinds: &[KindValue]| -> Vec<FieldType> { kinds.iter().map(field_type).collect() };
	match kind {
		KindValue::Any => primitive(PrimitiveType::Any),
		KindValue::None => primitive(PrimitiveType::None),
		KindValue::Null => primitive(PrimitiveType::Null),
		KindValue::Bool => primitive(PrimitiveType::Bool),
		KindValue::Bytes => primitive(PrimitiveType::Bytes),
		KindValue::Datetime => primitive(PrimitiveType::Datetime),
		KindValue::Decimal => primitive(PrimitiveType::Decimal),
		KindValue::Duration => primitive(PrimitiveType::Duration),
		KindValue::Number => primitive(PrimitiveType::Number),
		KindValue::Object => primitive(PrimitiveType::Object),
		KindValue::String => primitive(PrimitiveType::String),
		KindValue::Uuid => primitive(PrimitiveType::Uuid),
		KindValue::Regex => primitive(PrimitiveType::Regex),
		KindValue::Range => primitive(PrimitiveType::Range),
		KindValue::Float => primitive(PrimitiveType::Float),
		KindValue::Int => primitive(PrimitiveType::Int),
		KindValue::F16 => spelled("f16"),
		KindValue::F32 => spelled("f32"),
		KindValue::I8 => spelled("i8"),
		KindValue::I16 => spelled("i16"),
		KindValue::I32 => spelled("i32"),
		KindValue::U8 => spelled("u8"),
		KindValue::U16 => spelled("u16"),
		KindValue::U32 => spelled("u32"),
		KindValue::U64 => spelled("u64"),
		KindValue::Date => spelled("date"),
		KindValue::Time => spelled("time"),
		KindValue::Timestamp => spelled("timestamp"),
		KindValue::Interval => spelled("interval"),
		KindValue::BigDecimal => spelled("bigdecimal"),
		KindValue::Enum(labels) => spelled(format!(
			"enum<{}>",
			labels.iter().map(|label| quoted(label, '\'')).collect::<Vec<_>>().join(", ")
		)),
		// Quoted, so a name that is also a kind word (`date`) still reads as a name.
		KindValue::Named(name) => spelled(quoted(name, '`')),
		// JSON text is no kind spelling, so it stays unknown.
		KindValue::Unknown(raw) => spelled(raw.to_string()),
		KindValue::Function(..) => primitive(PrimitiveType::Function),
		KindValue::Table(tables) => FieldType::Table {
			tables: tables.clone(),
		},
		KindValue::Record(tables) => FieldType::Record {
			tables: tables.clone(),
		},
		KindValue::File(buckets) => FieldType::File {
			buckets: buckets.clone(),
		},
		// The engine's variant names (`MultiLine`) are v2's geometry types in lower case.
		KindValue::Geometry(kinds) => FieldType::Geometry {
			kinds: kinds.iter().map(|k| k.to_ascii_lowercase()).collect(),
		},
		KindValue::Either(kinds) => FieldType::Union {
			variants: many(kinds),
		},
		KindValue::Array(item, max) => FieldType::Array {
			inner: Box::new(field_type(item)),
			max: *max,
		},
		KindValue::Set(item, max) => FieldType::Set {
			inner: Box::new(field_type(item)),
			max: *max,
		},
		KindValue::Literal(value) => match value {
			LiteralValue::String(s) => literal(serde_json::Value::String(s.clone()), None),
			LiteralValue::Integer(i) => literal(serde_json::Value::from(*i), None),
			LiteralValue::Float(f) => match serde_json::Number::from_f64(*f) {
				Some(n) => literal(serde_json::Value::Number(n), Some(LiteralType::Float)),
				// NaN and the infinities have no JSON number; the parser keeps their spelling.
				None => literal(
					serde_json::Value::String(
						match (f.is_nan(), f.is_sign_negative()) {
							(true, _) => "NaN",
							(false, false) => "Infinity",
							(false, true) => "-Infinity",
						}
						.to_string(),
					),
					Some(LiteralType::Float),
				),
			},
			LiteralValue::Decimal(text) => {
				literal(serde_json::Value::String(text.clone()), Some(LiteralType::Decimal))
			}
			LiteralValue::Duration {
				secs,
				nanos,
			} => literal(
				serde_json::Value::String(surrealdb_types::Duration::new(*secs, *nanos).to_sql()),
				Some(LiteralType::Duration),
			),
			LiteralValue::Bool(b) => literal(serde_json::Value::Bool(*b), None),
			LiteralValue::Array(items) => FieldType::Tuple {
				items: many(items),
			},
			LiteralValue::Object(fields) => FieldType::Object {
				fields: fields
					.iter()
					.map(|(name, kind)| ObjectField {
						name: name.clone(),
						r#type: field_type(kind),
					})
					.collect(),
			},
		},
	}
}

#[cfg(test)]
mod tests {
	use serde_json::{Value, json};

	use super::*;

	fn value(raw: Value) -> KindValue {
		serde_json::from_value(raw).expect("kind_value")
	}

	fn v2(raw: Value) -> Value {
		serde_json::to_value(top(&value(raw)).kind).expect("v2 kind")
	}

	#[test]
	fn an_option_folds_into_optional_at_the_top_and_nests_as_option_below() {
		let option = json!({ "Either": ["None", "String"] });
		let converted = top(&value(option.clone()));
		assert_eq!(converted.kind, Kind::String);
		assert!(converted.optional);
		assert_eq!(
			v2(json!({ "Array": [option, null] })),
			json!({ "kind": "array", "item": { "kind": "option", "item": { "kind": "string" } } })
		);
	}

	#[test]
	fn unions_are_canonical() {
		assert_eq!(
			v2(json!({ "Either": ["String", "Int", "String", { "Either": ["Null"] }] })),
			v2(json!({ "Either": ["Null", "Int", "String"] }))
		);
	}

	#[test]
	fn sized_numbers_keep_their_width_and_signatures_drop() {
		assert_eq!(v2(json!("Int")), json!({ "kind": "int" }));
		assert_eq!(v2(json!("I8")), json!({ "kind": "int", "width": 8 }));
		assert_eq!(v2(json!("U64")), json!({ "kind": "int", "width": 64, "signed": false }));
		assert_eq!(v2(json!("F32")), json!({ "kind": "float", "width": 32 }));
		assert_eq!(v2(json!({ "Function": [["Int"], "String"] })), json!({ "kind": "function" }));
		// A width is part of the kind's identity.
		assert_eq!(
			v2(json!({ "Either": ["I8", "Int"] })),
			json!({ "kind": "union", "variants": [
				{ "kind": "int", "width": 8 },
				{ "kind": "int" },
			] })
		);
	}

	#[test]
	fn the_4_x_kinds_have_nodes() {
		let converted =
			top(&value(json!({ "Either": ["Date", "BigDecimal", { "Named": "money" }] })));
		assert!(converted.unrepresented.is_empty());
		assert_eq!(
			serde_json::to_value(converted.kind).expect("v2"),
			json!({ "kind": "union", "variants": [
				{ "kind": "bigdecimal" },
				{ "kind": "date" },
				{ "kind": "named", "name": "money" },
			] })
		);
	}

	#[test]
	fn an_enum_keeps_its_labels_as_declared() {
		assert_eq!(
			v2(json!({ "Enum": ["b", "a", "b"] })),
			json!({ "kind": "enum", "labels": ["b", "a", "b"] })
		);
		let awkward = ["it's", "back\\slash", "a, b>", "`tick`", "\u{e9}t\u{e9}"];
		assert_eq!(v2(json!({ "Enum": awkward })), json!({ "kind": "enum", "labels": awkward }));
	}

	#[test]
	fn a_name_is_a_name_whatever_it_spells() {
		for name in ["date", "int", "my-type", "with `tick`", "back\\slash"] {
			assert_eq!(v2(json!({ "Named": name })), json!({ "kind": "named", "name": name }));
		}
	}

	#[test]
	fn variants_this_build_does_not_know_are_unknown_and_listed() {
		let converted = top(&value(json!({ "Either": ["String", { "Hologram": 3 }] })));
		assert_eq!(converted.unrepresented, [r#"{"Hologram":3}"#]);
		assert_eq!(
			serde_json::to_value(converted.kind).expect("v2"),
			json!({ "kind": "union", "variants": [
				{ "kind": "string" },
				{ "kind": "unknown", "source": r#"{"Hologram":3}"# },
			] })
		);
	}

	#[test]
	fn non_finite_floats_are_spelled_as_the_parser_spells_them() {
		for (f, text) in
			[(f64::NAN, "NaN"), (f64::INFINITY, "Infinity"), (f64::NEG_INFINITY, "-Infinity")]
		{
			let converted = top(&KindValue::Literal(LiteralValue::Float(f)));
			assert_eq!(Some((converted.kind, converted.optional)), kind_v2::parse_top_exact(text));
		}
	}

	#[test]
	fn literals_are_spelled_as_v2_spells_them() {
		assert_eq!(
			v2(json!({ "Literal": { "Duration": { "secs": 90, "nanos": 0 } } })),
			json!({ "kind": "literal", "type": "duration", "value": "1m30s" })
		);
		assert_eq!(
			v2(json!({ "Literal": { "Decimal": "1.25" } })),
			json!({ "kind": "literal", "type": "decimal", "value": "1.25" })
		);
		assert_eq!(
			v2(json!({ "Literal": { "Float": 1.5 } })),
			json!({ "kind": "literal", "type": "float", "value": "1.5" })
		);
	}

	#[test]
	fn geometry_types_are_lower_case_and_sorted() {
		assert_eq!(
			v2(json!({ "Geometry": ["Polygon", "MultiLine", "Point"] })),
			json!({ "kind": "geometry", "types": ["multiline", "point", "polygon"] })
		);
	}

	/// Every engine kind in the fixture converts, the v2 kind parser reads the engine's
	/// spelling of every one (`surrealdb_types::typing::render_kind`), and both readings give
	/// the same v2 kind.
	#[test]
	fn converting_agrees_with_parsing_the_engine_spelling() {
		let fixture: Vec<Value> =
			serde_json::from_str(include_str!("../../tests/fixtures/codegen/kind_values.json"))
				.expect("fixture");
		let mut unreadable = Vec::new();
		let mut compared = 0;
		for entry in &fixture {
			let text = entry["kind"].as_str().expect("kind text");
			let converted = top(&value(entry["kind_value"].clone()));
			// A function's engine spelling drops its signature, so the two cannot agree on more
			// than `function`, which both give.
			if converted.kind == Kind::Function {
				assert_eq!(text, "function");
				continue;
			}
			match kind_v2::parse_top_exact(text) {
				Some(parsed) => {
					assert_eq!((converted.kind, converted.optional), parsed, "{text}");
					compared += 1;
				}
				None => unreadable.push(text),
			}
		}
		assert!(unreadable.is_empty(), "the v2 parser cannot read {unreadable:?}");
		assert!(compared > 30, "compared only {compared} kinds");
	}
}
