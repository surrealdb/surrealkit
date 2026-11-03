//! The engine's kinds (`kind_value`, the `surrealdb_types::Kind` serde form) as schema
//! document v2 kinds.
//!
//! A kind is lowered into the [`FieldType`] tree the v2 kind parser produces from kind
//! text, and then normalised by the same code (`typegen::kind_v2`), so a kind reaches the
//! document in one spelling whichever way it was read.
//!
//! v2 has no node for some engine kinds. Numeric widths widen: `f16` and `f32` are
//! `float`, and every fixed-width integer is `int`. A function's signature is dropped,
//! because v2's `function` is bare. `date`, `time`, `timestamp`, `interval`,
//! `bigdecimal`, named types (`DEFINE TYPE`) and variants this build does not know become
//! `{ kind: "unknown", source }`, with `source` the kind's SurrealQL spelling, and are
//! listed in [`V2Kind::unrepresented`] so the caller can report them.

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
	/// The SurrealQL spelling of every part that became `unknown`, in the order met.
	pub unrepresented: Vec<String>,
}

/// `kind` in a top-level position (a field, a literal-object field, a function argument,
/// a statement's result): a `none` variant becomes `optional`.
pub fn top(kind: &KindValue) -> V2Kind {
	let mut unrepresented = Vec::new();
	let ty = field_type(kind, &mut unrepresented);
	let (kind, optional) = kind_v2::top(&ty);
	V2Kind {
		kind,
		optional,
		unrepresented,
	}
}

/// `kind` in a nested position (a tuple item, an array or set item): NONE becomes
/// `{ kind: "option", item }`, and `optional` is always `false`.
pub fn nested(kind: &KindValue) -> V2Kind {
	let mut unrepresented = Vec::new();
	let ty = field_type(kind, &mut unrepresented);
	V2Kind {
		kind: kind_v2::nested(&ty),
		optional: false,
		unrepresented,
	}
}

fn primitive(name: PrimitiveType) -> FieldType {
	FieldType::Primitive {
		name,
	}
}

fn unknown(source: impl Into<String>, unrepresented: &mut Vec<String>) -> FieldType {
	let source = source.into();
	unrepresented.push(source.clone());
	FieldType::Unknown {
		source,
	}
}

fn literal(value: serde_json::Value, literal_type: Option<LiteralType>) -> FieldType {
	FieldType::Literal {
		value,
		literal_type,
	}
}

fn field_type(kind: &KindValue, unrepresented: &mut Vec<String>) -> FieldType {
	let many = |kinds: &[KindValue], unrepresented: &mut Vec<String>| -> Vec<FieldType> {
		kinds.iter().map(|k| field_type(k, unrepresented)).collect()
	};
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
		KindValue::F16 | KindValue::F32 | KindValue::Float => primitive(PrimitiveType::Float),
		KindValue::I8
		| KindValue::I16
		| KindValue::I32
		| KindValue::Int
		| KindValue::U8
		| KindValue::U16
		| KindValue::U32
		| KindValue::U64 => primitive(PrimitiveType::Int),
		KindValue::Date => unknown("date", unrepresented),
		KindValue::Time => unknown("time", unrepresented),
		KindValue::Timestamp => unknown("timestamp", unrepresented),
		KindValue::Interval => unknown("interval", unrepresented),
		KindValue::BigDecimal => unknown("bigdecimal", unrepresented),
		KindValue::Named(name) => unknown(name.clone(), unrepresented),
		KindValue::Unknown(raw) => unknown(raw.to_string(), unrepresented),
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
			variants: many(kinds, unrepresented),
		},
		KindValue::Array(item, max) => FieldType::Array {
			inner: Box::new(field_type(item, unrepresented)),
			max: *max,
		},
		KindValue::Set(item, max) => FieldType::Set {
			inner: Box::new(field_type(item, unrepresented)),
			max: *max,
		},
		KindValue::Enum(labels) => FieldType::Union {
			variants: labels
				.iter()
				.map(|label| literal(serde_json::Value::String(label.clone()), None))
				.collect(),
		},
		KindValue::Literal(value) => match value {
			LiteralValue::String(s) => literal(serde_json::Value::String(s.clone()), None),
			LiteralValue::Integer(i) => literal(serde_json::Value::from(*i), None),
			LiteralValue::Float(f) => match serde_json::Number::from_f64(*f) {
				Some(n) => literal(serde_json::Value::Number(n), Some(LiteralType::Float)),
				// NaN and the infinities have no JSON number; the engine spells them.
				None => unknown(format!("{f}f"), unrepresented),
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
				items: many(items, unrepresented),
			},
			LiteralValue::Object(fields) => FieldType::Object {
				fields: fields
					.iter()
					.map(|(name, kind)| ObjectField {
						name: name.clone(),
						r#type: field_type(kind, unrepresented),
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
	fn widths_widen_and_signatures_drop() {
		assert_eq!(v2(json!("U64")), json!({ "kind": "int" }));
		assert_eq!(v2(json!("F32")), json!({ "kind": "float" }));
		assert_eq!(v2(json!({ "Function": [["Int"], "String"] })), json!({ "kind": "function" }));
	}

	#[test]
	fn kinds_without_a_v2_node_are_unknown_and_listed() {
		let converted = top(&value(json!({ "Either": ["Date", { "Named": "money" }] })));
		assert_eq!(converted.unrepresented, ["date", "money"]);
		assert_eq!(
			serde_json::to_value(converted.kind).expect("v2"),
			json!({ "kind": "union", "variants": [
				{ "kind": "unknown", "source": "date" },
				{ "kind": "unknown", "source": "money" },
			] })
		);
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
		assert_eq!(
			v2(json!({ "Enum": ["b", "a"] })),
			json!({ "kind": "union", "variants": [
				{ "kind": "literal", "type": "string", "value": "a" },
				{ "kind": "literal", "type": "string", "value": "b" },
			] })
		);
	}

	#[test]
	fn geometry_types_are_lower_case_and_sorted() {
		assert_eq!(
			v2(json!({ "Geometry": ["Polygon", "MultiLine", "Point"] })),
			json!({ "kind": "geometry", "types": ["multiline", "point", "polygon"] })
		);
	}

	/// Every engine kind in the fixture converts, and wherever the v2 kind parser can read
	/// the engine's spelling of the same kind, both readings give the same v2 kind. The
	/// spellings it cannot read are pinned: the 4.x kinds and numeric widths, and two engine
	/// renderings the fixture captured (`1.25decdec`, an object literal with an unescaped key).
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
		assert_eq!(
			unreadable,
			[
				"date",
				"time",
				"timestamp",
				"interval",
				"bigdecimal",
				"f16",
				"f32",
				"i8",
				"i16",
				"i32",
				"u8",
				"u16",
				"u32",
				"u64",
				"1.25decdec",
				"{name: string, nick: none | string, weird key: int}",
				"enum<'a', 'b'>",
				"money",
			]
		);
		assert!(compared > 30, "compared only {compared} kinds");
	}
}
