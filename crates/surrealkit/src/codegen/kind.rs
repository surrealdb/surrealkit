//! `surrealdb_types::Kind` as the engine serialises it, without the engine.
//!
//! [`KindValue`] mirrors the externally-tagged serde form variant for variant, so a document
//! reads without linking the engine. A variant this build does not know (a newer engine) becomes
//! [`KindValue::Unknown`] holding the raw JSON, rather than failing the whole document, and
//! serialises back to exactly what it was read from.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};

/// A SurrealQL type, in the shape `surrealdb_types::Kind` serialises to.
#[derive(Debug, Clone, PartialEq)]
pub enum KindValue {
	Any,
	None,
	Null,
	Bool,
	Bytes,
	Datetime,
	Date,
	Time,
	Timestamp,
	Interval,
	Decimal,
	BigDecimal,
	Duration,
	F16,
	F32,
	Float,
	I8,
	I16,
	I32,
	Int,
	U8,
	U16,
	U32,
	U64,
	Number,
	Object,
	String,
	Uuid,
	Regex,
	Range,
	Table(Vec<String>),
	Record(Vec<String>),
	Geometry(Vec<String>),
	Either(Vec<KindValue>),
	Set(Box<KindValue>, Option<u64>),
	Array(Box<KindValue>, Option<u64>),
	Function(Option<Vec<KindValue>>, Option<Box<KindValue>>),
	Literal(LiteralValue),
	File(Vec<String>),
	Enum(Vec<String>),
	Named(String),
	/// A variant this build does not know, kept as read.
	Unknown(Value),
}

/// A literal type: a value used as a type (`'active'`, `42`, `{ a: int }`).
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
	String(String),
	Integer(i64),
	Float(f64),
	/// The decimal's text, as the engine writes it.
	Decimal(String),
	Duration {
		secs: u64,
		nanos: u32,
	},
	Bool(bool),
	Array(Vec<KindValue>),
	Object(BTreeMap<String, KindValue>),
}

/// The unit variants and the names they serialise to.
const UNIT_VARIANTS: [(&str, KindValue); 30] = [
	("Any", KindValue::Any),
	("None", KindValue::None),
	("Null", KindValue::Null),
	("Bool", KindValue::Bool),
	("Bytes", KindValue::Bytes),
	("Datetime", KindValue::Datetime),
	("Date", KindValue::Date),
	("Time", KindValue::Time),
	("Timestamp", KindValue::Timestamp),
	("Interval", KindValue::Interval),
	("Decimal", KindValue::Decimal),
	("BigDecimal", KindValue::BigDecimal),
	("Duration", KindValue::Duration),
	("F16", KindValue::F16),
	("F32", KindValue::F32),
	("Float", KindValue::Float),
	("I8", KindValue::I8),
	("I16", KindValue::I16),
	("I32", KindValue::I32),
	("Int", KindValue::Int),
	("U8", KindValue::U8),
	("U16", KindValue::U16),
	("U32", KindValue::U32),
	("U64", KindValue::U64),
	("Number", KindValue::Number),
	("Object", KindValue::Object),
	("String", KindValue::String),
	("Uuid", KindValue::Uuid),
	("Regex", KindValue::Regex),
	("Range", KindValue::Range),
];

impl KindValue {
	/// Reads a kind from its serde JSON form. Anything unrecognised is kept as
	/// [`KindValue::Unknown`].
	pub fn from_json(raw: &Value) -> KindValue {
		Self::parse(raw).unwrap_or_else(|| KindValue::Unknown(raw.clone()))
	}

	/// The serde JSON form this kind was read from.
	pub fn to_json(&self) -> Value {
		match self {
			KindValue::Table(tables) => tagged("Table", strings(tables)),
			KindValue::Record(tables) => tagged("Record", strings(tables)),
			KindValue::Geometry(names) => tagged("Geometry", strings(names)),
			KindValue::File(buckets) => tagged("File", strings(buckets)),
			KindValue::Enum(labels) => tagged("Enum", strings(labels)),
			KindValue::Either(members) => {
				tagged("Either", Value::Array(members.iter().map(KindValue::to_json).collect()))
			}
			KindValue::Set(inner, len) => tagged("Set", collection(inner, *len)),
			KindValue::Array(inner, len) => tagged("Array", collection(inner, *len)),
			KindValue::Function(args, returns) => tagged(
				"Function",
				Value::Array(vec![
					args.as_ref().map_or(Value::Null, |args| {
						Value::Array(args.iter().map(KindValue::to_json).collect())
					}),
					returns.as_ref().map_or(Value::Null, |returns| returns.to_json()),
				]),
			),
			KindValue::Literal(literal) => tagged("Literal", literal.to_json()),
			KindValue::Named(name) => tagged("Named", Value::String(name.clone())),
			KindValue::Unknown(raw) => raw.clone(),
			unit => Value::String(
				UNIT_VARIANTS
					.iter()
					.find(|(_, candidate)| candidate == unit)
					.map_or_else(String::new, |(name, _)| (*name).to_string()),
			),
		}
	}

	fn parse(raw: &Value) -> Option<KindValue> {
		match raw {
			Value::String(name) => {
				UNIT_VARIANTS.iter().find(|(known, _)| known == name).map(|(_, kind)| kind.clone())
			}
			Value::Object(map) => {
				let mut entries = map.iter();
				let (tag, body) = entries.next()?;
				if entries.next().is_some() {
					return None;
				}
				Self::parse_tagged(tag, body)
			}
			_ => None,
		}
	}

	fn parse_tagged(tag: &str, body: &Value) -> Option<KindValue> {
		Some(match tag {
			"Table" => KindValue::Table(string_list(body)?),
			"Record" => KindValue::Record(string_list(body)?),
			"Geometry" => KindValue::Geometry(string_list(body)?),
			"File" => KindValue::File(string_list(body)?),
			"Enum" => KindValue::Enum(string_list(body)?),
			"Either" => KindValue::Either(kind_list(body)?),
			"Set" => {
				let (inner, len) = parse_collection(body)?;
				KindValue::Set(inner, len)
			}
			"Array" => {
				let (inner, len) = parse_collection(body)?;
				KindValue::Array(inner, len)
			}
			"Function" => {
				let [args, returns] = body.as_array()?.as_slice() else {
					return None;
				};
				let args = match args {
					Value::Null => None,
					list => Some(kind_list(list)?),
				};
				let returns = match returns {
					Value::Null => None,
					kind => Some(Box::new(Self::parse(kind)?)),
				};
				KindValue::Function(args, returns)
			}
			"Literal" => KindValue::Literal(LiteralValue::parse(body)?),
			"Named" => KindValue::Named(body.as_str()?.to_string()),
			_ => return None,
		})
	}
}

impl LiteralValue {
	fn to_json(&self) -> Value {
		match self {
			LiteralValue::String(value) => tagged("String", Value::String(value.clone())),
			LiteralValue::Integer(value) => tagged("Integer", Value::from(*value)),
			LiteralValue::Float(value) => {
				tagged("Float", Number::from_f64(*value).map_or(Value::Null, Value::Number))
			}
			LiteralValue::Decimal(value) => tagged("Decimal", Value::String(value.clone())),
			LiteralValue::Duration {
				secs,
				nanos,
			} => {
				let mut duration = Map::new();
				duration.insert("nanos".to_string(), Value::from(*nanos));
				duration.insert("secs".to_string(), Value::from(*secs));
				tagged("Duration", Value::Object(duration))
			}
			LiteralValue::Bool(value) => tagged("Bool", Value::Bool(*value)),
			LiteralValue::Array(items) => {
				tagged("Array", Value::Array(items.iter().map(KindValue::to_json).collect()))
			}
			LiteralValue::Object(fields) => tagged(
				"Object",
				Value::Object(
					fields.iter().map(|(name, kind)| (name.clone(), kind.to_json())).collect(),
				),
			),
		}
	}

	fn parse(raw: &Value) -> Option<LiteralValue> {
		let map = raw.as_object()?;
		let mut entries = map.iter();
		let (tag, body) = entries.next()?;
		if entries.next().is_some() {
			return None;
		}
		Some(match tag.as_str() {
			"String" => LiteralValue::String(body.as_str()?.to_string()),
			"Integer" => LiteralValue::Integer(body.as_i64()?),
			"Float" => LiteralValue::Float(body.as_f64()?),
			"Decimal" => LiteralValue::Decimal(body.as_str()?.to_string()),
			"Duration" => LiteralValue::Duration {
				secs: body.get("secs")?.as_u64()?,
				nanos: u32::try_from(body.get("nanos")?.as_u64()?).ok()?,
			},
			"Bool" => LiteralValue::Bool(body.as_bool()?),
			"Array" => LiteralValue::Array(kind_list(body)?),
			"Object" => LiteralValue::Object(
				body.as_object()?
					.iter()
					.map(|(name, kind)| Some((name.clone(), KindValue::parse(kind)?)))
					.collect::<Option<BTreeMap<_, _>>>()?,
			),
			_ => return None,
		})
	}
}

fn tagged(tag: &str, body: Value) -> Value {
	let mut map = Map::new();
	map.insert(tag.to_string(), body);
	Value::Object(map)
}

fn strings(items: &[String]) -> Value {
	Value::Array(items.iter().cloned().map(Value::String).collect())
}

fn collection(inner: &KindValue, len: Option<u64>) -> Value {
	Value::Array(vec![inner.to_json(), len.map_or(Value::Null, Value::from)])
}

fn string_list(raw: &Value) -> Option<Vec<String>> {
	raw.as_array()?.iter().map(|item| item.as_str().map(str::to_string)).collect()
}

fn kind_list(raw: &Value) -> Option<Vec<KindValue>> {
	raw.as_array()?.iter().map(|item| Some(KindValue::from_json(item))).collect()
}

fn parse_collection(raw: &Value) -> Option<(Box<KindValue>, Option<u64>)> {
	let [inner, len] = raw.as_array()?.as_slice() else {
		return None;
	};
	let len = match len {
		Value::Null => None,
		len => Some(len.as_u64()?),
	};
	Some((Box::new(KindValue::from_json(inner)), len))
}

impl Serialize for KindValue {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.to_json().serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for KindValue {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		Ok(KindValue::from_json(&Value::deserialize(deserializer)?))
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn fixture() -> Vec<Value> {
		let raw = include_str!("../../tests/fixtures/codegen/kind_values.json");
		serde_json::from_str(raw).expect("kind_values.json is valid JSON")
	}

	#[test]
	fn every_engine_kind_is_recognised() {
		for entry in fixture() {
			let raw = &entry["kind_value"];
			let kind = KindValue::from_json(raw);
			assert!(
				!contains_unknown(&kind),
				"{} deserialised to an unknown variant: {kind:?}",
				entry["kind"]
			);
		}
	}

	#[test]
	fn every_engine_kind_round_trips() {
		for entry in fixture() {
			let raw = &entry["kind_value"];
			let kind = KindValue::from_json(raw);
			assert_eq!(&kind.to_json(), raw, "round trip of {}", entry["kind"]);
			let text = serde_json::to_string(&kind).expect("serialises");
			let again: KindValue = serde_json::from_str(&text).expect("deserialises");
			assert_eq!(again, kind);
		}
	}

	#[test]
	fn an_unknown_variant_is_kept_as_read() {
		let raw = json!({ "Quux": 1 });
		let kind = KindValue::from_json(&raw);
		assert_eq!(kind, KindValue::Unknown(raw.clone()));
		assert_eq!(kind.to_json(), raw);
	}

	#[test]
	fn an_unknown_variant_nested_in_a_known_one_does_not_poison_it() {
		let raw = json!({ "Array": [{ "Quux": [1, 2] }, null] });
		let kind = KindValue::from_json(&raw);
		assert_eq!(
			kind,
			KindValue::Array(Box::new(KindValue::Unknown(json!({ "Quux": [1, 2] }))), None)
		);
		assert_eq!(kind.to_json(), raw);
	}

	#[test]
	fn an_unknown_unit_variant_is_kept_as_read() {
		let kind = KindValue::from_json(&json!("Quux"));
		assert_eq!(kind, KindValue::Unknown(json!("Quux")));
	}

	#[test]
	fn an_unknown_literal_is_kept_as_read() {
		let raw = json!({ "Literal": { "Quux": 1 } });
		assert_eq!(KindValue::from_json(&raw), KindValue::Unknown(raw));
	}

	fn contains_unknown(kind: &KindValue) -> bool {
		match kind {
			KindValue::Unknown(_) => true,
			KindValue::Either(members) => members.iter().any(contains_unknown),
			KindValue::Set(inner, _) | KindValue::Array(inner, _) => contains_unknown(inner),
			KindValue::Function(args, returns) => {
				args.iter().flatten().any(contains_unknown)
					|| returns.as_deref().is_some_and(contains_unknown)
			}
			KindValue::Literal(LiteralValue::Array(items)) => items.iter().any(contains_unknown),
			KindValue::Literal(LiteralValue::Object(fields)) => {
				fields.values().any(contains_unknown)
			}
			_ => false,
		}
	}
}
