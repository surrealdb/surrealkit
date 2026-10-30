//! Kind normalisation for schema document v2 (contract section 4).
//!
//! The engine's kind text is parsed by the shared kind parser into the v1 [`FieldType`] tree,
//! then lowered into a v2 [`Kind`] with the normalisation rules applied:
//!
//! 1. `array<T, N>` / `set<T, N>`: `N` is an exact `length`.
//! 2. NONE becomes optionality: at the top of a field, a literal-object field or a function
//!    argument a `none` variant is removed and reported as `optional`; anywhere else it becomes
//!    `{ kind: "option", item }`. A union never contains `none`, `option` or a union.
//! 3. Unions are flattened, deduplicated and sorted by canonical JSON; one survivor replaces
//!    the union.
//! 4. Name lists are sorted and deduplicated; literal-object fields are sorted by name.
//! 5. Literal values are strings (booleans excepted) in their SurrealQL spelling without the
//!    type suffix.

use super::canonical::canonical_of;
use super::document::{Kind, LiteralType, LiteralValue, ObjectLiteralField};
use super::type_parser::{KindProblem, parse_type_checked};
use super::types::{FieldType, LiteralType as V1LiteralType, PrimitiveType};

/// A kind parsed from engine text, at a top-level position.
pub(super) struct ParsedKind {
	pub kind: Kind,
	/// The kind admitted NONE (and NONE was removed from it).
	pub optional: bool,
	/// Everything that could not be parsed, with byte spans into the text.
	pub problems: Vec<KindProblem>,
}

/// Parse `text` as a kind in a top-level position (field, literal-object field, function
/// argument): a `none` variant becomes `optional`.
pub(super) fn parse_top(text: &str) -> ParsedKind {
	let (parsed, problems) = parse_type_checked(text);
	let (kind, optional) = top(&parsed);
	ParsedKind {
		kind,
		optional,
		problems,
	}
}

/// Lower a kind in a top-level position.
fn top(ty: &FieldType) -> (Kind, bool) {
	let (has_none, rest) = flatten(ty);
	match rest.len() {
		0 if has_none => (Kind::None, true),
		0 => (Kind::Any, false),
		1 => (rest.into_iter().next().unwrap_or(Kind::Any), has_none),
		_ => (
			Kind::Union {
				variants: rest,
			},
			has_none,
		),
	}
}

/// Lower a kind in a nested position (tuple item, array or set item): NONE becomes `option`.
fn nested(ty: &FieldType) -> Kind {
	let (has_none, rest) = flatten(ty);
	let inner = match rest.len() {
		0 if has_none => return Kind::None,
		0 => Kind::Any,
		1 => rest.into_iter().next().unwrap_or(Kind::Any),
		_ => Kind::Union {
			variants: rest,
		},
	};
	if has_none {
		Kind::Option {
			item: Box::new(inner),
		}
	} else {
		inner
	}
}

/// The variants of `ty` once unions and options are flattened: whether NONE is among them,
/// and the others, lowered, deduplicated and sorted by canonical JSON.
fn flatten(ty: &FieldType) -> (bool, Vec<Kind>) {
	let mut has_none = false;
	let mut out: Vec<Kind> = Vec::new();
	collect(ty, &mut has_none, &mut out);
	let mut keyed: Vec<(String, Kind)> =
		out.into_iter().map(|k| (canonical_of(&k).unwrap_or_default(), k)).collect();
	keyed.sort_by(|a, b| a.0.cmp(&b.0));
	keyed.dedup_by(|a, b| a.0 == b.0);
	(has_none, keyed.into_iter().map(|(_, k)| k).collect())
}

fn collect(ty: &FieldType, has_none: &mut bool, out: &mut Vec<Kind>) {
	match ty {
		FieldType::Union {
			variants,
		} => {
			for v in variants {
				collect(v, has_none, out);
			}
		}
		FieldType::Option {
			inner,
		} => {
			*has_none = true;
			collect(inner, has_none, out);
		}
		FieldType::Primitive {
			name: PrimitiveType::None,
		} => *has_none = true,
		other => out.push(lower(other)),
	}
}

fn sorted(names: &[String]) -> Vec<String> {
	let mut names = names.to_vec();
	names.sort();
	names.dedup();
	names
}

/// Lower a kind that is not a union, option or `none`.
fn lower(ty: &FieldType) -> Kind {
	match ty {
		FieldType::Primitive {
			name,
		} => primitive(*name),
		FieldType::Array {
			inner,
			max,
		} => Kind::Array {
			item: Box::new(nested(inner)),
			length: *max,
		},
		FieldType::Set {
			inner,
			max,
		} => Kind::Set {
			item: Box::new(nested(inner)),
			length: *max,
		},
		FieldType::Record {
			tables,
		} => Kind::Record {
			tables: sorted(tables),
		},
		FieldType::Table {
			tables,
		} => Kind::Table {
			tables: sorted(tables),
		},
		FieldType::File {
			buckets,
		} => Kind::File {
			buckets: sorted(buckets),
		},
		FieldType::Geometry {
			kinds,
		} => Kind::Geometry {
			types: sorted(kinds),
		},
		FieldType::Literal {
			value,
			literal_type,
		} => literal(value, *literal_type),
		FieldType::Tuple {
			items,
		} => Kind::Tuple {
			items: items.iter().map(nested).collect(),
		},
		FieldType::Object {
			fields,
		} => {
			let mut fields: Vec<ObjectLiteralField> = fields
				.iter()
				.map(|f| {
					let (kind, optional) = top(&f.r#type);
					ObjectLiteralField {
						name: f.name.clone(),
						kind,
						optional,
					}
				})
				.collect();
			fields.sort_by(|a, b| a.name.cmp(&b.name));
			Kind::ObjectLiteral {
				fields,
			}
		}
		FieldType::Unknown {
			source,
		} => Kind::Unknown {
			source: source.clone(),
		},
		// Handled by `collect`; lowering them on their own still has to mean something.
		FieldType::Union {
			..
		}
		| FieldType::Option {
			..
		} => nested(ty),
	}
}

fn primitive(name: PrimitiveType) -> Kind {
	match name {
		PrimitiveType::String => Kind::String,
		PrimitiveType::Int => Kind::Int,
		PrimitiveType::Float => Kind::Float,
		PrimitiveType::Bool => Kind::Bool,
		PrimitiveType::Number => Kind::Number,
		PrimitiveType::Decimal => Kind::Decimal,
		PrimitiveType::Datetime => Kind::Datetime,
		PrimitiveType::Duration => Kind::Duration,
		PrimitiveType::Uuid => Kind::Uuid,
		PrimitiveType::Bytes => Kind::Bytes,
		PrimitiveType::Any => Kind::Any,
		PrimitiveType::Null => Kind::Null,
		PrimitiveType::None => Kind::None,
		PrimitiveType::Object => Kind::Object,
		PrimitiveType::Function => Kind::Function,
		PrimitiveType::Regex => Kind::Regex,
		PrimitiveType::Range => Kind::Range,
	}
}

/// A v1 literal as a v2 literal kind. Numbers are spelled as the engine spells them, without
/// the type suffix; integers and decimals are exact, floats use the shortest round-trip form
/// (which is how the engine renders them).
fn literal(value: &serde_json::Value, literal_type: Option<V1LiteralType>) -> Kind {
	let text = |v: &serde_json::Value| match v {
		serde_json::Value::String(s) => s.clone(),
		serde_json::Value::Number(n) => match (n.as_i64(), n.as_u64(), n.as_f64()) {
			(Some(i), _, _) => i.to_string(),
			(None, Some(u), _) => u.to_string(),
			(None, None, Some(f)) => f.to_string(),
			_ => n.to_string(),
		},
		other => other.to_string(),
	};
	let (literal_type, value) = match (literal_type, value) {
		(Some(V1LiteralType::Float), v) => (LiteralType::Float, LiteralValue::Text(text(v))),
		(Some(V1LiteralType::Decimal), v) => (LiteralType::Decimal, LiteralValue::Text(text(v))),
		(Some(V1LiteralType::Duration), v) => (LiteralType::Duration, LiteralValue::Text(text(v))),
		(None, serde_json::Value::Bool(b)) => (LiteralType::Bool, LiteralValue::Bool(*b)),
		// The parser only produces untagged numbers for integers. The engine rejects an
		// integer literal beyond 64 bits, so every one that reaches here is exact.
		(None, v @ serde_json::Value::Number(_)) => (LiteralType::Int, LiteralValue::Text(text(v))),
		(None, v) => (LiteralType::String, LiteralValue::Text(text(v))),
	};
	Kind::Literal {
		literal_type,
		value,
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn kind(text: &str) -> (serde_json::Value, bool) {
		let parsed = parse_top(text);
		assert!(parsed.problems.is_empty(), "{text}: {:?}", parsed.problems);
		(serde_json::to_value(&parsed.kind).unwrap(), parsed.optional)
	}

	#[test]
	fn none_at_the_top_becomes_optional_and_nested_becomes_option() {
		assert_eq!(kind("none | string"), (json!({"kind": "string"}), true));
		assert_eq!(kind("none"), (json!({"kind": "none"}), true));
		assert_eq!(
			kind("none | string | null"),
			(json!({"kind": "union", "variants": [{"kind": "null"}, {"kind": "string"}]}), true)
		);
		assert_eq!(
			kind("[string, int, none | bool]"),
			(
				json!({"kind": "tuple", "items": [
					{"kind": "string"}, {"kind": "int"},
					{"kind": "option", "item": {"kind": "bool"}},
				]}),
				false
			)
		);
		assert_eq!(
			kind("array<none | int | string>"),
			(
				json!({"kind": "array", "item": {"kind": "option", "item": {"kind": "union",
					"variants": [{"kind": "int"}, {"kind": "string"}]}}}),
				false
			)
		);
	}

	#[test]
	fn unions_are_flat_unique_and_sorted() {
		assert_eq!(
			kind("'open' | 'closed' | 'open'"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "literal", "type": "string", "value": "closed"},
					{"kind": "literal", "type": "string", "value": "open"},
				]}),
				false
			)
		);
		assert_eq!(kind("int | int"), (json!({"kind": "int"}), false));
	}

	#[test]
	fn names_sort_and_lengths_are_exact() {
		assert_eq!(
			kind("record<team | org>"),
			(json!({"kind": "record", "tables": ["org", "team"]}), false)
		);
		assert_eq!(
			kind("geometry<polygon | point>"),
			(json!({"kind": "geometry", "types": ["point", "polygon"]}), false)
		);
		assert_eq!(
			kind("set<int, 5>"),
			(json!({"kind": "set", "item": {"kind": "int"}, "length": 5}), false)
		);
		assert_eq!(kind("array"), (json!({"kind": "array", "item": {"kind": "any"}}), false));
	}

	#[test]
	fn literals_are_spelled_without_suffixes() {
		assert_eq!(
			kind("-5 | 0 |  1.5f |  10dec"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "literal", "type": "decimal", "value": "10"},
					{"kind": "literal", "type": "float", "value": "1.5"},
					{"kind": "literal", "type": "int", "value": "-5"},
					{"kind": "literal", "type": "int", "value": "0"},
				]}),
				false
			)
		);
		assert_eq!(
			kind("true | 1d"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "literal", "type": "bool", "value": true},
					{"kind": "literal", "type": "duration", "value": "1d"},
				]}),
				false
			)
		);
	}

	#[test]
	fn object_literals_sort_and_carry_optional_flags() {
		assert_eq!(
			kind("{ b: none | string, a: int }"),
			(
				json!({"kind": "objectLiteral", "fields": [
					{"name": "a", "kind": {"kind": "int"}, "optional": false},
					{"name": "b", "kind": {"kind": "string"}, "optional": true},
				]}),
				false
			)
		);
	}

	#[test]
	fn unparseable_text_is_unknown_with_a_problem() {
		let parsed = parse_top("array<frob>");
		assert_eq!(parsed.problems.len(), 1);
		assert_eq!(
			serde_json::to_value(&parsed.kind).unwrap(),
			json!({"kind": "array", "item": {"kind": "unknown", "source": "frob"}})
		);
	}
}
