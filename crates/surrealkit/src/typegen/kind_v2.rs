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
//! 6. SurrealDB 4.0's sized numbers carry their width: `i8`/`i16`/`i32` are `int` with `width`,
//!    `u8`…`u64` are `int` with `width` and `signed: false`, `f16`/`f32` are `float` with `width`.
//!    The engine renders `i64` and `f64` as `int` and `float`, so those carry no width. A width
//!    is part of a kind's identity: `i8 | int` is a union of two variants.
//! 7. The other 4.0 kinds have nodes of their own: `date`, `time`, `timestamp`, `interval`,
//!    `bigdecimal`; `enum<'a', 'b'>` is `enum` with its labels in declaration order (exempt from
//!    rules 3 and 4: the order is semantic, nothing is sorted or deduplicated); a `DEFINE TYPE`
//!    name is `named`.

use super::canonical::canonical_of;
use super::document::{Kind, LiteralType, LiteralValue, ObjectLiteralField};
use super::type_parser::{KindProblem, SizedNumber, V2Extension, parse_type_v2, v2_extension};
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
	let (parsed, problems) = parse_type_v2(text);
	let (kind, optional) = top(&parsed);
	ParsedKind {
		kind,
		optional,
		problems: unlowered(text, problems),
	}
}

/// Parse `text` as a kind in a value position, such as a `DEFINE TYPE`'s kind: a `none` variant
/// is an `option`, not optionality.
pub(super) fn parse_value(text: &str) -> ParsedKind {
	let (parsed, problems) = parse_type_v2(text);
	ParsedKind {
		kind: nested(&parsed),
		optional: false,
		problems: unlowered(text, problems),
	}
}

/// The problems of `text` that remain once the kinds this module lowers to nodes are accounted
/// for: a problem the parser `understood` is one such kind, and is dropped. Should one not
/// lower after all, it stays, as an error.
fn unlowered(text: &str, problems: Vec<KindProblem>) -> Vec<KindProblem> {
	problems
		.into_iter()
		.filter_map(|problem| {
			if !problem.understood {
				return Some(problem);
			}
			match text.get(problem.span.clone()).and_then(extension) {
				Some(_) => None,
				None => Some(KindProblem {
					understood: false,
					..problem
				}),
			}
		})
		.collect()
}

/// The node for kind text that [`parse_type_v2`] leaves as `unknown`: a sized number, `date`,
/// `time`, `timestamp`, `interval`, `bigdecimal`, an `enum<…>` or a `DEFINE TYPE` name.
fn extension(source: &str) -> Option<Kind> {
	Some(match v2_extension(source)? {
		V2Extension::Sized(number) => sized(number),
		V2Extension::Plain(name) => match name.as_str() {
			"date" => Kind::Date,
			"time" => Kind::Time,
			"timestamp" => Kind::Timestamp,
			"interval" => Kind::Interval,
			"bigdecimal" => Kind::BigDecimal,
			_ => return None,
		},
		V2Extension::Enum(labels) => Kind::Enum {
			labels,
		},
		V2Extension::Named(name) => Kind::Named {
			name,
		},
	})
}

/// A sized number: `int`/`float` with the width the engine would not render away. `i64` and
/// `f64` are what `int` and `float` already mean, so they carry none.
fn sized(number: SizedNumber) -> Kind {
	if number.float {
		Kind::Float {
			width: (number.width != 64).then_some(number.width),
		}
	} else {
		Kind::Int {
			width: (number.width != 64 || !number.signed).then_some(number.width),
			signed: (!number.signed).then_some(false),
		}
	}
}

/// Parse `text` as a kind in a top-level position, or `None` when any part of it could not be
/// parsed.
pub(crate) fn parse_top_exact(text: &str) -> Option<(Kind, bool)> {
	let parsed = parse_top(text);
	parsed.problems.is_empty().then_some((parsed.kind, parsed.optional))
}

/// Lower a kind in a top-level position.
pub(crate) fn top(ty: &FieldType) -> (Kind, bool) {
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
pub(crate) fn nested(ty: &FieldType) -> Kind {
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
		// The 4.0 kinds `parse_type_v2` leaves as unknown have nodes; anything else is
		// unknown for good.
		FieldType::Unknown {
			source,
		} => extension(source).unwrap_or_else(|| Kind::Unknown {
			source: source.clone(),
		}),
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
		PrimitiveType::Int => Kind::int(),
		PrimitiveType::Float => Kind::float(),
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

	/// A sized number keeps its width, wherever a kind may appear; the engine's own `int` and
	/// `float` (`i64`, `f64`) carry none.
	#[test]
	fn sized_numbers_carry_their_width_everywhere() {
		for (text, node) in [
			("i8", json!({"kind": "int", "width": 8})),
			("i16", json!({"kind": "int", "width": 16})),
			("i32", json!({"kind": "int", "width": 32})),
			("i64", json!({"kind": "int"})),
			("u8", json!({"kind": "int", "width": 8, "signed": false})),
			("u16", json!({"kind": "int", "width": 16, "signed": false})),
			("u32", json!({"kind": "int", "width": 32, "signed": false})),
			("u64", json!({"kind": "int", "width": 64, "signed": false})),
			("f16", json!({"kind": "float", "width": 16})),
			("f32", json!({"kind": "float", "width": 32})),
			("f64", json!({"kind": "float"})),
			("U8", json!({"kind": "int", "width": 8, "signed": false})),
		] {
			assert_eq!(kind(text), (node.clone(), false), "{text}");
			assert_eq!(kind(&format!("option<{text}>")), (node.clone(), true), "{text}");
			assert_eq!(
				kind(&format!("array<{text}, 2>")),
				(json!({"kind": "array", "item": node.clone(), "length": 2}), false),
				"{text}"
			);
			assert_eq!(
				kind(&format!("set<none | {text}>")),
				(json!({"kind": "set", "item": {"kind": "option", "item": node.clone()}}), false),
				"{text}"
			);
			assert_eq!(
				kind(&format!("[{text}, string]")),
				(json!({"kind": "tuple", "items": [node.clone(), {"kind": "string"}]}), false),
				"{text}"
			);
			assert_eq!(
				kind(&format!("{{ a: {text}, b: none | {text} }}")),
				(
					json!({"kind": "objectLiteral", "fields": [
						{"name": "a", "kind": node.clone(), "optional": false},
						{"name": "b", "kind": node.clone(), "optional": true},
					]}),
					false
				),
				"{text}"
			);
		}
	}

	/// A width is part of a kind's identity: unions deduplicate on it and do not collapse
	/// `i8 | int` into `int`.
	#[test]
	fn widths_are_part_of_a_unions_identity() {
		assert_eq!(
			kind("i8 | i8 | i64 | int"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "int", "width": 8},
					{"kind": "int"},
				]}),
				false
			)
		);
		assert_eq!(
			kind("f32 | f64 | u16 | string"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "float", "width": 32},
					{"kind": "float"},
					{"kind": "int", "signed": false, "width": 16},
					{"kind": "string"},
				]}),
				false
			)
		);
	}

	#[test]
	fn the_other_four_oh_kinds_have_nodes() {
		for (text, node) in [
			("date", json!({"kind": "date"})),
			("time", json!({"kind": "time"})),
			("timestamp", json!({"kind": "timestamp"})),
			("interval", json!({"kind": "interval"})),
			("bigdecimal", json!({"kind": "bigdecimal"})),
			("mood", json!({"kind": "named", "name": "mood"})),
			("Mood2", json!({"kind": "named", "name": "Mood2"})),
			("`my type`", json!({"kind": "named", "name": "my type"})),
			("⟨odd name⟩", json!({"kind": "named", "name": "odd name"})),
		] {
			assert_eq!(kind(text), (node.clone(), false), "{text}");
			assert_eq!(kind(&format!("none | {text}")), (node.clone(), true), "{text}");
			assert_eq!(
				kind(&format!("array<none | {text}>")),
				(json!({"kind": "array", "item": {"kind": "option", "item": node.clone()}}), false),
				"{text}"
			);
		}
	}

	/// An enum keeps its labels exactly: unescaped, in declaration order, duplicates and all.
	#[test]
	fn an_enum_keeps_its_labels_in_declaration_order() {
		assert_eq!(
			kind("enum<'z', 'a', 'm'>"),
			(json!({"kind": "enum", "labels": ["z", "a", "m"]}), false)
		);
		assert_eq!(
			kind("enum<'a', 'a', 'b'>"),
			(json!({"kind": "enum", "labels": ["a", "a", "b"]}), false)
		);
		// The spellings the engine writes (probed on 4.0): `'` inside a label switches to
		// double quotes, a backslash is doubled.
		assert_eq!(
			kind(r#"enum<'a', 'b c', "it's", 'q"x', 'back\\slash', 'é ☃', ''>"#),
			(
				json!({"kind": "enum", "labels": ["a", "b c", "it's", "q\"x", "back\\slash", "é ☃", ""]}),
				false
			)
		);
		assert_eq!(
			kind("none | enum<'b', 'a'>"),
			(json!({"kind": "enum", "labels": ["b", "a"]}), true)
		);
		// In a union the enum is one variant; the union sorts its variants, not its labels, and
		// two enums are the same variant only when their labels are in the same order.
		assert_eq!(
			kind("enum<'b', 'a'> | int"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "enum", "labels": ["b", "a"]},
					{"kind": "int"},
				]}),
				false
			)
		);
		assert_eq!(
			kind("enum<'b', 'a'> | enum<'a', 'b'> | enum<'b', 'a'>"),
			(
				json!({"kind": "union", "variants": [
					{"kind": "enum", "labels": ["a", "b"]},
					{"kind": "enum", "labels": ["b", "a"]},
				]}),
				false
			)
		);
	}

	/// Names that were never kinds stay errors and do not become `named`.
	#[test]
	fn names_that_are_not_kinds_are_still_unknown() {
		for text in ["either", "references", "literal", "mood<int>", "date<int>", "i32<int>"] {
			let parsed = parse_top(text);
			assert_eq!(parsed.problems.len(), 1, "{text}");
			assert_eq!(parsed.problems[0].diagnostic(text).code, "kind-unrecognized", "{text}");
			assert!(matches!(parsed.kind, Kind::Unknown { .. }), "{text}: {:?}", parsed.kind);
		}
		for text in ["enum", "enum<>", "enum<1>", "enum<'a'"] {
			let parsed = parse_top(text);
			assert_eq!(parsed.problems.len(), 1, "{text}");
			assert!(matches!(parsed.kind, Kind::Unknown { .. }), "{text}");
		}
	}

	/// A `DEFINE TYPE`'s kind is a value kind: NONE is an `option`, not optionality.
	#[test]
	fn a_type_kind_keeps_none_as_an_option() {
		let parsed = parse_value("none | string");
		assert!(!parsed.optional);
		assert_eq!(
			serde_json::to_value(&parsed.kind).unwrap(),
			json!({"kind": "option", "item": {"kind": "string"}})
		);
		let parsed = parse_value("'sad' | 'ok'");
		assert!(parsed.problems.is_empty());
		assert_eq!(
			serde_json::to_value(&parsed.kind).unwrap(),
			json!({"kind": "union", "variants": [
				{"kind": "literal", "type": "string", "value": "ok"},
				{"kind": "literal", "type": "string", "value": "sad"},
			]})
		);
	}

	#[test]
	fn unparseable_text_is_unknown_with_a_problem() {
		let parsed = parse_top("array<frob<int>>");
		assert_eq!(parsed.problems.len(), 1);
		assert_eq!(
			serde_json::to_value(&parsed.kind).unwrap(),
			json!({"kind": "array", "item": {"kind": "unknown", "source": "frob<int>"}})
		);
		assert_eq!(parsed.problems[0].diagnostic("array<frob<int>>").code, "kind-unrecognized");
	}
}
