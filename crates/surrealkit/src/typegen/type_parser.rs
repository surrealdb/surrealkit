//! Pure SurrealQL `TYPE`-clause parser. No database or IO.
//!
//! Two entry points:
//! - [`extract_type_clause`] pulls the `TYPE …` substring out of a full `DEFINE FIELD` statement,
//!   stopping at the first top-level clause keyword.
//! - [`parse_type`] turns that substring into a structured [`FieldType`].
//!
//! The parser is *total*: anything it does not understand becomes
//! [`FieldType::Unknown`] rather than panicking or being dropped.

use super::types::{FieldType, PrimitiveType};

/// Clause keywords that terminate the `TYPE` body in a `DEFINE FIELD` statement.
const TERMINATORS: &[&str] =
	&["DEFAULT", "VALUE", "ASSERT", "PERMISSIONS", "READONLY", "COMMENT", "REFERENCE"];

/// Extract the `TYPE` body from a `DEFINE FIELD` statement.
///
/// Returns the substring between the `TYPE` keyword and the next top-level
/// clause keyword (or end of statement). `None` if there is no `TYPE` clause.
/// Bracket depth and quotes are tracked so terminators and the `TYPE` keyword
/// are only recognised at the top level and outside string literals.
pub fn extract_type_clause(stmt: &str) -> Option<String> {
	let chars: Vec<char> = stmt.chars().collect();
	let n = chars.len();
	let mut i = 0;
	let mut depth: i32 = 0;
	let mut quote: Option<char> = None;
	let mut type_start: Option<usize> = None;

	while i < n {
		let c = chars[i];
		if let Some(q) = quote {
			if c == q {
				quote = None;
			}
			i += 1;
			continue;
		}
		match c {
			'\'' | '"' => {
				quote = Some(c);
				i += 1;
				continue;
			}
			'<' | '(' | '[' | '{' => {
				depth += 1;
				i += 1;
				continue;
			}
			'>' | ')' | ']' | '}' => {
				depth -= 1;
				i += 1;
				continue;
			}
			_ => {}
		}

		if c.is_ascii_alphabetic() {
			let word_start = i;
			let mut j = i;
			while j < n && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
				j += 1;
			}
			let upper: String =
				chars[word_start..j].iter().collect::<String>().to_ascii_uppercase();
			match type_start {
				Some(start) if depth == 0 && TERMINATORS.contains(&upper.as_str()) => {
					let body: String = chars[start..word_start].iter().collect();
					return non_empty(body.trim());
				}
				None if depth == 0 && upper == "TYPE" => {
					type_start = Some(j);
				}
				_ => {}
			}
			i = j;
			continue;
		}
		i += 1;
	}

	let start = type_start?;
	let body: String = chars[start..].iter().collect();
	non_empty(body.trim().trim_end_matches(';').trim())
}

fn non_empty(s: &str) -> Option<String> {
	if s.is_empty() {
		None
	} else {
		Some(s.to_string())
	}
}

/// Parse a SurrealQL type expression (e.g. `option<array<record<user>, 10>>`)
/// into a [`FieldType`]. Total — unrecognised input yields
/// [`FieldType::Unknown`].
pub fn parse_type(src: &str) -> FieldType {
	let mut p = Parser::new(src);
	let ty = p.parse_union();
	p.skip_ws();
	if !p.at_end() {
		// Trailing tokens we did not consume — treat the whole thing as opaque.
		return FieldType::Unknown {
			source: src.trim().to_string(),
		};
	}
	ty
}

struct Parser {
	chars: Vec<char>,
	pos: usize,
}

impl Parser {
	fn new(src: &str) -> Self {
		Self {
			chars: src.chars().collect(),
			pos: 0,
		}
	}

	fn at_end(&self) -> bool {
		self.pos >= self.chars.len()
	}

	fn peek(&self) -> Option<char> {
		self.chars.get(self.pos).copied()
	}

	fn skip_ws(&mut self) {
		while let Some(c) = self.peek() {
			if c.is_whitespace() {
				self.pos += 1;
			} else {
				break;
			}
		}
	}

	/// `primary ('|' primary)*`
	fn parse_union(&mut self) -> FieldType {
		let mut variants = vec![self.parse_primary()];
		loop {
			self.skip_ws();
			if self.peek() == Some('|') {
				self.pos += 1;
				variants.push(self.parse_primary());
			} else {
				break;
			}
		}
		if variants.len() == 1 {
			variants.into_iter().next().expect("checked len == 1")
		} else {
			FieldType::Union {
				variants,
			}
		}
	}

	fn parse_primary(&mut self) -> FieldType {
		self.skip_ws();
		match self.peek() {
			None => FieldType::Unknown {
				source: String::new(),
			},
			Some(c) if c == '\'' || c == '"' => self.parse_string_literal(c),
			Some(c) if c.is_ascii_digit() || c == '-' || c == '+' => self.parse_number_literal(),
			Some(c) if c.is_ascii_alphabetic() => self.parse_ident_type(),
			Some(_) => {
				let source = self.remaining_trimmed();
				self.pos = self.chars.len();
				FieldType::Unknown {
					source,
				}
			}
		}
	}

	fn remaining_trimmed(&self) -> String {
		self.chars[self.pos..].iter().collect::<String>().trim().to_string()
	}

	fn parse_string_literal(&mut self, quote: char) -> FieldType {
		self.pos += 1; // opening quote
		let start = self.pos;
		while let Some(c) = self.peek() {
			self.pos += 1;
			if c == quote {
				let value: String = self.chars[start..self.pos - 1].iter().collect();
				return FieldType::Literal {
					value: serde_json::Value::String(value),
				};
			}
		}
		// Unterminated quote.
		FieldType::Unknown {
			source: self.chars[start - 1..].iter().collect(),
		}
	}

	fn parse_number_literal(&mut self) -> FieldType {
		let start = self.pos;
		while let Some(c) = self.peek() {
			if c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E' {
				self.pos += 1;
			} else {
				break;
			}
		}
		let raw: String = self.chars[start..self.pos].iter().collect();
		match serde_json::from_str::<serde_json::Number>(&raw) {
			Ok(num) => FieldType::Literal {
				value: serde_json::Value::Number(num),
			},
			Err(_) => FieldType::Unknown {
				source: raw,
			},
		}
	}

	fn read_ident(&mut self) -> String {
		let start = self.pos;
		while let Some(c) = self.peek() {
			if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
				self.pos += 1;
			} else {
				break;
			}
		}
		self.chars[start..self.pos].iter().collect()
	}

	/// Read the inner content of a balanced `<…>` group. Assumes `peek() == '<'`.
	fn read_angle_group(&mut self) -> Option<String> {
		if self.peek() != Some('<') {
			return None;
		}
		self.pos += 1; // '<'
		let start = self.pos;
		let mut depth = 1;
		let mut quote: Option<char> = None;
		while let Some(c) = self.peek() {
			if let Some(q) = quote {
				if c == q {
					quote = None;
				}
				self.pos += 1;
				continue;
			}
			match c {
				'\'' | '"' => quote = Some(c),
				'<' | '(' | '[' => depth += 1,
				'>' | ')' | ']' => {
					depth -= 1;
					if depth == 0 {
						let inner: String = self.chars[start..self.pos].iter().collect();
						self.pos += 1; // '>'
						return Some(inner);
					}
				}
				_ => {}
			}
			self.pos += 1;
		}
		None
	}

	fn parse_ident_type(&mut self) -> FieldType {
		let ident = self.read_ident();
		let lower = ident.to_ascii_lowercase();
		self.skip_ws();
		let has_generic = self.peek() == Some('<');

		match lower.as_str() {
			"option" => {
				let inner = self.read_angle_group().unwrap_or_default();
				FieldType::Option {
					inner: Box::new(parse_type(&inner)),
				}
			}
			"array" | "set" => {
				let inner = self.read_angle_group();
				let (element, max) = parse_collection_args(inner.as_deref());
				if lower == "array" {
					FieldType::Array {
						inner: Box::new(element),
						max,
					}
				} else {
					FieldType::Set {
						inner: Box::new(element),
						max,
					}
				}
			}
			"record" => {
				let tables = self
					.read_angle_group()
					.map(|inner| split_top_level(&inner, '|'))
					.unwrap_or_default()
					.into_iter()
					.map(|t| t.trim().to_string())
					.filter(|t| !t.is_empty() && !t.eq_ignore_ascii_case("any"))
					.collect();
				FieldType::Record {
					tables,
				}
			}
			"geometry" => {
				let kinds = self
					.read_angle_group()
					.map(|inner| split_top_level(&inner, '|'))
					.unwrap_or_default()
					.into_iter()
					.map(|t| t.trim().to_string())
					.filter(|t| !t.is_empty())
					.collect();
				FieldType::Geometry {
					kinds,
				}
			}
			"true" => FieldType::Literal {
				value: serde_json::Value::Bool(true),
			},
			"false" => FieldType::Literal {
				value: serde_json::Value::Bool(false),
			},
			_ => {
				if let Some(prim) = primitive(&lower) {
					// A primitive should not carry generics; if it does, it is
					// something we do not model — fall back to Unknown.
					if has_generic {
						let inner = self.read_angle_group().unwrap_or_default();
						FieldType::Unknown {
							source: format!("{ident}<{inner}>"),
						}
					} else {
						FieldType::Primitive {
							name: prim,
						}
					}
				} else {
					if has_generic {
						let inner = self.read_angle_group().unwrap_or_default();
						return FieldType::Unknown {
							source: format!("{ident}<{inner}>"),
						};
					}
					FieldType::Unknown {
						source: ident,
					}
				}
			}
		}
	}
}

/// Interpret the generic arguments of `array<…>` / `set<…>`: an element type
/// and an optional numeric maximum.
fn parse_collection_args(inner: Option<&str>) -> (FieldType, Option<u64>) {
	let Some(inner) = inner else {
		return (
			FieldType::Primitive {
				name: PrimitiveType::Any,
			},
			None,
		);
	};
	let parts = split_top_level(inner, ',');
	let element = parts.first().map(|s| parse_type(s.trim())).unwrap_or(FieldType::Primitive {
		name: PrimitiveType::Any,
	});
	let max = parts.get(1).and_then(|s| s.trim().parse::<u64>().ok());
	(element, max)
}

/// Split `s` on `sep`, but only at the top level — ignoring separators inside
/// `<…>`, `(…)`, `[…]`, or string literals.
pub(super) fn split_top_level(s: &str, sep: char) -> Vec<String> {
	let mut parts = Vec::new();
	let mut current = String::new();
	let mut depth = 0;
	let mut quote: Option<char> = None;
	for c in s.chars() {
		if let Some(q) = quote {
			if c == q {
				quote = None;
			}
			current.push(c);
			continue;
		}
		match c {
			'\'' | '"' => {
				quote = Some(c);
				current.push(c);
			}
			'<' | '(' | '[' => {
				depth += 1;
				current.push(c);
			}
			'>' | ')' | ']' => {
				depth -= 1;
				current.push(c);
			}
			_ if c == sep && depth == 0 => {
				parts.push(current.trim().to_string());
				current.clear();
			}
			_ => current.push(c),
		}
	}
	if !current.trim().is_empty() || !parts.is_empty() {
		parts.push(current.trim().to_string());
	}
	parts
}

/// Strip optionality from a parsed type, returning the inner type and whether
/// it was optional.
///
/// SurrealDB represents an optional field as a `none | T` union (it normalises
/// `option<T>` away), so we detect a `none` member and remove it. A literal
/// `option<…>` is also handled, in case it ever survives unnormalised.
pub(super) fn unwrap_optional(ty: FieldType) -> (FieldType, bool) {
	match ty {
		FieldType::Option {
			inner,
		} => (*inner, true),
		FieldType::Union {
			variants,
		} => {
			let is_none = |t: &FieldType| {
				matches!(
					t,
					FieldType::Primitive {
						name: PrimitiveType::None
					}
				)
			};
			if variants.iter().any(is_none) {
				let rest: Vec<FieldType> = variants.into_iter().filter(|t| !is_none(t)).collect();
				let inner = match rest.len() {
					0 => FieldType::Primitive {
						name: PrimitiveType::None,
					},
					1 => rest.into_iter().next().expect("checked len == 1"),
					_ => FieldType::Union {
						variants: rest,
					},
				};
				(inner, true)
			} else {
				(
					FieldType::Union {
						variants,
					},
					false,
				)
			}
		}
		other => (other, false),
	}
}

fn primitive(name: &str) -> Option<PrimitiveType> {
	Some(match name {
		"string" => PrimitiveType::String,
		"int" => PrimitiveType::Int,
		"float" => PrimitiveType::Float,
		"bool" | "boolean" => PrimitiveType::Bool,
		"number" => PrimitiveType::Number,
		"decimal" => PrimitiveType::Decimal,
		"datetime" => PrimitiveType::Datetime,
		"duration" => PrimitiveType::Duration,
		"uuid" => PrimitiveType::Uuid,
		"bytes" => PrimitiveType::Bytes,
		"any" => PrimitiveType::Any,
		"null" => PrimitiveType::Null,
		"none" => PrimitiveType::None,
		"object" => PrimitiveType::Object,
		"function" => PrimitiveType::Function,
		_ => return None,
	})
}

#[cfg(test)]
mod tests {
	use test_case::test_case;

	use super::*;

	fn prim(p: PrimitiveType) -> FieldType {
		FieldType::Primitive {
			name: p,
		}
	}

	#[test_case("string", PrimitiveType::String)]
	#[test_case("int", PrimitiveType::Int)]
	#[test_case("float", PrimitiveType::Float)]
	#[test_case("bool", PrimitiveType::Bool)]
	#[test_case("number", PrimitiveType::Number)]
	#[test_case("decimal", PrimitiveType::Decimal)]
	#[test_case("datetime", PrimitiveType::Datetime)]
	#[test_case("duration", PrimitiveType::Duration)]
	#[test_case("uuid", PrimitiveType::Uuid)]
	#[test_case("bytes", PrimitiveType::Bytes)]
	#[test_case("any", PrimitiveType::Any)]
	#[test_case("null", PrimitiveType::Null)]
	#[test_case("object", PrimitiveType::Object)]
	#[test_case("function", PrimitiveType::Function)]
	fn primitives(src: &str, expected: PrimitiveType) {
		assert_eq!(parse_type(src), prim(expected));
	}

	#[test]
	fn option_wraps_inner() {
		assert_eq!(
			parse_type("option<string>"),
			FieldType::Option {
				inner: Box::new(prim(PrimitiveType::String))
			}
		);
	}

	#[test]
	fn array_without_max() {
		assert_eq!(
			parse_type("array<string>"),
			FieldType::Array {
				inner: Box::new(prim(PrimitiveType::String)),
				max: None
			}
		);
	}

	#[test]
	fn array_with_max() {
		assert_eq!(
			parse_type("array<int, 10>"),
			FieldType::Array {
				inner: Box::new(prim(PrimitiveType::Int)),
				max: Some(10)
			}
		);
	}

	#[test]
	fn set_of_records() {
		assert_eq!(
			parse_type("set<record<user>>"),
			FieldType::Set {
				inner: Box::new(FieldType::Record {
					tables: vec!["user".to_string()]
				}),
				max: None
			}
		);
	}

	#[test]
	fn record_single_and_union_and_bare() {
		assert_eq!(
			parse_type("record<user>"),
			FieldType::Record {
				tables: vec!["user".to_string()]
			}
		);
		assert_eq!(
			parse_type("record<user | admin>"),
			FieldType::Record {
				tables: vec!["user".to_string(), "admin".to_string()]
			}
		);
		assert_eq!(
			parse_type("record"),
			FieldType::Record {
				tables: vec![]
			}
		);
		assert_eq!(
			parse_type("record<any>"),
			FieldType::Record {
				tables: vec![]
			}
		);
	}

	#[test]
	fn geometry_kinds() {
		assert_eq!(
			parse_type("geometry<point | polygon>"),
			FieldType::Geometry {
				kinds: vec!["point".to_string(), "polygon".to_string()]
			}
		);
	}

	#[test]
	fn union_of_primitives_with_null() {
		assert_eq!(
			parse_type("string | int | null"),
			FieldType::Union {
				variants: vec![
					prim(PrimitiveType::String),
					prim(PrimitiveType::Int),
					prim(PrimitiveType::Null),
				]
			}
		);
	}

	#[test]
	fn string_literal_union() {
		assert_eq!(
			parse_type("'a' | 'b'"),
			FieldType::Union {
				variants: vec![
					FieldType::Literal {
						value: serde_json::Value::String("a".to_string())
					},
					FieldType::Literal {
						value: serde_json::Value::String("b".to_string())
					},
				]
			}
		);
	}

	#[test]
	fn numeric_literal_union() {
		let parsed = parse_type("200 | 404");
		assert_eq!(
			parsed,
			FieldType::Union {
				variants: vec![
					FieldType::Literal {
						value: serde_json::json!(200)
					},
					FieldType::Literal {
						value: serde_json::json!(404)
					},
				]
			}
		);
	}

	#[test]
	fn deeply_nested() {
		assert_eq!(
			parse_type("option<array<record<user>>>"),
			FieldType::Option {
				inner: Box::new(FieldType::Array {
					inner: Box::new(FieldType::Record {
						tables: vec!["user".to_string()]
					}),
					max: None
				})
			}
		);
	}

	#[test]
	fn array_of_options() {
		assert_eq!(
			parse_type("array<option<string>>"),
			FieldType::Array {
				inner: Box::new(FieldType::Option {
					inner: Box::new(prim(PrimitiveType::String))
				}),
				max: None
			}
		);
	}

	#[test]
	fn garbage_is_unknown_not_panic() {
		assert_eq!(
			parse_type("@@@ not a type @@@"),
			FieldType::Unknown {
				source: "@@@ not a type @@@".to_string()
			}
		);
		// Should not panic on empty input either.
		assert!(matches!(parse_type(""), FieldType::Unknown { .. }));
	}

	#[test]
	fn unwrap_optional_strips_none_union() {
		// SurrealDB emits `option<string>` as `none | string`.
		let (inner, opt) = unwrap_optional(parse_type("none | string"));
		assert!(opt);
		assert_eq!(inner, prim(PrimitiveType::String));

		// `none | string | int` -> optional union of the remainder.
		let (inner, opt) = unwrap_optional(parse_type("none | string | int"));
		assert!(opt);
		assert_eq!(
			inner,
			FieldType::Union {
				variants: vec![prim(PrimitiveType::String), prim(PrimitiveType::Int)]
			}
		);

		// No `none` -> not optional, unchanged.
		let (inner, opt) = unwrap_optional(parse_type("string"));
		assert!(!opt);
		assert_eq!(inner, prim(PrimitiveType::String));

		// Literal `option<…>` still handled.
		let (inner, opt) = unwrap_optional(parse_type("option<bool>"));
		assert!(opt);
		assert_eq!(inner, prim(PrimitiveType::Bool));
	}

	#[test]
	fn extract_terminated_by_default() {
		assert_eq!(
			extract_type_clause("DEFINE FIELD name ON user TYPE string DEFAULT 'x'"),
			Some("string".to_string())
		);
	}

	#[test]
	fn extract_terminated_by_assert() {
		assert_eq!(
			extract_type_clause("DEFINE FIELD age ON user TYPE int ASSERT $value != NONE"),
			Some("int".to_string())
		);
	}

	#[test]
	fn extract_full_type_before_permissions() {
		assert_eq!(
			extract_type_clause(
				"DEFINE FIELD tags ON user TYPE option<array<string>> PERMISSIONS FULL"
			),
			Some("option<array<string>>".to_string())
		);
	}

	#[test]
	fn extract_flexible_object() {
		assert_eq!(
			extract_type_clause("DEFINE FIELD meta ON user FLEXIBLE TYPE object"),
			Some("object".to_string())
		);
	}

	#[test]
	fn extract_none_when_no_type() {
		assert_eq!(extract_type_clause("DEFINE FIELD x ON user VALUE 1"), None);
	}

	#[test]
	fn extract_does_not_split_inside_generics() {
		// `READONLY`-like words can't appear inside a type, but the pipe and
		// commas inside generics must not confuse extraction.
		assert_eq!(
			extract_type_clause("DEFINE FIELD r ON user TYPE record<user | admin> READONLY"),
			Some("record<user | admin>".to_string())
		);
	}
}
