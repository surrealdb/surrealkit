//! Pure SurrealQL kind parser. No database or IO.
//!
//! Entry points:
//! - [`parse_type_checked`] turns a kind (the body of a `TYPE` clause, a function argument or
//!   return type) into a structured [`FieldType`] and reports every part it could not understand.
//! - `parse_type` (tests only) is the same without the report.
//! - [`parse_type_prefix`] parses one kind at the start of a longer text.
//! - `extract_type_clause` (tests only) pulls the `TYPE …` body out of a `DEFINE FIELD` statement.
//!
//! It accepts what SurrealDB 3.x renders (see `docs/typegen/schema-document-v2.md`, section
//! 4): every primitive, `option<T>` and its `none | T` normal form, `array` and `set` with an
//! optional exact length, `record`, `table`, `file` and `geometry` with their name lists
//! (backtick-quoted names included), tuples `[A, B]`, object literals `{ a: int, "b c": T }`,
//! and literal strings (with escapes), integers, floats (`1.5f`, `NaN`), decimals (`10dec`),
//! durations (`1h30m`) and booleans. Spacing is free, which matters because the engine writes
//! stray double spaces before float and decimal literals.
//!
//! The parser is *total*: anything it does not understand becomes [`FieldType::Unknown`],
//! never a panic, and every `Unknown` is matched by a [`KindProblem`] saying why. An
//! unrecognised name stays local (`array<frob>` is an array of unknown); a syntax error makes
//! the whole kind unknown.

use std::ops::Range;

use super::types::{FieldType, LiteralType, ObjectField, PrimitiveType};

/// Why part of a kind could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KindProblem {
	/// The byte range, within the parsed text, of the part at fault.
	pub span: Range<usize>,
	pub reason: String,
}

/// Extract the `TYPE` body from a `DEFINE FIELD` statement: the text between
/// the `TYPE` keyword and the next clause keyword. `None` without a `TYPE`
/// clause. See [`super::define::parse_field`] for how clauses are found.
#[cfg(test)]
pub fn extract_type_clause(stmt: &str) -> Option<String> {
	super::define::parse_field(stmt).kind.map(str::to_string)
}

/// Parse a SurrealQL kind (e.g. `option<array<record<user>, 10>>`) into a
/// [`FieldType`]. Total — unrecognised input yields [`FieldType::Unknown`].
#[cfg(test)]
pub fn parse_type(src: &str) -> FieldType {
	parse_type_checked(src).0
}

/// Parse a kind, with a [`KindProblem`] for every [`FieldType::Unknown`] in the
/// result. Spans are byte ranges within `src`.
pub(super) fn parse_type_checked(src: &str) -> (FieldType, Vec<KindProblem>) {
	let mut p = KindParser::new(src);
	p.skip_ws();
	match p.union() {
		Ok(kind) => {
			p.skip_ws();
			if p.at_end() {
				(kind, p.problems)
			} else {
				let span = p.pos..src.len();
				let reason = format!("unexpected `{}` after the kind", src[span.clone()].trim());
				(
					whole_unknown(src),
					vec![KindProblem {
						span,
						reason,
					}],
				)
			}
		}
		Err(problem) => (whole_unknown(src), vec![problem]),
	}
}

/// Parse one kind at the start of `src` and return it with the number of bytes
/// it took, so a caller can carry on after it (a function's return kind is
/// followed by its body). On a syntax error the kind is unknown and nothing is
/// consumed.
pub(super) fn parse_type_prefix(src: &str) -> (FieldType, usize, Vec<KindProblem>) {
	let mut p = KindParser::new(src);
	p.skip_ws();
	match p.union() {
		Ok(kind) => (kind, p.pos, p.problems),
		Err(problem) => (whole_unknown(src), 0, vec![problem]),
	}
}

fn whole_unknown(src: &str) -> FieldType {
	FieldType::Unknown {
		source: src.trim().to_string(),
	}
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

pub(super) fn is_none(t: &FieldType) -> bool {
	matches!(
		t,
		FieldType::Primitive {
			name: PrimitiveType::None
		}
	)
}

/// Split `s` on `sep`, but only at the top level — ignoring separators inside
/// `<…>`, `(…)`, `[…]`, `{…}`, string literals (escapes included) and quoted
/// identifiers.
pub(super) fn split_top_level(s: &str, sep: char) -> Vec<String> {
	let mut parts = Vec::new();
	let mut depth = 0i32;
	let mut last = 0;
	for token in super::scan::tokenize(s) {
		let text = token.text(s);
		match token.kind {
			super::scan::TokenKind::Open => depth += 1,
			super::scan::TokenKind::Close => depth -= 1,
			super::scan::TokenKind::Punct if text == "<" => depth += 1,
			super::scan::TokenKind::Punct if text == ">" => depth -= 1,
			super::scan::TokenKind::Punct if depth == 0 && text.starts_with(sep) => {
				parts.push(s[last..token.start].trim().to_string());
				last = token.end;
			}
			_ => {}
		}
	}
	let tail = s[last..].trim();
	if !tail.is_empty() || !parts.is_empty() {
		parts.push(tail.to_string());
	}
	parts
}

/// Recursive-descent parser over the bytes of one kind.
struct KindParser<'a> {
	src: &'a str,
	pos: usize,
	problems: Vec<KindProblem>,
}

type Parsed = Result<FieldType, KindProblem>;

impl<'a> KindParser<'a> {
	fn new(src: &'a str) -> Self {
		Self {
			src,
			pos: 0,
			problems: Vec::new(),
		}
	}

	fn bytes(&self) -> &'a [u8] {
		self.src.as_bytes()
	}

	fn at_end(&self) -> bool {
		self.pos >= self.src.len()
	}

	fn peek(&self) -> Option<u8> {
		self.bytes().get(self.pos).copied()
	}

	fn rest(&self) -> &'a str {
		&self.src[self.pos..]
	}

	fn skip_ws(&mut self) {
		while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
			self.pos += 1;
		}
	}

	/// A problem from `start` to the current position: never empty unless the
	/// text is, pointing at the last character when the text ran out, and always
	/// on character boundaries.
	fn error(&self, start: usize, reason: impl Into<String>) -> KindProblem {
		let len = self.src.len();
		let (mut from, mut to) = if len == 0 {
			(0, 0)
		} else if start >= len {
			(len - 1, len)
		} else {
			(start, self.pos.clamp(start + 1, len))
		};
		while !self.src.is_char_boundary(from) {
			from -= 1;
		}
		while !self.src.is_char_boundary(to) {
			to += 1;
		}
		KindProblem {
			span: from..to,
			reason: reason.into(),
		}
	}

	fn expect(&mut self, byte: u8, what: &str) -> Result<(), KindProblem> {
		self.skip_ws();
		if self.peek() == Some(byte) {
			self.pos += 1;
			Ok(())
		} else {
			let start = self.pos;
			self.pos = (self.pos + 1).min(self.src.len());
			Err(self.error(start, format!("expected {what}")))
		}
	}

	/// `primary ('|' primary)*` — authored order is kept.
	fn union(&mut self) -> Parsed {
		let mut variants = vec![self.primary()?];
		loop {
			self.skip_ws();
			if self.peek() == Some(b'|') {
				self.pos += 1;
				variants.push(self.primary()?);
			} else {
				break;
			}
		}
		Ok(if variants.len() == 1 {
			variants.remove(0)
		} else {
			FieldType::Union {
				variants,
			}
		})
	}

	fn primary(&mut self) -> Parsed {
		self.skip_ws();
		let start = self.pos;
		match self.peek() {
			None => Err(self.error(start, "expected a kind")),
			Some(q @ (b'\'' | b'"')) => self.string_literal(q),
			Some(b'0'..=b'9' | b'-' | b'+') => self.number_literal(),
			Some(b'{') => self.object_literal(),
			Some(b'[') => self.tuple(),
			Some(b) if b.is_ascii_alphabetic() || b == b'_' => self.named_kind(),
			Some(_) => {
				self.pos += 1;
				Err(self.error(start, "unexpected character where a kind was expected"))
			}
		}
	}

	fn read_word(&mut self) -> &'a str {
		let start = self.pos;
		while self.peek().is_some_and(super::scan::is_word_byte) {
			self.pos += 1;
		}
		&self.src[start..self.pos]
	}

	/// After a name, consume `<` if one follows (spaces allowed) and say so.
	fn open_generic(&mut self) -> bool {
		let save = self.pos;
		self.skip_ws();
		if self.peek() == Some(b'<') {
			self.pos += 1;
			true
		} else {
			self.pos = save;
			false
		}
	}

	fn named_kind(&mut self) -> Parsed {
		let start = self.pos;
		let name = self.read_word();
		let lower = name.to_ascii_lowercase();
		match lower.as_str() {
			"option" => {
				if !self.open_generic() {
					return Err(self.error(start, "`option` needs a kind: `option<T>`"));
				}
				let inner = self.union()?;
				self.expect(b'>', "`>` to close `option<`")?;
				Ok(FieldType::Option {
					inner: Box::new(inner),
				})
			}
			"array" | "set" => {
				let (inner, max) = if self.open_generic() {
					self.collection_args()?
				} else {
					(
						FieldType::Primitive {
							name: PrimitiveType::Any,
						},
						None,
					)
				};
				let inner = Box::new(inner);
				Ok(if lower == "array" {
					FieldType::Array {
						inner,
						max,
					}
				} else {
					FieldType::Set {
						inner,
						max,
					}
				})
			}
			"record" | "table" | "file" | "geometry" => {
				let names = if self.open_generic() {
					self.name_list()?
				} else {
					Vec::new()
				};
				Ok(match lower.as_str() {
					"record" => FieldType::Record {
						tables: names
							.into_iter()
							.filter(|t| !t.eq_ignore_ascii_case("any"))
							.collect(),
					},
					"table" => FieldType::Table {
						tables: names,
					},
					"file" => FieldType::File {
						buckets: names,
					},
					_ => FieldType::Geometry {
						kinds: names,
					},
				})
			}
			"true" | "false" if !self.next_is_generic() => Ok(FieldType::Literal {
				value: serde_json::Value::Bool(lower == "true"),
				literal_type: None,
			}),
			_ if (name == "NaN" || name == "Infinity") && !self.next_is_generic() => {
				Ok(float_literal(name.to_string()))
			}
			_ => match primitive(&lower) {
				Some(prim) if !self.next_is_generic() => Ok(FieldType::Primitive {
					name: prim,
				}),
				Some(_) => self.local_unknown(start, format!("`{name}` takes no type parameter")),
				None => self.local_unknown(start, unknown_name_reason(name)),
			},
		}
	}

	fn next_is_generic(&self) -> bool {
		self.rest().trim_start().starts_with('<')
	}

	/// An unrecognised name (with its `<…>`, if any) becomes an unknown kind in
	/// place, so the rest of the kind still parses.
	fn local_unknown(&mut self, start: usize, reason: String) -> Parsed {
		if self.open_generic() {
			self.skip_balanced_angle(start)?;
		}
		let source = self.src[start..self.pos].trim().to_string();
		self.problems.push(KindProblem {
			span: start..self.pos,
			reason,
		});
		Ok(FieldType::Unknown {
			source,
		})
	}

	/// With the opening `<` consumed, skip to just past its matching `>`.
	fn skip_balanced_angle(&mut self, start: usize) -> Result<(), KindProblem> {
		let mut depth = 1;
		while let Some(b) = self.peek() {
			match b {
				b'\'' | b'"' | b'`' => {
					self.string_body(b)?;
					continue;
				}
				b'<' | b'(' | b'[' | b'{' => depth += 1,
				b'>' | b')' | b']' | b'}' => {
					depth -= 1;
					if depth == 0 {
						self.pos += 1;
						return Ok(());
					}
				}
				_ => {}
			}
			self.pos += 1;
		}
		Err(self.error(start, "unbalanced `<`"))
	}

	/// `<T>` or `<T, N>`, with the `<` consumed.
	fn collection_args(&mut self) -> Result<(FieldType, Option<u64>), KindProblem> {
		let inner = self.union()?;
		self.skip_ws();
		let mut max = None;
		if self.peek() == Some(b',') {
			self.pos += 1;
			self.skip_ws();
			let start = self.pos;
			let digits = self.read_word();
			max = Some(
				digits
					.parse::<u64>()
					.map_err(|_| self.error(start, "expected a length after `,`"))?,
			);
		}
		self.expect(b'>', "`>` to close the collection kind")?;
		Ok((inner, max))
	}

	/// `name ('|' name)* '>'` with the `<` consumed. Names may be backtick- or
	/// `⟨⟩`-quoted (the engine quotes names that are not plain identifiers).
	fn name_list(&mut self) -> Result<Vec<String>, KindProblem> {
		let mut names = Vec::new();
		loop {
			self.skip_ws();
			let start = self.pos;
			let name = match self.peek() {
				Some(b'`') => {
					self.string_body(b'`')?;
					super::scan::unquote(&self.src[start..self.pos])
				}
				_ if self.rest().starts_with('\u{27E8}') => {
					let end = self
						.rest()
						.find('\u{27E9}')
						.ok_or_else(|| self.error(start, "unterminated `⟨` name"))?;
					self.pos += end + '\u{27E9}'.len_utf8();
					super::scan::unquote(&self.src[start..self.pos])
				}
				_ => {
					let word = self.read_word();
					if word.is_empty() {
						self.pos = (self.pos + 1).min(self.src.len());
						return Err(self.error(start, "expected a name"));
					}
					word.to_string()
				}
			};
			names.push(name);
			self.skip_ws();
			match self.peek() {
				Some(b'|') => self.pos += 1,
				Some(b'>') => {
					self.pos += 1;
					return Ok(names);
				}
				_ => {
					let at = self.pos;
					self.pos = (self.pos + 1).min(self.src.len());
					return Err(self.error(at, "expected `|` or `>` in a name list"));
				}
			}
		}
	}

	/// Skip a quoted run starting at the opening `quote`, honouring backslash
	/// escapes, and return its raw body.
	fn string_body(&mut self, quote: u8) -> Result<&'a str, KindProblem> {
		let start = self.pos;
		self.pos += 1;
		while let Some(b) = self.peek() {
			match b {
				b'\\' => self.pos = (self.pos + 2).min(self.src.len()),
				_ if b == quote => {
					self.pos += 1;
					return Ok(&self.src[start + 1..self.pos - 1]);
				}
				_ => self.pos += 1,
			}
		}
		Err(self.error(start, "unterminated quoted text"))
	}

	fn string_literal(&mut self, quote: u8) -> Parsed {
		let body = self.string_body(quote)?;
		Ok(FieldType::Literal {
			value: serde_json::Value::String(super::scan::unescape(body)),
			literal_type: None,
		})
	}

	/// Integers, floats (`1.5f`, `-Infinity`), decimals (`10dec`) and durations
	/// (`1d`, `1h30m`, `500ms`).
	fn number_literal(&mut self) -> Parsed {
		let start = self.pos;
		if matches!(self.peek(), Some(b'-' | b'+')) {
			self.pos += 1;
		}
		if self.rest().starts_with("Infinity") {
			self.pos += "Infinity".len();
			return Ok(float_literal(
				self.src[start..self.pos].trim_start_matches('+').to_string(),
			));
		}
		let digits_start = self.pos;
		self.eat_digits();
		if self.pos == digits_start {
			return Err(self.error(start, "expected a number"));
		}
		let mut fractional = false;
		if self.peek() == Some(b'.')
			&& self.byte_at(self.pos + 1).is_some_and(|b| b.is_ascii_digit())
		{
			fractional = true;
			self.pos += 1;
			self.eat_digits();
		}
		if matches!(self.peek(), Some(b'e' | b'E')) && self.exponent_follows() {
			fractional = true;
			self.pos += 1;
			if matches!(self.peek(), Some(b'-' | b'+')) {
				self.pos += 1;
			}
			self.eat_digits();
		}
		let number = self.src[start..self.pos].trim_start_matches('+');
		let suffix_start = self.pos;
		let suffix = self.read_unit();
		match suffix {
			"" => {
				let value = serde_json::from_str::<serde_json::Number>(number)
					.map_err(|_| self.error(start, format!("`{number}` is not a number")))?;
				Ok(FieldType::Literal {
					value: serde_json::Value::Number(value),
					literal_type: None,
				})
			}
			"f" => {
				let value = number
					.parse::<f64>()
					.ok()
					.and_then(serde_json::Number::from_f64)
					.map(serde_json::Value::Number)
					.ok_or_else(|| self.error(start, format!("`{number}f` is not a float")))?;
				Ok(FieldType::Literal {
					value,
					literal_type: Some(LiteralType::Float),
				})
			}
			"dec" => Ok(FieldType::Literal {
				value: serde_json::Value::String(number.to_string()),
				literal_type: Some(LiteralType::Decimal),
			}),
			unit if !fractional && is_duration_unit(unit) => {
				// Further parts of a compound duration: `1h30m`.
				loop {
					let part = self.pos;
					self.eat_digits();
					if self.pos == part {
						break;
					}
					if !is_duration_unit(self.read_unit()) {
						return Err(self.error(start, "malformed duration literal"));
					}
				}
				Ok(FieldType::Literal {
					value: serde_json::Value::String(self.src[start..self.pos].to_string()),
					literal_type: Some(LiteralType::Duration),
				})
			}
			other => {
				self.pos = suffix_start + other.len();
				Err(self.error(suffix_start, format!("unrecognised number suffix `{other}`")))
			}
		}
	}

	fn byte_at(&self, at: usize) -> Option<u8> {
		self.bytes().get(at).copied()
	}

	fn eat_digits(&mut self) {
		while self.peek().is_some_and(|b| b.is_ascii_digit()) {
			self.pos += 1;
		}
	}

	fn exponent_follows(&self) -> bool {
		match self.byte_at(self.pos + 1) {
			Some(b) if b.is_ascii_digit() => true,
			Some(b'-' | b'+') => self.byte_at(self.pos + 2).is_some_and(|b| b.is_ascii_digit()),
			_ => false,
		}
	}

	/// A run of ASCII letters, or `µs`.
	fn read_unit(&mut self) -> &'a str {
		let start = self.pos;
		if self.rest().starts_with("µs") {
			self.pos += "µs".len();
			return &self.src[start..self.pos];
		}
		while self.peek().is_some_and(|b| b.is_ascii_alphabetic()) {
			self.pos += 1;
		}
		&self.src[start..self.pos]
	}

	/// `{ key: kind, … }`. Keys may be bare or quoted.
	fn object_literal(&mut self) -> Parsed {
		let open = self.pos;
		self.pos += 1;
		let mut fields = Vec::new();
		loop {
			self.skip_ws();
			if self.peek() == Some(b'}') {
				self.pos += 1;
				return Ok(FieldType::Object {
					fields,
				});
			}
			let start = self.pos;
			let name = match self.peek() {
				Some(q @ (b'\'' | b'"' | b'`')) => super::scan::unescape(self.string_body(q)?),
				_ => {
					let word = self.read_word();
					if word.is_empty() {
						self.pos = (self.pos + 1).min(self.src.len());
						return Err(self.error(start, "expected an object key"));
					}
					word.to_string()
				}
			};
			self.expect(b':', "`:` after an object key")?;
			let kind = self.union()?;
			fields.push(ObjectField {
				name,
				r#type: kind,
			});
			self.skip_ws();
			match self.peek() {
				Some(b',') => self.pos += 1,
				Some(b'}') => {}
				None => return Err(self.error(open, "unterminated object kind")),
				_ => {
					let at = self.pos;
					self.pos += 1;
					return Err(self.error(at, "expected `,` or `}` in an object kind"));
				}
			}
		}
	}

	/// `[A, B, …]`.
	fn tuple(&mut self) -> Parsed {
		let open = self.pos;
		self.pos += 1;
		let mut items = Vec::new();
		loop {
			self.skip_ws();
			if self.peek() == Some(b']') {
				self.pos += 1;
				return Ok(FieldType::Tuple {
					items,
				});
			}
			items.push(self.union()?);
			self.skip_ws();
			match self.peek() {
				Some(b',') => self.pos += 1,
				Some(b']') => {}
				None => return Err(self.error(open, "unterminated tuple kind")),
				_ => {
					let at = self.pos;
					self.pos += 1;
					return Err(self.error(at, "expected `,` or `]` in a tuple kind"));
				}
			}
		}
	}
}

fn float_literal(text: String) -> FieldType {
	FieldType::Literal {
		value: serde_json::Value::String(text),
		literal_type: Some(LiteralType::Float),
	}
}

fn is_duration_unit(unit: &str) -> bool {
	matches!(unit, "ns" | "us" | "µs" | "ms" | "s" | "m" | "h" | "d" | "w" | "y")
}

fn unknown_name_reason(name: &str) -> String {
	match name.to_ascii_lowercase().as_str() {
		"references" => "`references` is not a SurrealDB 3.x kind; a reverse reference is a \
		                 `COMPUTED <~table` lookup"
			.to_string(),
		"either" => {
			"`either<…>` is not a SurrealDB 3.x kind; write the variants as a union `A | B`"
				.to_string()
		}
		"literal" => "`literal` is not a kind name; write the literal value itself".to_string(),
		_ => format!("unrecognised kind `{name}`"),
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
		"regex" => PrimitiveType::Regex,
		"range" => PrimitiveType::Range,
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

	fn lit(value: serde_json::Value, literal_type: Option<LiteralType>) -> FieldType {
		FieldType::Literal {
			value,
			literal_type,
		}
	}

	fn s(v: &str) -> FieldType {
		lit(serde_json::json!(v), None)
	}

	fn names(v: &[&str]) -> Vec<String> {
		v.iter().map(|n| n.to_string()).collect()
	}

	/// Parse and require that nothing was unrecognised.
	fn clean(src: &str) -> FieldType {
		let (kind, problems) = parse_type_checked(src);
		assert!(problems.is_empty(), "{src}: {problems:?}");
		kind
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
	#[test_case("none", PrimitiveType::None)]
	#[test_case("object", PrimitiveType::Object)]
	#[test_case("function", PrimitiveType::Function)]
	#[test_case("regex", PrimitiveType::Regex)]
	#[test_case("range", PrimitiveType::Range)]
	fn primitives(src: &str, expected: PrimitiveType) {
		assert_eq!(clean(src), prim(expected));
	}

	#[test]
	fn option_wraps_inner() {
		assert_eq!(
			clean("option<string>"),
			FieldType::Option {
				inner: Box::new(prim(PrimitiveType::String))
			}
		);
	}

	#[test]
	fn array_without_max() {
		assert_eq!(
			clean("array<string>"),
			FieldType::Array {
				inner: Box::new(prim(PrimitiveType::String)),
				max: None
			}
		);
	}

	#[test]
	fn array_with_max() {
		assert_eq!(
			clean("array<int, 10>"),
			FieldType::Array {
				inner: Box::new(prim(PrimitiveType::Int)),
				max: Some(10)
			}
		);
	}

	#[test]
	fn bare_collections_hold_any() {
		for (src, set) in [("array", false), ("set", true)] {
			let inner = Box::new(prim(PrimitiveType::Any));
			let expected = if set {
				FieldType::Set {
					inner,
					max: None,
				}
			} else {
				FieldType::Array {
					inner,
					max: None,
				}
			};
			assert_eq!(clean(src), expected);
		}
	}

	#[test]
	fn set_of_records() {
		assert_eq!(
			clean("set<record<user>>"),
			FieldType::Set {
				inner: Box::new(FieldType::Record {
					tables: names(&["user"])
				}),
				max: None
			}
		);
	}

	#[test]
	fn record_single_and_union_and_bare() {
		assert_eq!(
			clean("record<user>"),
			FieldType::Record {
				tables: names(&["user"])
			}
		);
		assert_eq!(
			clean("record<user | admin>"),
			FieldType::Record {
				tables: names(&["user", "admin"])
			}
		);
		assert_eq!(
			clean("record"),
			FieldType::Record {
				tables: vec![]
			}
		);
		assert_eq!(
			clean("record<any>"),
			FieldType::Record {
				tables: vec![]
			}
		);
	}

	#[test]
	fn quoted_table_names_are_unquoted() {
		assert_eq!(
			clean("record<`my-table` | `a|b` | ⟨odd name⟩>"),
			FieldType::Record {
				tables: names(&["my-table", "a|b", "odd name"])
			}
		);
	}

	#[test]
	fn table_and_file_kinds() {
		assert_eq!(
			clean("table"),
			FieldType::Table {
				tables: vec![]
			}
		);
		assert_eq!(
			clean("table<person | team>"),
			FieldType::Table {
				tables: names(&["person", "team"])
			}
		);
		assert_eq!(
			clean("file"),
			FieldType::File {
				buckets: vec![]
			}
		);
		assert_eq!(
			clean("file<avatars | docs>"),
			FieldType::File {
				buckets: names(&["avatars", "docs"])
			}
		);
	}

	#[test]
	fn geometry_kinds() {
		assert_eq!(
			clean("geometry<point | polygon>"),
			FieldType::Geometry {
				kinds: names(&["point", "polygon"])
			}
		);
	}

	#[test]
	fn union_of_primitives_with_null() {
		assert_eq!(
			clean("string | int | null"),
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
			clean("'a' | 'b'"),
			FieldType::Union {
				variants: vec![s("a"), s("b")]
			}
		);
	}

	#[test]
	fn string_literals_resolve_escapes() {
		assert_eq!(clean(r#""it's \"x\"""#), s(r#"it's "x""#));
		assert_eq!(clean(r"'it\'s'"), s("it's"));
		assert_eq!(clean(r"'a\nb'"), s("a\nb"));
	}

	#[test]
	fn numeric_literal_union() {
		assert_eq!(
			clean("200 | 404"),
			FieldType::Union {
				variants: vec![
					lit(serde_json::json!(200), None),
					lit(serde_json::json!(404), None)
				]
			}
		);
	}

	#[test]
	fn typed_number_literals_survive_engine_spacing() {
		// Exactly what 3.2.4 and 3.3.0 render for `-5 | 0 | 1.5 | 10dec`.
		assert_eq!(
			clean("-5 | 0 |  1.5f |  10dec"),
			FieldType::Union {
				variants: vec![
					lit(serde_json::json!(-5), None),
					lit(serde_json::json!(0), None),
					lit(serde_json::json!(1.5), Some(LiteralType::Float)),
					lit(serde_json::json!("10"), Some(LiteralType::Decimal)),
				]
			}
		);
		assert_eq!(clean("0.5dec"), lit(serde_json::json!("0.5"), Some(LiteralType::Decimal)));
		assert_eq!(clean("NaN"), lit(serde_json::json!("NaN"), Some(LiteralType::Float)));
		assert_eq!(
			clean("-Infinity"),
			lit(serde_json::json!("-Infinity"), Some(LiteralType::Float))
		);
	}

	#[test]
	fn duration_literals() {
		for d in ["1d", "1h30m", "500ms", "3µs", "2us", "1y2w3d4h5m6s7ms8us9ns"] {
			assert_eq!(clean(d), lit(serde_json::json!(d), Some(LiteralType::Duration)), "{d}");
		}
		assert_eq!(
			clean("'auto' | 1 | true | 1d"),
			FieldType::Union {
				variants: vec![
					s("auto"),
					lit(serde_json::json!(1), None),
					lit(serde_json::json!(true), None),
					lit(serde_json::json!("1d"), Some(LiteralType::Duration)),
				]
			}
		);
	}

	#[test]
	fn tuples() {
		assert_eq!(
			clean("[string, int, none | bool]"),
			FieldType::Tuple {
				items: vec![
					prim(PrimitiveType::String),
					prim(PrimitiveType::Int),
					FieldType::Union {
						variants: vec![prim(PrimitiveType::None), prim(PrimitiveType::Bool)]
					},
				]
			}
		);
		assert_eq!(
			clean("[]"),
			FieldType::Tuple {
				items: vec![]
			}
		);
	}

	#[test]
	fn object_literals_nest_and_keep_commas_inside() {
		let field = |name: &str, t: FieldType| ObjectField {
			name: name.into(),
			r#type: t,
		};
		assert_eq!(
			clean("{ a: int, b: none | string, c: { d: bool }, e: array<int, 2> }"),
			FieldType::Object {
				fields: vec![
					field("a", prim(PrimitiveType::Int)),
					field(
						"b",
						FieldType::Union {
							variants: vec![prim(PrimitiveType::None), prim(PrimitiveType::String)]
						}
					),
					field(
						"c",
						FieldType::Object {
							fields: vec![field("d", prim(PrimitiveType::Bool))]
						}
					),
					field(
						"e",
						FieldType::Array {
							inner: Box::new(prim(PrimitiveType::Int)),
							max: Some(2)
						}
					),
				]
			}
		);
		// Quoted keys (the engine double-quotes keys that are not identifiers), and
		// the empty literal object the engine renders as `{  }`.
		assert_eq!(
			clean(r#"{ "odd key": int, "q\"k": bool }"#),
			FieldType::Object {
				fields: vec![
					field("odd key", prim(PrimitiveType::Int)),
					field("q\"k", prim(PrimitiveType::Bool))
				]
			}
		);
		assert_eq!(
			clean("{  }"),
			FieldType::Object {
				fields: vec![]
			}
		);
		// A tagged union of object literals.
		assert!(matches!(
			clean("{ kind: 'circle', r: float } | { h: float, kind: 'rect', w: float }"),
			FieldType::Union { variants } if variants.len() == 2
		));
	}

	#[test]
	fn deeply_nested() {
		assert_eq!(
			clean("option<array<record<user>>>"),
			FieldType::Option {
				inner: Box::new(FieldType::Array {
					inner: Box::new(FieldType::Record {
						tables: names(&["user"])
					}),
					max: None
				})
			}
		);
	}

	#[test]
	fn array_of_options() {
		assert_eq!(
			clean("array<option<string>>"),
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
		let (kind, problems) = parse_type_checked("@@@ not a type @@@");
		assert_eq!(
			kind,
			FieldType::Unknown {
				source: "@@@ not a type @@@".to_string()
			}
		);
		assert_eq!(problems.len(), 1);
		// Should not panic on empty input either.
		let (kind, problems) = parse_type_checked("");
		assert!(matches!(kind, FieldType::Unknown { .. }));
		assert_eq!(problems.len(), 1);
	}

	#[test]
	fn an_unknown_name_stays_local_and_is_reported() {
		let src = "array<frob>";
		let (kind, problems) = parse_type_checked(src);
		assert_eq!(
			kind,
			FieldType::Array {
				inner: Box::new(FieldType::Unknown {
					source: "frob".into()
				}),
				max: None
			}
		);
		assert_eq!(problems.len(), 1);
		assert_eq!(&src[problems[0].span.clone()], "frob");
		assert!(problems[0].reason.contains("unrecognised kind `frob`"), "{problems:?}");
	}

	#[test_case("range<int>", "takes no type parameter" ; "range takes no parameter")]
	#[test_case("references<person>", "COMPUTED <~table" ; "references was removed")]
	#[test_case("references", "COMPUTED <~table" ; "bare references")]
	#[test_case("either<int, string>", "union" ; "either was removed")]
	#[test_case("literal", "literal value itself" ; "literal is not a kind")]
	fn removed_and_misspelt_kinds_are_explained(src: &str, reason: &str) {
		let (kind, problems) = parse_type_checked(src);
		assert_eq!(
			kind,
			FieldType::Unknown {
				source: src.into()
			}
		);
		assert_eq!(problems.len(), 1, "{problems:?}");
		assert!(problems[0].reason.contains(reason), "{problems:?}");
	}

	#[test_case("array<int" ; "unclosed generic")]
	#[test_case("{ a: int" ; "unclosed object")]
	#[test_case("[int, " ; "unclosed tuple")]
	#[test_case("'abc" ; "unterminated string")]
	#[test_case("int |" ; "dangling bar")]
	#[test_case("10xyz" ; "bad suffix")]
	#[test_case("object FLEXIBLE" ; "trailing clause keyword")]
	#[test_case("record<a b>" ; "missing bar")]
	fn syntax_errors_make_the_whole_kind_unknown(src: &str) {
		let (kind, problems) = parse_type_checked(src);
		assert_eq!(
			kind,
			FieldType::Unknown {
				source: src.trim().into()
			}
		);
		assert_eq!(problems.len(), 1, "{problems:?}");
		let span = problems[0].span.clone();
		assert!(span.start < span.end && span.end <= src.len(), "{span:?} in {src:?}");
	}

	#[test]
	fn prefix_parse_stops_after_the_kind() {
		let src = "{ a: int } { RETURN { a: 1 } }";
		let (kind, used, problems) = parse_type_prefix(src);
		assert!(problems.is_empty());
		assert!(matches!(kind, FieldType::Object { .. }));
		assert_eq!(src[used..].trim_start(), "{ RETURN { a: 1 } }");
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
	fn split_top_level_respects_every_bracket_and_quote() {
		assert_eq!(
			split_top_level("$a: { x: int, y: int }, $b: array<int, 2>, $c: 'a,b'", ','),
			vec!["$a: { x: int, y: int }", "$b: array<int, 2>", "$c: 'a,b'"]
		);
		assert_eq!(split_top_level(r"'it\'s, ok', x", ','), vec![r"'it\'s, ok'", "x"]);
		assert!(split_top_level("", ',').is_empty());
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
		// 3.x writes FLEXIBLE after the kind; it used to be swallowed into it.
		assert_eq!(
			extract_type_clause("DEFINE FIELD meta ON user TYPE object FLEXIBLE PERMISSIONS FULL"),
			Some("object".to_string())
		);
	}

	#[test]
	fn extract_stops_at_computed_and_inline() {
		assert_eq!(
			extract_type_clause("DEFINE FIELD d ON p TYPE string COMPUTED string::concat(a, '!')"),
			Some("string".to_string())
		);
		assert_eq!(
			extract_type_clause("DEFINE FIELD w ON r TYPE none | float INLINE PERMISSIONS FULL"),
			Some("none | float".to_string())
		);
	}

	#[test]
	fn extract_none_when_no_type() {
		assert_eq!(extract_type_clause("DEFINE FIELD x ON user VALUE 1"), None);
	}

	#[test]
	fn extract_does_not_split_inside_generics() {
		assert_eq!(
			extract_type_clause("DEFINE FIELD r ON user TYPE record<user | admin> READONLY"),
			Some("record<user | admin>".to_string())
		);
		// A table called `value` inside the generic is not the VALUE clause.
		assert_eq!(
			extract_type_clause("DEFINE FIELD r ON user TYPE record<value> VALUE $value"),
			Some("record<value>".to_string())
		);
	}
}
