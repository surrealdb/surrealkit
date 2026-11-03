//! Expression facts schema document v2 derives from engine-rendered SurrealQL: whether a
//! `VALUE` reads its input, whether it has a fallback, the bounded `ASSERT` recognition of
//! contract section 8, and the statement-parenthesis normalisation of section 15.
//!
//! Nothing here evaluates SurrealQL. Expressions are tokenised with the typegen lexer, so a
//! keyword or `$value` inside a string literal never counts.

use super::canonical::canonical_of;
use super::document::{Bound, Kind, LengthTarget, LiteralType, RangeValueKind, Refinement};
use super::kind_v2::parse_top;
use super::scan::{Token, TokenKind, tokenize};
use super::type_parser::split_top_level;

/// The statement keywords whose enclosing parentheses `structure-3.3` (and plain `INFO` on
/// every version) adds, and the canonical form removes.
const STATEMENT_KEYWORDS: &[&str] =
	&["CREATE", "UPDATE", "UPSERT", "DELETE", "SELECT", "INSERT", "RELATE"];

/// Whether token `i` is `$` immediately followed by the word `name`.
fn is_param(src: &str, tokens: &[Token], i: usize, name: &str) -> bool {
	let (Some(dollar), Some(word)) = (tokens.get(i), tokens.get(i + 1)) else {
		return false;
	};
	dollar.kind == TokenKind::Punct
		&& dollar.text(src) == "$"
		&& word.kind == TokenKind::Word
		&& word.start == dollar.end
		&& word.text(src) == name
}

/// Whether `expr` references `$value` or `$input` outside string literals.
pub(super) fn reads_input(expr: &str) -> bool {
	let tokens = tokenize(expr);
	(0..tokens.len())
		.any(|i| is_param(expr, &tokens, i, "value") || is_param(expr, &tokens, i, "input"))
}

/// Whether `expr` supplies its own fallback: `??`, `?:` or `IF`, outside string literals.
pub(super) fn has_fallback(expr: &str) -> bool {
	let tokens = tokenize(expr);
	tokens.iter().enumerate().any(|(i, t)| {
		if t.is_word(expr, "IF") {
			return true;
		}
		t.kind == TokenKind::Punct
			&& t.text(expr) == "?"
			&& tokens.get(i + 1).is_some_and(|n| {
				n.kind == TokenKind::Punct && n.start == t.end && matches!(n.text(expr), "?" | ":")
			})
	})
}

/// Remove one pair of parentheses enclosing the whole of `text` when what is inside starts
/// with a statement keyword: `(CREATE x SET …)` becomes `CREATE x SET …`. Blocks and other
/// expressions are returned unchanged.
pub(super) fn strip_statement_parens(text: &str) -> String {
	let trimmed = text.trim();
	match enclosed(trimmed) {
		Some(inner) => {
			let inner = inner.trim();
			let first = tokenize(inner).first().copied();
			if first.is_some_and(|t| STATEMENT_KEYWORDS.iter().any(|kw| t.is_word(inner, kw))) {
				inner.to_string()
			} else {
				trimmed.to_string()
			}
		}
		None => trimmed.to_string(),
	}
}

/// The text inside `(…)` when one pair of parentheses encloses all of `text`.
fn enclosed(text: &str) -> Option<&str> {
	let tokens = tokenize(text);
	let first = tokens.first()?;
	if first.kind != TokenKind::Open || first.text(text) != "(" || first.start != 0 {
		return None;
	}
	let mut depth = 0i32;
	for (i, t) in tokens.iter().enumerate() {
		match t.kind {
			TokenKind::Open => depth += 1,
			TokenKind::Close => {
				depth -= 1;
				if depth == 0 {
					return (i == tokens.len() - 1 && t.end == text.len() && t.text(text) == ")")
						.then(|| &text[1..t.start]);
				}
			}
			_ => {}
		}
	}
	None
}

/// Split `text` on the top-level word `word` (outside brackets and strings).
fn split_on_word<'a>(text: &'a str, word: &str) -> Vec<&'a str> {
	let tokens = tokenize(text);
	let mut parts = Vec::new();
	let mut depth = 0i32;
	let mut last = 0;
	for t in &tokens {
		match t.kind {
			TokenKind::Open => depth += 1,
			TokenKind::Close => depth -= 1,
			TokenKind::Word if depth == 0 && t.is_word(text, word) => {
				parts.push(text[last..t.start].trim());
				last = t.end;
			}
			_ => {}
		}
	}
	parts.push(text[last..].trim());
	parts
}

/// The conjuncts of `text`, descending into parenthesised conjunctions. `None` when a
/// top-level `OR` remains.
fn conjuncts(text: &str) -> Option<Vec<&str>> {
	let text = text.trim();
	if split_on_word(text, "OR").len() > 1 {
		return None;
	}
	let parts = split_on_word(text, "AND");
	if parts.len() == 1 {
		return match enclosed(text) {
			Some(inner) => conjuncts(inner),
			None => Some(vec![text]),
		};
	}
	let mut out = Vec::new();
	for part in parts {
		out.extend(conjuncts(part)?);
	}
	Some(out)
}

/// `text` with whitespace between tokens removed (string literals kept intact).
fn compact(text: &str) -> String {
	tokenize(text).iter().map(|t| t.text(text)).collect()
}

/// Remove a leading `$value = NONE OR ` guard.
fn strip_none_guard(expr: &str) -> &str {
	let tokens = tokenize(expr);
	let guard = is_param(expr, &tokens, 0, "value")
		&& tokens.get(2).is_some_and(|t| t.text(expr) == "=")
		&& tokens.get(3).is_some_and(|t| t.is_word(expr, "NONE"))
		&& tokens.get(4).is_some_and(|t| t.is_word(expr, "OR"));
	match (guard, tokens.get(5)) {
		(true, Some(next)) => &expr[next.start..],
		_ => expr,
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
	Gt,
	Ge,
	Lt,
	Le,
}

impl Op {
	fn parse(text: &str) -> Option<(Op, &str)> {
		for (spelling, op) in [(">=", Op::Ge), ("<=", Op::Le), (">", Op::Gt), ("<", Op::Lt)] {
			if let Some(rest) = text.strip_prefix(spelling) {
				return Some((op, rest));
			}
		}
		None
	}

	/// The operator with its operands swapped: `5 < $value` is `$value > 5`.
	fn flipped(self) -> Op {
		match self {
			Op::Gt => Op::Lt,
			Op::Ge => Op::Le,
			Op::Lt => Op::Gt,
			Op::Le => Op::Ge,
		}
	}
}

/// One recognised conjunct.
enum Atom {
	Literals(Vec<Kind>),
	Range(Op, RangeValueKind, String),
	Length(LengthTarget, Op, u64),
	Email,
}

/// A single literal kind spelled in an expression, or `None`.
fn literal(text: &str) -> Option<(LiteralType, String, Kind)> {
	let parsed = parse_top(text.trim());
	if !parsed.problems.is_empty() || parsed.optional {
		return None;
	}
	match &parsed.kind {
		Kind::Literal {
			literal_type,
			value,
		} => {
			let value = match value {
				super::document::LiteralValue::Text(s) => s.clone(),
				super::document::LiteralValue::Bool(b) => b.to_string(),
			};
			Some((*literal_type, value, parsed.kind.clone()))
		}
		_ => None,
	}
}

fn range_kind(literal_type: LiteralType) -> Option<RangeValueKind> {
	match literal_type {
		LiteralType::Int => Some(RangeValueKind::Int),
		LiteralType::Float => Some(RangeValueKind::Float),
		LiteralType::Decimal => Some(RangeValueKind::Decimal),
		LiteralType::Duration => Some(RangeValueKind::Duration),
		LiteralType::String | LiteralType::Bool => None,
	}
}

fn atom(text: &str) -> Option<Atom> {
	let text = text.trim();
	let tokens = tokenize(text);
	// `$value INSIDE [ … ]` / `$value IN [ … ]`
	if is_param(text, &tokens, 0, "value")
		&& tokens.get(2).is_some_and(|t| t.is_word(text, "INSIDE") || t.is_word(text, "IN"))
	{
		let list = tokens.get(3)?;
		let body = enclosed_square(&text[list.start..])?;
		let mut values = Vec::new();
		for part in split_top_level(body, ',') {
			values.push(literal(&part)?.2);
		}
		if values.is_empty() {
			return None;
		}
		return Some(Atom::Literals(values));
	}
	let flat = compact(text);
	if flat == "string::is_email($value)" {
		return Some(Atom::Email);
	}
	for (call, target) in
		[("string::len($value)", LengthTarget::String), ("array::len($value)", LengthTarget::Array)]
	{
		if let Some(rest) = flat.strip_prefix(call) {
			let (op, number) = Op::parse(rest)?;
			let n = number.parse::<u64>().ok()?;
			return Some(Atom::Length(target, op, n));
		}
	}
	if let Some(rest) = flat.strip_prefix("$value") {
		let (op, number) = Op::parse(rest)?;
		let (lt, value, _) = literal(number)?;
		return Some(Atom::Range(op, range_kind(lt)?, value));
	}
	if let Some(lhs) = flat.strip_suffix("$value") {
		for (spelling, op) in [(">=", Op::Ge), ("<=", Op::Le), (">", Op::Gt), ("<", Op::Lt)] {
			if let Some(number) = lhs.strip_suffix(spelling) {
				let (lt, value, _) = literal(number)?;
				return Some(Atom::Range(op.flipped(), range_kind(lt)?, value));
			}
		}
	}
	None
}

/// The text inside `[…]` when one pair of square brackets encloses all of `text`.
fn enclosed_square(text: &str) -> Option<&str> {
	let tokens = tokenize(text);
	let first = tokens.first()?;
	let last = tokens.last()?;
	(first.text(text) == "["
		&& last.text(text) == "]"
		&& last.end == text.len()
		&& matching_close(&tokens) == Some(tokens.len() - 1))
	.then(|| &text[first.end..last.start])
}

/// The index of the token closing the bracket that opens at token 0.
fn matching_close(tokens: &[Token]) -> Option<usize> {
	let mut depth = 0i32;
	for (i, t) in tokens.iter().enumerate() {
		match t.kind {
			TokenKind::Open => depth += 1,
			TokenKind::Close => {
				depth -= 1;
				if depth == 0 {
					return Some(i);
				}
			}
			_ => {}
		}
	}
	None
}

/// Compare two range bounds of `kind` exactly, or `None` when they cannot be ordered exactly.
///
/// Ints compare as integers and durations as nanoseconds, never through `f64`, which cannot
/// tell 2^53 from 2^53 + 1. Decimals compare digit by digit. A float literal means an `f64`, so
/// floats compare as `f64` (a NaN cannot be ordered).
fn compare(kind: RangeValueKind, a: &str, b: &str) -> Option<std::cmp::Ordering> {
	match kind {
		RangeValueKind::Int => Some(a.parse::<i128>().ok()?.cmp(&b.parse::<i128>().ok()?)),
		RangeValueKind::Float => a.parse::<f64>().ok()?.partial_cmp(&b.parse::<f64>().ok()?),
		RangeValueKind::Decimal => compare_decimal(a, b),
		RangeValueKind::Duration => Some(duration_nanos(a)?.cmp(&duration_nanos(b)?)),
	}
}

/// A decimal literal as sign, significant digits and the power of ten of the last digit:
/// `-12.50` is `(true, "125", -1)`. Zero is `(false, "", 0)`, whatever its sign.
fn decimal_parts(text: &str) -> Option<(bool, String, i64)> {
	let (negative, rest) = match text.as_bytes().first()? {
		b'-' => (true, &text[1..]),
		b'+' => (false, &text[1..]),
		_ => (false, text),
	};
	let (mantissa, exponent) = match rest.find(['e', 'E']) {
		Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
		None => (rest, 0),
	};
	let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
	if int.is_empty() && frac.is_empty()
		|| !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
	{
		return None;
	}
	let frac_len = i64::try_from(frac.len()).ok()?;
	let digits = format!("{int}{frac}");
	let digits = digits.trim_start_matches('0');
	let trimmed = digits.trim_end_matches('0');
	if trimmed.is_empty() {
		return Some((false, String::new(), 0));
	}
	let dropped = i64::try_from(digits.len() - trimmed.len()).ok()?;
	let power = exponent.checked_sub(frac_len)?.checked_add(dropped)?;
	Some((negative, trimmed.to_string(), power))
}

/// Compare two decimal literals exactly.
fn compare_decimal(a: &str, b: &str) -> Option<std::cmp::Ordering> {
	use std::cmp::Ordering;
	let (a_neg, a_digits, a_pow) = decimal_parts(a)?;
	let (b_neg, b_digits, b_pow) = decimal_parts(b)?;
	let sign = |neg: bool, digits: &str| match (digits.is_empty(), neg) {
		(true, _) => 0i8,
		(false, true) => -1,
		(false, false) => 1,
	};
	let (a_sign, b_sign) = (sign(a_neg, &a_digits), sign(b_neg, &b_digits));
	if a_sign != b_sign || a_sign == 0 {
		return Some(a_sign.cmp(&b_sign));
	}
	// Same sign, both non-zero: compare magnitudes by the position of the leading digit,
	// then digit by digit.
	let lead = |digits: &str, pow: i64| -> Option<i64> {
		i64::try_from(digits.len()).ok()?.checked_add(pow)
	};
	let magnitude = match lead(&a_digits, a_pow)?.cmp(&lead(&b_digits, b_pow)?) {
		Ordering::Equal => {
			let width = a_digits.len().max(b_digits.len());
			format!("{a_digits:0<width$}").cmp(&format!("{b_digits:0<width$}"))
		}
		other => other,
	};
	Some(if a_sign < 0 {
		magnitude.reverse()
	} else {
		magnitude
	})
}

/// The nanoseconds in a SurrealQL duration literal such as `1h30m`.
fn duration_nanos(text: &str) -> Option<u128> {
	let mut total: u128 = 0;
	let mut rest = text;
	while !rest.is_empty() {
		let digits = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
		let n: u128 = rest[..digits].parse().ok()?;
		rest = &rest[digits..];
		let unit_len = rest.find(|c: char| c.is_ascii_digit()).unwrap_or(rest.len());
		let scale: u128 = match &rest[..unit_len] {
			"ns" => 1,
			"us" | "µs" => 1_000,
			"ms" => 1_000_000,
			"s" => 1_000_000_000,
			"m" => 60 * 1_000_000_000,
			"h" => 3_600 * 1_000_000_000,
			"d" => 86_400 * 1_000_000_000,
			"w" => 7 * 86_400 * 1_000_000_000,
			"y" => 365 * 86_400 * 1_000_000_000,
			_ => return None,
		};
		total = total.checked_add(n.checked_mul(scale)?)?;
		rest = &rest[unit_len..];
	}
	Some(total)
}

/// Keep the tighter of two bounds on the same side. `lower` says which side. `None` when the
/// two cannot be ordered exactly, so the assertion is not recognised.
fn tighter(kind: RangeValueKind, a: Bound, b: Bound, lower: bool) -> Option<Bound> {
	let a_wins = match compare(kind, &a.value, &b.value)? {
		std::cmp::Ordering::Equal => !a.inclusive,
		std::cmp::Ordering::Greater => lower,
		std::cmp::Ordering::Less => !lower,
	};
	Some(if a_wins {
		a
	} else {
		b
	})
}

/// Recognise `expr` (an `ASSERT` as the engine renders it). All or nothing: `None` as soon as
/// one conjunct is outside the grammar.
pub(super) fn recognize(expr: &str) -> Option<Vec<Refinement>> {
	let body = strip_none_guard(expr.trim());
	let parts = conjuncts(body)?;

	let mut literals: Option<Vec<(String, Kind)>> = None;
	let mut range: Option<(RangeValueKind, Option<Bound>, Option<Bound>)> = None;
	let mut lengths: Vec<(LengthTarget, Option<u64>, Option<u64>)> = Vec::new();
	let mut email = false;

	for part in parts {
		match atom(part)? {
			Atom::Email => email = true,
			Atom::Literals(values) => {
				let mut keyed: Vec<(String, Kind)> =
					values.into_iter().map(|k| (canonical_of(&k).unwrap_or_default(), k)).collect();
				keyed.sort_by(|a, b| a.0.cmp(&b.0));
				keyed.dedup_by(|a, b| a.0 == b.0);
				literals = Some(match literals {
					None => keyed,
					// Two memberships: the value is in both.
					Some(previous) => previous
						.into_iter()
						.filter(|(k, _)| keyed.iter().any(|(j, _)| j == k))
						.collect(),
				});
			}
			Atom::Range(op, kind, value) => {
				let (have_kind, mut min, mut max) = range.take().unwrap_or((kind, None, None));
				if have_kind != kind {
					return None;
				}
				let bound = Bound {
					value,
					inclusive: matches!(op, Op::Ge | Op::Le),
				};
				match op {
					Op::Gt | Op::Ge => {
						min = Some(match min {
							None => bound,
							Some(prev) => tighter(kind, prev, bound, true)?,
						});
					}
					Op::Lt | Op::Le => {
						max = Some(match max {
							None => bound,
							Some(prev) => tighter(kind, prev, bound, false)?,
						});
					}
				}
				range = Some((kind, min, max));
			}
			Atom::Length(target, op, n) => {
				let (lo, hi) = match op {
					Op::Ge => (Some(n), None),
					Op::Gt => (Some(n.checked_add(1)?), None),
					Op::Le => (None, Some(n)),
					Op::Lt => (None, Some(n.checked_sub(1)?)),
				};
				match lengths.iter_mut().find(|(t, _, _)| *t == target) {
					Some((_, min, max)) => {
						if let Some(lo) = lo {
							*min = Some(min.map_or(lo, |m| m.max(lo)));
						}
						if let Some(hi) = hi {
							*max = Some(max.map_or(hi, |m| m.min(hi)));
						}
					}
					None => lengths.push((target, lo, hi)),
				}
			}
		}
	}

	let mut out = Vec::new();
	if let Some(values) = literals {
		if values.is_empty() {
			return None;
		}
		out.push(Refinement::Literals {
			values: values.into_iter().map(|(_, k)| k).collect(),
		});
	}
	if let Some((value_kind, min, max)) = range {
		out.push(Refinement::Range {
			value_kind,
			min,
			max,
		});
	}
	for (target, min, max) in lengths {
		out.push(Refinement::Length {
			target,
			min,
			max,
		});
	}
	if email {
		out.push(Refinement::Email);
	}
	if out.is_empty() {
		return None;
	}
	out.sort_by_cached_key(|r| canonical_of(r).unwrap_or_default());
	Some(out)
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn rec(expr: &str) -> Option<serde_json::Value> {
		recognize(expr).map(|r| serde_json::to_value(r).unwrap())
	}

	#[test]
	fn reads_input_ignores_strings_and_other_params() {
		assert!(reads_input("string::lowercase($value)"));
		assert!(reads_input("$input.name"));
		assert!(!reads_input("time::now()"));
		assert!(!reads_input("'$value'"));
		assert!(!reads_input("$values"));
		assert!(!reads_input("string::slug(name)"));
	}

	#[test]
	fn fallbacks_are_detected() {
		assert!(has_fallback("string::lowercase($value ?? 'X')"));
		assert!(has_fallback("$value ?: 'x'"));
		assert!(has_fallback("IF $value THEN 1 ELSE 2 END"));
		assert!(!has_fallback("string::lowercase($value)"));
		assert!(!has_fallback("'a ?? b'"));
	}

	#[test]
	fn statement_parens_are_removed_and_other_parens_kept() {
		assert_eq!(
			strip_statement_parens("(CREATE audit_log SET x = 1)"),
			"CREATE audit_log SET x = 1"
		);
		assert_eq!(strip_statement_parens("(SELECT * FROM a)"), "SELECT * FROM a");
		assert_eq!(strip_statement_parens("CREATE a"), "CREATE a");
		assert_eq!(strip_statement_parens("{ CREATE a }"), "{ CREATE a }");
		assert_eq!(strip_statement_parens("(1 + 2)"), "(1 + 2)");
		assert_eq!(strip_statement_parens("(SELECT 1) + (SELECT 2)"), "(SELECT 1) + (SELECT 2)");
	}

	#[test]
	fn the_contract_examples_are_recognised() {
		assert_eq!(
			rec("$value = NONE OR $value >= 0 AND $value <= 150"),
			Some(json!([{"type": "range", "valueKind": "int",
				"min": {"value": "0", "inclusive": true}, "max": {"value": "150", "inclusive": true}}]))
		);
		assert_eq!(rec("string::is_email($value)"), Some(json!([{"type": "email"}])));
		assert_eq!(
			rec("string::len($value) >= 1 AND string::len($value) <= 80"),
			Some(json!([{"type": "length", "target": "string", "min": 1, "max": 80}]))
		);
		assert_eq!(
			rec("$value INSIDE ['member', 'admin', 'owner']"),
			Some(json!([{"type": "literals", "values": [
				{"kind": "literal", "type": "string", "value": "admin"},
				{"kind": "literal", "type": "string", "value": "member"},
				{"kind": "literal", "type": "string", "value": "owner"},
			]}]))
		);
		assert_eq!(
			rec("$value > 0f AND $value < 100f"),
			Some(json!([{"type": "range", "valueKind": "float",
				"min": {"value": "0", "inclusive": false}, "max": {"value": "100", "inclusive": false}}]))
		);
		assert_eq!(
			rec("$value >= 1m AND $value <= 1d"),
			Some(json!([{"type": "range", "valueKind": "duration",
				"min": {"value": "1m", "inclusive": true}, "max": {"value": "1d", "inclusive": true}}]))
		);
		assert_eq!(
			rec("array::len($value) < 4"),
			Some(json!([{"type": "length", "target": "array", "max": 3}]))
		);
		assert_eq!(
			rec("$value >= 0dec"),
			Some(
				json!([{"type": "range", "valueKind": "decimal", "min": {"value": "0", "inclusive": true}}])
			)
		);
	}

	#[test]
	fn bounds_on_one_side_intersect() {
		assert_eq!(
			rec("$value >= 0 AND $value > 5 AND 10 >= $value"),
			Some(json!([{"type": "range", "valueKind": "int",
				"min": {"value": "5", "inclusive": false}, "max": {"value": "10", "inclusive": true}}]))
		);
		assert_eq!(
			rec("($value >= 1 AND $value <= 3)"),
			Some(json!([{"type": "range", "valueKind": "int",
				"min": {"value": "1", "inclusive": true}, "max": {"value": "3", "inclusive": true}}]))
		);
	}

	#[test]
	fn bounds_beyond_f64_precision_are_ordered_exactly() {
		// 2^53 and 2^53 + 1 are the same f64; the tighter upper bound must still win.
		for expr in [
			"$value <= 9007199254740992 AND $value <= 9007199254740993",
			"$value <= 9007199254740993 AND $value <= 9007199254740992",
		] {
			assert_eq!(
				rec(expr),
				Some(json!([{"type": "range", "valueKind": "int",
					"max": {"value": "9007199254740992", "inclusive": true}}])),
				"{expr}"
			);
		}
		for expr in [
			"$value >= 0.10000000000000000001dec AND $value >= 0.1dec",
			"$value >= 0.1dec AND $value >= 0.10000000000000000001dec",
		] {
			assert_eq!(
				rec(expr).and_then(|r| r[0]["min"]["value"].as_str().map(str::to_string)),
				Some("0.10000000000000000001".to_string()),
				"{expr}"
			);
		}
	}

	#[test]
	fn decimals_compare_digit_by_digit() {
		use std::cmp::Ordering::{Equal, Greater, Less};
		for (a, b, want) in [
			("1", "1.0", Equal),
			("0", "-0.00", Equal),
			("-1.5", "-1.25", Less),
			("10", "9.99", Greater),
			("1e3", "999", Greater),
			("0.001", "1e-3", Equal),
			("-2", "1", Less),
			("123456789012345678901234567890", "123456789012345678901234567891", Less),
		] {
			assert_eq!(compare_decimal(a, b), Some(want), "{a} vs {b}");
			assert_eq!(compare_decimal(b, a), Some(want.reverse()), "{b} vs {a}");
		}
		assert_eq!(compare_decimal("1.2.3", "1"), None);
		assert_eq!(compare_decimal("", "1"), None);
	}

	#[test]
	fn anything_outside_the_grammar_is_not_recognised() {
		for expr in [
			"$value = NONE OR $value = /^[a-z]+$/",
			"$value = string::uppercase($value) OR $value CONTAINS '!'",
			"$value >= 0 AND $value <= 1.5f",
			"$value = 'x'",
			"$value >= other",
			"string::len($value) >= 1 OR string::len($value) = 0",
			"string::len($value) < 0",
			"$value >= 'a'",
			"$value INSIDE []",
		] {
			assert_eq!(rec(expr), None, "{expr}");
		}
	}
}
