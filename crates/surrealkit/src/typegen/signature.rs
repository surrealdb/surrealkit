//! Pure parser for `DEFINE FUNCTION` signatures. No database or IO.
//!
//! Extracts the function name, its typed arguments, and an optional `RETURNS`
//! type out of a statement such as:
//!
//! ```text
//! DEFINE FUNCTION fn::greet($name: string, $loud: option<bool>) -> string { ... }
//! ```
//!
//! SurrealDB emits the return type with an arrow (`-> string`) between the
//! argument list and the body, and normalises `option<T>` arguments to
//! `none | T`.

use super::type_parser::{parse_type, split_top_level, unwrap_optional};
use super::types::{FieldType, FnArg};

/// Result of parsing a `DEFINE FUNCTION` statement.
pub struct FnSignature {
	/// The `fn::…` name, if found in the statement.
	pub name: Option<String>,
	pub args: Vec<FnArg>,
	pub returns: Option<FieldType>,
}

pub fn parse_function(stmt: &str) -> FnSignature {
	let chars: Vec<char> = stmt.chars().collect();
	let name = extract_fn_name(stmt);
	let args = arg_list_bounds(&chars)
		.map(|(s, e)| parse_args(&collect(&chars, s, e)))
		.unwrap_or_default();

	// The return type sits between the closing `)` of the arg list and the
	// opening `{` of the body, prefixed by `->`.
	let returns = extract_arrow_return(&chars).map(|s| parse_type(&s));

	FnSignature {
		name,
		args,
		returns,
	}
}

fn collect(chars: &[char], start: usize, end: usize) -> String {
	chars[start..end].iter().collect()
}

fn extract_fn_name(stmt: &str) -> Option<String> {
	let idx = stmt.find("fn::")?;
	let rest = &stmt[idx..];
	let end = rest.find('(').unwrap_or(rest.len());
	let name = rest[..end].trim();
	if name.is_empty() {
		None
	} else {
		Some(name.to_string())
	}
}

/// Inner (start, end-exclusive) char indices of the first balanced `(…)` group.
fn arg_list_bounds(chars: &[char]) -> Option<(usize, usize)> {
	balanced(chars, '(', ')', 0)
}

/// Extract the arrow return type `-> T` that sits between the argument list's
/// closing `)` and the body's opening `{`.
fn extract_arrow_return(chars: &[char]) -> Option<String> {
	let (_, paren_close) = balanced(chars, '(', ')', 0)?;
	let (body_inner, _) = balanced(chars, '{', '}', paren_close + 1)?;
	let between: String = chars[paren_close + 1..body_inner - 1].iter().collect();
	let arrow = between.find("->")?;
	non_empty(between[arrow + 2..].trim())
}

/// Find the first balanced `open`/`close` group at or after `from`, returning
/// the (inner_start, inner_end_exclusive) char indices. Quote-aware.
fn balanced(chars: &[char], open: char, close: char, from: usize) -> Option<(usize, usize)> {
	let n = chars.len();
	let mut i = from;
	let mut quote: Option<char> = None;
	// Locate the opening delimiter (outside quotes).
	while i < n {
		let c = chars[i];
		if let Some(q) = quote {
			if c == q {
				quote = None;
			}
		} else if c == '\'' || c == '"' {
			quote = Some(c);
		} else if c == open {
			break;
		}
		i += 1;
	}
	if i >= n {
		return None;
	}
	let inner_start = i + 1;
	let mut depth = 1;
	i = inner_start;
	quote = None;
	while i < n {
		let c = chars[i];
		if let Some(q) = quote {
			if c == q {
				quote = None;
			}
		} else if c == '\'' || c == '"' {
			quote = Some(c);
		} else if c == open {
			depth += 1;
		} else if c == close {
			depth -= 1;
			if depth == 0 {
				return Some((inner_start, i));
			}
		}
		i += 1;
	}
	None
}

fn parse_args(inner: &str) -> Vec<FnArg> {
	split_top_level(inner, ',')
		.into_iter()
		.filter(|p| !p.trim().is_empty())
		.filter_map(|part| parse_arg(&part))
		.collect()
}

fn parse_arg(part: &str) -> Option<FnArg> {
	let part = part.trim();
	let colon = part.find(':')?;
	let name = part[..colon].trim().trim_start_matches('$').to_string();
	// The type runs to a `=` default (if any), at the top level.
	let type_src =
		split_top_level(part[colon + 1..].trim(), '=').into_iter().next().unwrap_or_default();
	let ty = parse_type(type_src.trim());
	// `option<T>` arguments arrive as `none | T`; collapse into the flag.
	let (r#type, optional) = unwrap_optional(ty);
	Some(FnArg {
		name,
		r#type,
		optional,
	})
}

fn non_empty(s: &str) -> Option<String> {
	if s.is_empty() {
		None
	} else {
		Some(s.to_string())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::typegen::types::PrimitiveType;

	fn prim(p: PrimitiveType) -> FieldType {
		FieldType::Primitive {
			name: p,
		}
	}

	#[test]
	fn single_arg_no_returns() {
		let sig = parse_function("DEFINE FUNCTION fn::greet($name: string) { RETURN $name; }");
		assert_eq!(sig.name.as_deref(), Some("fn::greet"));
		assert_eq!(sig.args.len(), 1);
		assert_eq!(sig.args[0].name, "name");
		assert_eq!(sig.args[0].r#type, prim(PrimitiveType::String));
		assert!(!sig.args[0].optional);
		assert_eq!(sig.returns, None);
	}

	#[test]
	fn args_and_returns_with_optional() {
		// Matches what `INFO FOR DB` emits: `-> T` return type, `none | T` for
		// optional arguments, body with no trailing `;`, and a `PERMISSIONS`
		// clause after the body.
		let sig = parse_function(
			"DEFINE FUNCTION fn::calc($a: int, $b: none | bool) -> string { RETURN $a } PERMISSIONS FULL",
		);
		assert_eq!(sig.args.len(), 2);
		assert_eq!(sig.args[0].name, "a");
		assert_eq!(sig.args[0].r#type, prim(PrimitiveType::Int));
		assert_eq!(sig.args[1].name, "b");
		assert!(sig.args[1].optional);
		assert_eq!(sig.args[1].r#type, prim(PrimitiveType::Bool));
		assert_eq!(sig.returns, Some(prim(PrimitiveType::String)));
	}

	#[test]
	fn no_args() {
		let sig = parse_function("DEFINE FUNCTION fn::now() { RETURN time::now(); }");
		assert!(sig.args.is_empty());
		assert_eq!(sig.name.as_deref(), Some("fn::now"));
	}

	#[test]
	fn body_with_braces_and_parens_does_not_confuse_scan() {
		let sig = parse_function(
			"DEFINE FUNCTION fn::complex($x: int) -> object { LET $y = { a: ($x + 1) }; RETURN $y }",
		);
		assert_eq!(sig.args.len(), 1);
		assert_eq!(sig.args[0].name, "x");
		assert_eq!(sig.returns, Some(prim(PrimitiveType::Object)));
	}

	#[test]
	fn defaulted_arg_stops_type_at_equals() {
		let sig = parse_function("DEFINE FUNCTION fn::f($x: int = 5) { RETURN $x; }");
		assert_eq!(sig.args.len(), 1);
		assert_eq!(sig.args[0].r#type, prim(PrimitiveType::Int));
	}

	#[test]
	fn no_arrow_means_no_return_type() {
		let sig = parse_function("DEFINE FUNCTION fn::f() { RETURN 1 } PERMISSIONS FULL");
		assert_eq!(sig.returns, None);
	}
}
