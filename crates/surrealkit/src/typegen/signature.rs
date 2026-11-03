//! Pure parser for `DEFINE FUNCTION` signatures. No database or IO.
//!
//! Extracts the function name, its typed arguments, and an optional return
//! kind out of a statement such as:
//!
//! ```text
//! DEFINE FUNCTION fn::greet($name: string, $loud: none | bool) -> string { ... } PERMISSIONS FULL
//! ```
//!
//! SurrealDB emits the return kind with an arrow (`-> string`) between the
//! argument list and the body, and normalises `option<T>` arguments to
//! `none | T`. The return kind is parsed as a kind rather than taken as "the
//! text up to the first `{`", because an object-literal return kind
//! (`-> { a: int } { … }`) starts with a brace itself.

use super::scan::{TokenKind, tokenize};
use super::type_parser::{
	KindProblem, parse_type_checked, parse_type_prefix, split_top_level, unwrap_optional,
};
use super::types::{FieldType, FnArg};

/// Result of parsing a `DEFINE FUNCTION` statement.
pub struct FnSignature {
	/// The `fn::…` name, if found in the statement.
	pub name: Option<String>,
	pub args: Vec<FnArg>,
	pub returns: Option<FieldType>,
	/// Every kind that could not be parsed: the kind text, and the problem
	/// within it.
	pub(super) problems: Vec<(String, KindProblem)>,
}

pub fn parse_function(stmt: &str) -> FnSignature {
	let name = extract_fn_name(stmt);
	let mut problems = Vec::new();
	let Some((open, close)) = arg_list_bounds(stmt) else {
		return FnSignature {
			name,
			args: Vec::new(),
			returns: None,
			problems,
		};
	};
	let args = parse_args(&stmt[open + 1..close], &mut problems);
	let returns = extract_arrow_return(&stmt[close + 1..], &mut problems);
	FnSignature {
		name,
		args,
		returns,
		problems,
	}
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

/// Byte offsets of the argument list's `(` and its matching `)`. Strings,
/// quoted identifiers and nested brackets are skipped over correctly.
pub(super) fn arg_list_bounds(stmt: &str) -> Option<(usize, usize)> {
	let tokens = tokenize(stmt);
	let open = tokens.iter().position(|t| t.kind == TokenKind::Open && t.text(stmt) == "(")?;
	let mut depth = 0i32;
	for t in &tokens[open..] {
		match t.kind {
			TokenKind::Open => depth += 1,
			TokenKind::Close => {
				depth -= 1;
				if depth == 0 {
					return Some((tokens[open].start, t.start));
				}
			}
			_ => {}
		}
	}
	None
}

/// The `-> kind` that may follow the argument list.
fn extract_arrow_return(
	after_args: &str,
	problems: &mut Vec<(String, KindProblem)>,
) -> Option<FieldType> {
	let rest = after_args.trim_start().strip_prefix("->")?;
	let (kind, used, found) = parse_type_prefix(rest);
	let lead = rest.len() - rest.trim_start().len();
	// The kind's own text: what the parser consumed or, after a syntax error
	// that consumed nothing, everything up to the body.
	let end = if used == 0 {
		rest[lead..].find('{').map_or(rest.len(), |at| lead + at)
	} else {
		used
	};
	let text = rest[lead..end].trim_end();
	if text.is_empty() {
		return None;
	}
	for problem in found {
		// Re-base the span onto `text`, keeping it inside it.
		let start = problem.span.start.clamp(lead, lead + text.len()) - lead;
		let stop = problem.span.end.clamp(lead, lead + text.len()) - lead;
		let span = if stop > start {
			start..stop
		} else {
			0..text.len()
		};
		problems.push((
			text.to_string(),
			KindProblem {
				span,
				reason: problem.reason,
			},
		));
	}
	Some(kind)
}

fn parse_args(inner: &str, problems: &mut Vec<(String, KindProblem)>) -> Vec<FnArg> {
	split_top_level(inner, ',')
		.into_iter()
		.filter(|p| !p.trim().is_empty())
		.filter_map(|part| parse_arg(&part, problems))
		.collect()
}

fn parse_arg(part: &str, problems: &mut Vec<(String, KindProblem)>) -> Option<FnArg> {
	let part = part.trim();
	let colon = part.find(':')?;
	let name = part[..colon].trim().trim_start_matches('$').to_string();
	// The kind runs to a `=` default (if any), at the top level.
	let type_src =
		split_top_level(part[colon + 1..].trim(), '=').into_iter().next().unwrap_or_default();
	let (ty, found) = parse_type_checked(type_src.trim());
	problems.extend(found.into_iter().map(|p| (type_src.trim().to_string(), p)));
	// `option<T>` arguments arrive as `none | T`; collapse into the flag.
	let (r#type, optional) = unwrap_optional(ty);
	Some(FnArg {
		name,
		r#type,
		optional,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::typegen::types::{ObjectField, PrimitiveType};

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
		assert!(sig.problems.is_empty());
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

	#[test]
	fn an_object_literal_return_kind_is_not_the_body() {
		let sig = parse_function(
			"DEFINE FUNCTION fn::pair($a: { x: int, y: int }) -> { sum: int } { RETURN { sum: $a.x + $a.y } }",
		);
		assert_eq!(sig.args.len(), 1);
		assert!(matches!(&sig.args[0].r#type, FieldType::Object { fields } if fields.len() == 2));
		assert_eq!(
			sig.returns,
			Some(FieldType::Object {
				fields: vec![ObjectField {
					name: "sum".into(),
					r#type: prim(PrimitiveType::Int)
				}]
			})
		);
		assert!(sig.problems.is_empty());
	}

	#[test]
	fn escaped_quotes_in_the_body_do_not_break_the_scan() {
		let sig = parse_function(
			r#"DEFINE FUNCTION fn::q($s: string) -> string { RETURN "a \") b" + 'it\'s (' } COMMENT "x(\"" PERMISSIONS FULL"#,
		);
		assert_eq!(sig.args.len(), 1);
		assert_eq!(sig.returns, Some(prim(PrimitiveType::String)));
	}

	#[test]
	fn nested_names_and_collection_kinds() {
		let sig = parse_function(
			"DEFINE FUNCTION fn::doc::related($d: record<doc>, $tags: set<string>) -> array<record<doc>> { RETURN [$d] } PERMISSIONS NONE",
		);
		assert_eq!(sig.name.as_deref(), Some("fn::doc::related"));
		assert_eq!(sig.args.len(), 2);
		assert!(matches!(sig.returns, Some(FieldType::Array { .. })));
		assert!(sig.problems.is_empty());
	}

	#[test]
	fn unparseable_kinds_are_reported() {
		let sig =
			parse_function("DEFINE FUNCTION fn::f($a: frob, $b: int) -> range<int> { RETURN 1 }");
		assert_eq!(sig.args.len(), 2);
		assert!(matches!(sig.args[0].r#type, FieldType::Unknown { .. }));
		assert!(matches!(sig.returns, Some(FieldType::Unknown { .. })));
		let texts: Vec<&str> = sig.problems.iter().map(|(text, _)| text.as_str()).collect();
		assert_eq!(texts.len(), 2, "{texts:?}");
		assert_eq!(texts, vec!["frob", "range<int>"]);
		for (text, problem) in &sig.problems {
			assert_eq!(&text[problem.span.clone()], *text, "the whole name is at fault");
		}
	}
}
