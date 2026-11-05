//! Query extraction over the TypeScript and TSX grammars.

use std::ops::Range;

use tree_sitter::{Node, Parser, Tree};

use super::literal::{Quoting, decode, decode_entities};
use super::{HOST_PARAM_PREFIX, HostQuery, HostQuerySource, Queries, Substitution};

/// Parses `source[content]`, a whole file or one script region of it. `tsx` selects the TSX
/// grammar, needed for files with JSX.
fn parse(source: &str, content: &Range<usize>, tsx: bool) -> Option<Tree> {
	let language = if tsx {
		tree_sitter_typescript::LANGUAGE_TSX
	} else {
		tree_sitter_typescript::LANGUAGE_TYPESCRIPT
	};
	let mut parser = Parser::new();
	parser.set_language(&language.into()).ok()?;
	parser.parse(&source[content.clone()], None)
}

/// Finds every query call and component attribute in `source[content]`, a whole file or one
/// script region of it.
///
/// Offsets and line numbers in the results refer to the whole `source`.
pub(super) fn extract(
	source: &str,
	content: Range<usize>,
	tsx: bool,
	queries: &Queries<'_>,
) -> Vec<HostQuery> {
	let Some(tree) = parse(source, &content, tsx) else {
		return Vec::new();
	};

	let base = content.start;
	let mut found = Vec::new();
	let mut cursor = tree.walk();
	loop {
		let node = cursor.node();
		match node.kind() {
			"call_expression" => found.extend(query_of(node, source, base, queries)),
			"jsx_opening_element" | "jsx_self_closing_element" => {
				found.extend(attribute_queries(node, source, base, queries));
			}
			_ => {}
		}
		if cursor.goto_first_child() {
			continue;
		}
		while !cursor.goto_next_sibling() {
			if !cursor.goto_parent() {
				return found;
			}
		}
	}
}

/// The query `source[expression]` is, when it is exactly one string or template literal: the
/// contents of a `{…}` attribute value or of a Vue `:q="…"`. `origin` says where it was found.
pub(super) fn expression_query(
	source: &str,
	expression: Range<usize>,
	origin: HostQuerySource,
) -> Option<HostQuery> {
	let tree = parse(source, &expression, false)?;
	let root = tree.root_node();
	let mut walker = root.walk();
	let mut statements = root.named_children(&mut walker).filter(|node| node.kind() != "comment");
	let statement = statements.next()?;
	if statements.next().is_some() || statement.kind() != "expression_statement" {
		return None;
	}
	let mut inner = statement.walk();
	let mut parts = statement.named_children(&mut inner).filter(|node| node.kind() != "comment");
	let literal = parts.next()?;
	if parts.next().is_some() {
		return None;
	}
	literal_query(literal, source, expression.start, origin)
}

/// The byte ranges of every comment in `source[content]`, in the whole file's offsets.
pub(super) fn comments(source: &str, content: Range<usize>, tsx: bool) -> Vec<Range<usize>> {
	let Some(tree) = parse(source, &content, tsx) else {
		return Vec::new();
	};

	let mut found = Vec::new();
	let mut cursor = tree.walk();
	loop {
		let node = cursor.node();
		if node.kind() == "comment" {
			found.push(shifted(node, content.start));
		}
		if cursor.goto_first_child() {
			continue;
		}
		while !cursor.goto_next_sibling() {
			if !cursor.goto_parent() {
				return found;
			}
		}
	}
}

/// The query a call carries, if it is `receiver.method(literal, …)` with a name in `methods`, or
/// `function(…)` or `receiver.function(…)` with a name in `functions`, and the argument at that
/// function's `arg` (a method's is the first) is a string or template literal. Comments between
/// arguments are not arguments. `base` is the offset of the parsed region in `source`.
fn query_of(call: Node<'_>, source: &str, base: usize, queries: &Queries<'_>) -> Option<HostQuery> {
	let function = call.child_by_field_name("function")?;
	let (name, is_member) = match function.kind() {
		"member_expression" => (function.child_by_field_name("property")?, true),
		"identifier" => (function, false),
		_ => return None,
	};
	let name = &source[shifted(name, base)];
	let (origin, arg) = if is_member && queries.methods.contains(&name) {
		(HostQuerySource::Method(name.to_string()), 0)
	} else if let Some(function) = queries.functions.iter().find(|function| function.name == name) {
		(HostQuerySource::Function(name.to_string()), function.arg)
	} else {
		return None;
	};
	let arguments = call.child_by_field_name("arguments")?;
	if arguments.kind() != "arguments" {
		return None;
	}
	let mut walker = arguments.walk();
	let argument =
		arguments.named_children(&mut walker).filter(|child| child.kind() != "comment").nth(arg)?;
	literal_query(argument, source, base, origin)
}

/// The queries a JSX element carries in its query attribute, if its name is one of the
/// components: `<Query q="…" />`, `q={"…"}` or `` q={`…`} ``. A member name such as `<Kit.Query>`
/// matches on its last segment.
fn attribute_queries(
	element: Node<'_>,
	source: &str,
	base: usize,
	queries: &Queries<'_>,
) -> Option<HostQuery> {
	let name = &source[shifted(element.child_by_field_name("name")?, base)];
	let component = name.rsplit('.').next().unwrap_or(name);
	if !queries.components.contains(&component) {
		return None;
	}
	let origin = HostQuerySource::Attribute {
		component: component.to_string(),
		attribute: queries.attribute.to_string(),
	};
	let mut walker = element.walk();
	let attribute = element.named_children(&mut walker).find(|child| {
		child.kind() == "jsx_attribute"
			&& child.named_child(0).is_some_and(|attribute_name| {
				&source[shifted(attribute_name, base)] == queries.attribute
			})
	})?;
	let mut walker = attribute.walk();
	let value =
		attribute.named_children(&mut walker).filter(|child| child.kind() != "comment").nth(1)?;
	match value.kind() {
		// A JSX attribute string has no backslash escapes, only HTML character references.
		"string" => {
			let host_range = contents(source, &shifted(value, base), b"\"'")?;
			let mut text = String::new();
			let mut segments = Vec::new();
			decode_entities(source, host_range.clone(), &mut text, &mut segments);
			Some(HostQuery::new(source, text, host_range, origin, Vec::new(), segments))
		}
		"jsx_expression" => {
			let mut walker = value.walk();
			let mut parts =
				value.named_children(&mut walker).filter(|child| child.kind() != "comment");
			let literal = parts.next()?;
			if parts.next().is_some() {
				return None;
			}
			literal_query(literal, source, base, origin)
		}
		_ => None,
	}
}

/// The query a string or template literal node is. `base` is the offset of the parsed region in
/// `source`.
fn literal_query(
	node: Node<'_>,
	source: &str,
	base: usize,
	origin: HostQuerySource,
) -> Option<HostQuery> {
	let literal = shifted(node, base);
	match node.kind() {
		"string" => string_query(source, literal, origin),
		"template_string" => template_query(source, literal, node, base, origin),
		_ => None,
	}
}

fn shifted(node: Node<'_>, base: usize) -> Range<usize> {
	node.start_byte() + base..node.end_byte() + base
}

/// The contents range of the literal spanning `literal`, when one of `delimiters` opens and closes
/// it.
fn contents(source: &str, literal: &Range<usize>, delimiters: &[u8]) -> Option<Range<usize>> {
	let bytes = source.as_bytes();
	let open = *bytes.get(literal.start)?;
	let close = *bytes.get(literal.end.checked_sub(1)?)?;
	(literal.len() >= 2 && open == close && delimiters.contains(&open))
		.then(|| literal.start + 1..literal.end - 1)
}

fn string_query(source: &str, literal: Range<usize>, origin: HostQuerySource) -> Option<HostQuery> {
	let host_range = contents(source, &literal, b"\"'")?;
	let mut text = String::new();
	let mut segments = Vec::new();
	decode(source, host_range.clone(), Quoting::String, &mut text, &mut segments)?;
	Some(HostQuery::new(source, text, host_range, origin, Vec::new(), segments))
}

/// Builds the query of a template literal: each chunk decodes to its runtime value and every
/// `${…}` becomes a `$__hostN` parameter, which is a bound value in almost every position.
fn template_query(
	source: &str,
	literal: Range<usize>,
	node: Node<'_>,
	base: usize,
	origin: HostQuerySource,
) -> Option<HostQuery> {
	let host_range = contents(source, &literal, b"`")?;
	let mut text = String::new();
	let mut segments = Vec::new();
	let mut substitutions = Vec::new();
	let mut cursor = host_range.start;
	let mut walker = node.walk();
	for hole in node.children(&mut walker).filter(|child| child.kind() == "template_substitution") {
		let hole = shifted(hole, base);
		decode(source, cursor..hole.start, Quoting::Template, &mut text, &mut segments)?;
		let param = format!("{HOST_PARAM_PREFIX}{}", substitutions.len());
		let text_start = text.len();
		text.push('$');
		text.push_str(&param);
		substitutions.push(Substitution {
			param,
			text_range: text_start..text.len(),
			host_range: hole.clone(),
		});
		cursor = hole.end;
	}
	decode(source, cursor..host_range.end, Quoting::Template, &mut text, &mut segments)?;
	Some(HostQuery::new(source, text, host_range, origin, substitutions, segments))
}
