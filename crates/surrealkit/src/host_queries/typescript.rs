//! Query extraction over the TypeScript and TSX grammars.

use std::ops::Range;

use tree_sitter::{Node, Parser, Tree};

use super::literal::{Quoting, decode};
use super::{HOST_PARAM_PREFIX, HostQuery, Segment, Substitution, line_and_column};

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

/// Finds every query call in `source[content]`, a whole file or one script region of it.
///
/// Offsets and line numbers in the results refer to the whole `source`.
pub(super) fn extract(
	source: &str,
	content: Range<usize>,
	tsx: bool,
	methods: &[&str],
) -> Vec<HostQuery> {
	let Some(tree) = parse(source, &content, tsx) else {
		return Vec::new();
	};

	let mut queries = Vec::new();
	let mut cursor = tree.walk();
	loop {
		let node = cursor.node();
		if node.kind() == "call_expression"
			&& let Some(query) = query_of(node, source, content.start, methods)
		{
			queries.push(query);
		}
		if cursor.goto_first_child() {
			continue;
		}
		while !cursor.goto_next_sibling() {
			if !cursor.goto_parent() {
				return queries;
			}
		}
	}
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

/// The query a `receiver.method(literal, …)` call carries, if `method` is one of `methods` and
/// the first argument is a string or template literal. `base` is the offset of the parsed region
/// in `source`.
fn query_of(call: Node<'_>, source: &str, base: usize, methods: &[&str]) -> Option<HostQuery> {
	let function = call.child_by_field_name("function")?;
	if function.kind() != "member_expression" {
		return None;
	}
	let method = function.child_by_field_name("property")?;
	let method = &source[shifted(method, base)];
	if !methods.contains(&method) {
		return None;
	}
	let arguments = call.child_by_field_name("arguments")?;
	if arguments.kind() != "arguments" {
		return None;
	}
	let mut walker = arguments.walk();
	let first = arguments.named_children(&mut walker).find(|child| child.kind() != "comment")?;
	let literal = shifted(first, base);
	match first.kind() {
		"string" => string_query(source, literal, method),
		"template_string" => template_query(source, literal, first, base, method),
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

fn string_query(source: &str, literal: Range<usize>, method: &str) -> Option<HostQuery> {
	let host_range = contents(source, &literal, b"\"'")?;
	let mut text = String::new();
	let mut segments = Vec::new();
	decode(source, host_range.clone(), Quoting::String, &mut text, &mut segments)?;
	Some(host_query(source, text, host_range, method, Vec::new(), segments))
}

/// Builds the query of a template literal: each chunk decodes to its runtime value and every
/// `${…}` becomes a `$__hostN` parameter, which is a bound value in almost every position.
fn template_query(
	source: &str,
	literal: Range<usize>,
	node: Node<'_>,
	base: usize,
	method: &str,
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
	Some(host_query(source, text, host_range, method, substitutions, segments))
}

fn host_query(
	source: &str,
	text: String,
	host_range: Range<usize>,
	method: &str,
	substitutions: Vec<Substitution>,
	segments: Vec<Segment>,
) -> HostQuery {
	let (line, column) = line_and_column(source, host_range.start);
	HostQuery {
		text,
		host_range,
		line,
		column,
		method: method.to_string(),
		substitutions,
		segments,
	}
}
