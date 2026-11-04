//! Reads the templates of Svelte, Vue and Astro files for queries in component attributes.
//!
//! None of the three has a grammar here, so this is a scanner for the one construct it needs: an
//! opening tag `<Name …>` and its attributes. It reads the template only, never the `<script>` and
//! `<style>` elements, HTML comments, or an Astro file's frontmatter, and it parses every tag it
//! meets, not only the ones that match, so a `>` or a `<Query` inside another tag's quoted value
//! or brace expression is never mistaken for markup.
//!
//! The forms of the query attribute (`q` below) are:
//!
//! | host   | form              | meaning                                                         |
//! |--------|-------------------|-----------------------------------------------------------------|
//! | Svelte | `q="…"`, `q='…'`  | text with HTML references; each `{…}` is a substitution         |
//! | Svelte | `q={…}`           | a JavaScript expression                                         |
//! | Vue    | `q="…"`           | static text with HTML references                                |
//! | Vue    | `:q="…"`          | a JavaScript expression (also `v-bind:q`)                       |
//! | Astro  | `q="…"`           | text with HTML references                                       |
//! | Astro  | `q={…}`           | a JavaScript expression                                         |
//! | Astro  | `` q=`…` ``       | a template literal                                              |
//!
//! A JavaScript expression is a query only when it is exactly one string or template literal. A
//! variable, a call or a concatenation is not.

use std::ops::Range;

use super::literal::decode_entities;
use super::{
	HOST_PARAM_PREFIX, HostQuery, HostQuerySource, Queries, Segment, Substitution, frameworks,
	typescript,
};

/// The template syntax of a host file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Flavor {
	Svelte,
	Vue,
	Astro,
}

impl Flavor {
	/// Whether `{…}` is a JavaScript expression in the template: Vue spells it `{{ … }}` in text
	/// and has none in attribute values.
	fn has_braces(self) -> bool {
		self != Self::Vue
	}
}

/// What reading one template found.
pub(super) struct Scan {
	/// The queries in component attributes, in document order.
	pub queries: Vec<HostQuery>,
	/// The contents (without the braces) of the `{…}` expressions the template holds, in text and
	/// in attribute values. Nested ones are listed as well as the one around them.
	pub expressions: Vec<Range<usize>>,
}

/// How an attribute's value is written.
enum Value {
	/// `"…"` or `'…'`: the range is the contents.
	Quoted(Range<usize>),
	/// `{…}`: the range is the contents.
	Braced(Range<usize>),
	/// A template literal, backticks included.
	Template(Range<usize>),
	/// A bare word.
	Unquoted,
}

struct Attribute {
	name: Range<usize>,
	value: Option<Value>,
}

struct Tag {
	attributes: Vec<Attribute>,
	/// The offset of the closing `>`.
	end: usize,
}

/// Reads the template of `source` for the component attributes `queries` names.
pub(super) fn scan(flavor: Flavor, source: &str, queries: &Queries<'_>) -> Scan {
	let bytes = source.as_bytes();
	// ASCII lowercasing keeps every byte offset.
	let lower = source.to_ascii_lowercase();
	let mut scan = Scan {
		queries: Vec::new(),
		expressions: Vec::new(),
	};
	let mut at = match flavor {
		Flavor::Astro => frameworks::astro_frontmatter(source).map_or(0, |content| content.end),
		_ => 0,
	};
	while let Some(found) =
		bytes[at..].iter().position(|byte| *byte == b'<' || (*byte == b'{' && flavor.has_braces()))
	{
		at += found;
		if bytes[at] == b'{' {
			// Only recorded: markup can nest inside an Astro expression, so the scan goes on
			// into it.
			if let Some(close) = braced_end(source, at, source.len()) {
				scan.expressions.push(at + 1..close);
			}
			at += 1;
			continue;
		}
		if source[at..].starts_with("<!--") {
			at = source[at + 4..].find("-->").map_or(source.len(), |close| at + 4 + close + 3);
			continue;
		}
		let name_start = at + 1;
		if !bytes.get(name_start).is_some_and(u8::is_ascii_alphabetic) {
			at += 1;
			continue;
		}
		let name_end = name_start
			+ bytes[name_start..]
				.iter()
				.position(|byte| is_name_end(*byte))
				.unwrap_or(bytes.len() - name_start);
		let delimited = bytes
			.get(name_end)
			.is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'/' | b'>'));
		let tag = delimited.then(|| parse_tag(flavor, source, name_end, &mut scan.expressions));
		let Some(Some(tag)) = tag else {
			at += 1;
			continue;
		};
		let name = &source[name_start..name_end];
		at = tag.end + 1;

		let raw_text = ["script", "style"].into_iter().find(|raw| name.eq_ignore_ascii_case(raw));
		if let Some(raw) = raw_text {
			// A block's contents are not markup. A self-closing tag has none.
			if bytes[tag.end - 1] != b'/' {
				at = lower[at..]
					.find(&format!("</{raw}"))
					.map_or(source.len(), |at_close| at + at_close);
			}
			continue;
		}

		// A member name such as `Kit.Query` matches on its last segment.
		let component = name.rsplit('.').next().unwrap_or(name);
		if !queries.components.contains(&component) {
			continue;
		}
		let origin = HostQuerySource::Attribute {
			component: component.to_string(),
			attribute: queries.attribute.to_string(),
		};
		for attribute in &tag.attributes {
			let query = attribute_query(flavor, source, attribute, queries.attribute, &origin);
			scan.queries.extend(query);
		}
	}
	scan
}

/// The query an attribute of a matching component carries, if it is the query attribute in one of
/// the forms its host reads as text or as a literal.
fn attribute_query(
	flavor: Flavor,
	source: &str,
	attribute: &Attribute,
	wanted: &str,
	origin: &HostQuerySource,
) -> Option<HostQuery> {
	let name = &source[attribute.name.clone()];
	let value = attribute.value.as_ref()?;
	let origin = origin.clone();
	if name == wanted {
		return match (flavor, value) {
			(Flavor::Svelte, Value::Quoted(inner)) => interpolated(source, inner.clone(), origin),
			(_, Value::Quoted(inner)) => Some(text(source, inner.clone(), origin)),
			(Flavor::Svelte | Flavor::Astro, Value::Braced(inner)) => {
				typescript::expression_query(source, inner.clone(), origin)
			}
			(Flavor::Astro, Value::Template(literal)) => {
				typescript::expression_query(source, literal.clone(), origin)
			}
			_ => None,
		};
	}
	let bound = name.strip_prefix("v-bind:").or_else(|| name.strip_prefix(':'));
	match (flavor, value) {
		(Flavor::Vue, Value::Quoted(inner)) if bound == Some(wanted) => {
			typescript::expression_query(source, inner.clone(), origin)
		}
		_ => None,
	}
}

/// A plain-text attribute value: its HTML character references decoded, nothing else.
fn text(source: &str, inner: Range<usize>, origin: HostQuerySource) -> HostQuery {
	let mut text = String::new();
	let mut segments = Vec::new();
	decode_entities(source, inner.clone(), &mut text, &mut segments);
	HostQuery::new(source, text, inner, origin, Vec::new(), segments)
}

/// A Svelte quoted attribute value: text with HTML character references, where each `{…}` is an
/// interpolated expression and becomes a `$__hostN` parameter.
fn interpolated(source: &str, inner: Range<usize>, origin: HostQuerySource) -> Option<HostQuery> {
	let mut text = String::new();
	let mut segments: Vec<Segment> = Vec::new();
	let mut substitutions = Vec::new();
	let mut cursor = inner.start;
	let mut at = inner.start;
	while let Some(found) = source[at..inner.end].find('{') {
		let open = at + found;
		let close = braced_end(source, open, inner.end)?;
		decode_entities(source, cursor..open, &mut text, &mut segments);
		let param = format!("{HOST_PARAM_PREFIX}{}", substitutions.len());
		let text_start = text.len();
		text.push('$');
		text.push_str(&param);
		substitutions.push(Substitution {
			param,
			text_range: text_start..text.len(),
			host_range: open..close + 1,
		});
		cursor = close + 1;
		at = cursor;
	}
	decode_entities(source, cursor..inner.end, &mut text, &mut segments);
	Some(HostQuery::new(source, text, inner, origin, substitutions, segments))
}

/// Whether `byte` ends a tag or attribute name.
fn is_name_end(byte: u8) -> bool {
	byte.is_ascii_whitespace() || matches!(byte, b'/' | b'>' | b'=' | b'"' | b'\'' | b'{' | b'<')
}

fn skip_whitespace(bytes: &[u8], mut at: usize) -> usize {
	while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
		at += 1;
	}
	at
}

/// Parses the attributes of the opening tag whose name ends at `from`, up to its `>`. `None` when
/// a quote or a brace is never closed.
fn parse_tag(
	flavor: Flavor,
	source: &str,
	from: usize,
	expressions: &mut Vec<Range<usize>>,
) -> Option<Tag> {
	let bytes = source.as_bytes();
	let mut attributes = Vec::new();
	let mut at = from;
	loop {
		at = skip_whitespace(bytes, at);
		match *bytes.get(at)? {
			b'>' => {
				return Some(Tag {
					attributes,
					end: at,
				});
			}
			b'/' => {
				at += 1;
				continue;
			}
			// A shorthand or spread attribute: `{q}`, `{...props}`.
			b'{' if flavor.has_braces() => {
				let close = braced_end(source, at, source.len())?;
				expressions.push(at + 1..close);
				at = close + 1;
				continue;
			}
			_ => {}
		}
		let name_start = at;
		while bytes.get(at).is_some_and(|byte| !is_name_end(*byte)) {
			at += 1;
		}
		if at == name_start {
			// A stray quote or brace.
			at += 1;
			continue;
		}
		let name = name_start..at;
		let equals = skip_whitespace(bytes, at);
		if bytes.get(equals) != Some(&b'=') {
			attributes.push(Attribute {
				name,
				value: None,
			});
			continue;
		}
		let (value, next) =
			parse_value(flavor, source, skip_whitespace(bytes, equals + 1), expressions)?;
		attributes.push(Attribute {
			name,
			value: Some(value),
		});
		at = next;
	}
}

/// Parses the attribute value at `at`, returning it and the offset after it.
fn parse_value(
	flavor: Flavor,
	source: &str,
	at: usize,
	expressions: &mut Vec<Range<usize>>,
) -> Option<(Value, usize)> {
	let bytes = source.as_bytes();
	match *bytes.get(at)? {
		quote @ (b'"' | b'\'') => {
			let end = quote_end(flavor, source, at, quote, expressions)?;
			Some((Value::Quoted(at + 1..end), end + 1))
		}
		b'{' if flavor.has_braces() => {
			let close = braced_end(source, at, source.len())?;
			expressions.push(at + 1..close);
			Some((Value::Braced(at + 1..close), close + 1))
		}
		b'`' if flavor == Flavor::Astro => {
			let end = template_end(source, at, source.len())?;
			Some((Value::Template(at..end + 1), end + 1))
		}
		_ => {
			let len = bytes[at..]
				.iter()
				.position(|byte| byte.is_ascii_whitespace() || *byte == b'>')
				.unwrap_or(bytes.len() - at);
			Some((Value::Unquoted, at + len))
		}
	}
}

/// The offset of the quote closing the value opened at `open`. In Svelte a `{…}` inside the value
/// is an expression, so a quote inside one does not close it.
fn quote_end(
	flavor: Flavor,
	source: &str,
	open: usize,
	quote: u8,
	expressions: &mut Vec<Range<usize>>,
) -> Option<usize> {
	let bytes = source.as_bytes();
	let mut at = open + 1;
	while let Some(&byte) = bytes.get(at) {
		if byte == quote {
			return Some(at);
		}
		if byte == b'{' && flavor == Flavor::Svelte {
			let close = braced_end(source, at, source.len())?;
			expressions.push(at + 1..close);
			at = close;
		}
		at += 1;
	}
	None
}

/// The offset of the `}` matching the `{` at `open`, looking at `source[..limit]`. Strings,
/// template literals and comments inside are skipped, so a brace or a quote in one does not count.
fn braced_end(source: &str, open: usize, limit: usize) -> Option<usize> {
	let bytes = source.as_bytes();
	let mut depth = 0usize;
	let mut at = open;
	while at < limit {
		match bytes[at] {
			b'{' => depth += 1,
			b'}' => {
				depth -= 1;
				if depth == 0 {
					return Some(at);
				}
			}
			b'"' | b'\'' => at = string_end(bytes, at, limit)?,
			b'`' => at = template_end(source, at, limit)?,
			b'/' if bytes.get(at + 1) == Some(&b'/') => {
				at += bytes[at..limit].iter().position(|byte| *byte == b'\n').unwrap_or(limit - at);
			}
			b'/' if bytes.get(at + 1) == Some(&b'*') => {
				at += source[at + 2..limit].find("*/")? + 3;
			}
			_ => {}
		}
		at += 1;
	}
	None
}

/// The offset of the quote closing the string opened at `open`.
fn string_end(bytes: &[u8], open: usize, limit: usize) -> Option<usize> {
	let mut at = open + 1;
	while at < limit {
		match bytes[at] {
			b'\\' => at += 1,
			byte if byte == bytes[open] => return Some(at),
			_ => {}
		}
		at += 1;
	}
	None
}

/// The offset of the backtick closing the template literal opened at `open`.
fn template_end(source: &str, open: usize, limit: usize) -> Option<usize> {
	let bytes = source.as_bytes();
	let mut at = open + 1;
	while at < limit {
		match bytes[at] {
			b'\\' => at += 1,
			b'`' => return Some(at),
			b'$' if bytes.get(at + 1) == Some(&b'{') => {
				at = braced_end(source, at + 1, limit)?;
			}
			_ => {}
		}
		at += 1;
	}
	None
}
