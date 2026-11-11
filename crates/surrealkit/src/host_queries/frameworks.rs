//! Finds the script regions of Svelte, Vue and Astro files.

use std::ops::Range;

/// The contents of one `<script>` block.
pub(super) struct ScriptBlock {
	/// Byte range of the block's contents in the whole file.
	pub content: Range<usize>,
	/// Whether the block is JSX-capable (`lang="tsx"` or `lang="jsx"`).
	pub tsx: bool,
}

/// The `type` values of a `<script>` that holds JavaScript or TypeScript.
const SCRIPT_TYPES: &[&str] = &[
	"",
	"module",
	"text/javascript",
	"application/javascript",
	"text/ecmascript",
	"application/ecmascript",
	"text/typescript",
	"application/typescript",
	"text/jsx",
	"text/tsx",
	"js",
	"javascript",
	"ts",
	"typescript",
	"jsx",
	"tsx",
];

/// Every `<script …>…</script>` block of `source` that holds JavaScript or TypeScript, in order.
///
/// The tag name is matched case-insensitively. Blocks whose `type` names something else (JSON
/// data, an importmap, a text template) and self-closing tags are skipped.
pub(super) fn script_blocks(source: &str) -> Vec<ScriptBlock> {
	// ASCII lowercasing keeps every byte offset, so offsets found here index `source`.
	let lower = source.to_ascii_lowercase();
	let mut blocks = Vec::new();
	let mut from = 0;
	while let Some(found) = lower[from..].find("<script") {
		let name_end = from + found + "<script".len();
		from = name_end;
		let delimited = lower
			.as_bytes()
			.get(name_end)
			.is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'>' | b'/'));
		if !delimited {
			continue;
		}
		let Some(tag_end) = tag_end(source, name_end) else {
			break;
		};
		let attributes = &source[name_end..tag_end];
		let content_start = tag_end + 1;
		from = content_start;
		if attributes.trim_end().ends_with('/') {
			continue;
		}
		let Some(close) = lower[content_start..].find("</script") else {
			break;
		};
		let content = content_start..content_start + close;
		from = content.end;
		if let Some(tsx) = script_language(attributes) {
			blocks.push(ScriptBlock {
				content,
				tsx,
			});
		}
	}
	blocks
}

/// The frontmatter of an Astro file: the contents between the leading `---` fences, or `None` when
/// the file has no closed frontmatter.
pub(super) fn astro_frontmatter(source: &str) -> Option<Range<usize>> {
	let lead = source.len()
		- source.trim_start_matches(|c: char| c.is_whitespace() || c == '\u{feff}').len();
	let after_fence = source.len() - source[lead..].strip_prefix("---")?.len();
	let opening_end =
		source[after_fence..].find('\n').map_or(source.len(), |at| after_fence + at + 1);
	if !source[after_fence..opening_end].trim().is_empty() {
		return None;
	}
	let mut line_start = opening_end;
	for line in source[opening_end..].split_inclusive('\n') {
		if line.trim() == "---" {
			return Some(opening_end..line_start);
		}
		line_start += line.len();
	}
	None
}

/// The offset of the `>` closing the tag whose attributes start at `from`, skipping `>` inside
/// quoted attribute values.
fn tag_end(source: &str, from: usize) -> Option<usize> {
	let mut quote = None;
	for (at, byte) in source.bytes().enumerate().skip(from) {
		match (quote, byte) {
			(Some(open), byte) if byte == open => quote = None,
			(Some(_), _) => {}
			(None, b'"' | b'\'') => quote = Some(byte),
			(None, b'>') => return Some(at),
			(None, _) => {}
		}
	}
	None
}

/// Whether a `<script>` with these attributes holds JavaScript or TypeScript, and if so whether it
/// needs the JSX grammar. `None` skips the block.
fn script_language(attributes: &str) -> Option<bool> {
	let mut tsx = false;
	let mut rest = attributes.trim_start_matches(|c: char| c.is_whitespace() || c == '/');
	while !rest.is_empty() {
		let before = rest.len();
		let name_len = rest
			.find(|c: char| c.is_whitespace() || matches!(c, '=' | '/' | '>'))
			.unwrap_or(rest.len());
		let name = rest[..name_len].to_ascii_lowercase();
		rest = rest[name_len..].trim_start();
		let mut value = String::new();
		if let Some(after_equals) = rest.strip_prefix('=') {
			let after_equals = after_equals.trim_start();
			let (raw, remainder) = match after_equals.chars().next() {
				Some(quote @ ('"' | '\'')) => {
					let inner = &after_equals[1..];
					let close = inner.find(quote).unwrap_or(inner.len());
					(&inner[..close], inner.get(close + 1..).unwrap_or(""))
				}
				_ => {
					let end = after_equals
						.find(|c: char| c.is_whitespace() || c == '>')
						.unwrap_or(after_equals.len());
					(&after_equals[..end], &after_equals[end..])
				}
			};
			value = raw.trim().to_ascii_lowercase();
			rest = remainder;
		}
		rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == '/');
		match name.as_str() {
			"type" if !SCRIPT_TYPES.contains(&value.as_str()) => return None,
			"lang" => tsx = matches!(value.as_str(), "tsx" | "jsx"),
			_ => {}
		}
		if rest.len() == before {
			// A stray character neither scan consumed; step over it.
			rest = rest.get(1..).unwrap_or("");
		}
	}
	Some(tsx)
}
