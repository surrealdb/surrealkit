//! Decodes the contents of a JavaScript string or template literal to its runtime value.

use std::ops::Range;

use super::Segment;

/// Which kind of literal is being decoded; they differ in line endings and legacy escapes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Quoting {
	/// A `"…"` or `'…'` string.
	String,
	/// A template literal's chunk between substitutions.
	Template,
}

/// Appends the runtime value of `source[range]` to `text` and its span map to `segments`.
///
/// Copied runs map byte for byte; an escape sequence or a normalised line ending is one segment
/// covering the whole host sequence. `None` when the range holds an escape with no representable
/// value (a malformed sequence, or a lone surrogate), so the literal has no usable runtime string.
pub(super) fn decode(
	source: &str,
	range: Range<usize>,
	quoting: Quoting,
	text: &mut String,
	segments: &mut Vec<Segment>,
) -> Option<()> {
	let mut at = range.start;
	while at < range.end {
		let rest = &source[at..range.end];
		let run = rest
			.find(|c| c == '\\' || (c == '\r' && quoting == Quoting::Template))
			.unwrap_or(rest.len());
		if run > 0 {
			segments.push(Segment {
				text_start: text.len(),
				text_len: run,
				host_start: at,
				host_len: run,
			});
			text.push_str(&rest[..run]);
			at += run;
			continue;
		}

		let (decoded, consumed) = if rest.starts_with('\r') {
			// JavaScript normalises CRLF and CR to LF inside a template.
			(
				Some('\n'),
				if rest.starts_with("\r\n") {
					2
				} else {
					1
				},
			)
		} else {
			escape(rest, quoting)?
		};
		if let Some(decoded) = decoded {
			segments.push(Segment {
				text_start: text.len(),
				text_len: decoded.len_utf8(),
				host_start: at,
				host_len: consumed,
			});
			text.push(decoded);
		}
		at += consumed;
	}
	Some(())
}

/// Decodes the escape sequence at the start of `rest` (which begins with a backslash).
///
/// Returns the decoded char, `None` inside the `Some` for a line continuation, and the number of
/// host bytes consumed.
fn escape(rest: &str, quoting: Quoting) -> Option<(Option<char>, usize)> {
	let c = rest[1..].chars().next()?;
	let after = 1 + c.len_utf8();
	let tail = &rest[after..];
	let one = |decoded: char| Some((Some(decoded), after));
	match c {
		'n' => one('\n'),
		't' => one('\t'),
		'r' => one('\r'),
		'b' => one('\u{8}'),
		'f' => one('\u{c}'),
		'v' => one('\u{b}'),
		'0' if !tail.starts_with(|next: char| next.is_ascii_digit()) => one('\0'),
		// Legacy octal escapes and `\8`/`\9` are syntax errors in a template.
		'0'..='9' if quoting == Quoting::Template => None,
		'0'..='7' => {
			let limit = if c <= '3' {
				2
			} else {
				1
			};
			let more =
				tail.bytes().take(limit).take_while(|byte| (b'0'..=b'7').contains(byte)).count();
			let value = u32::from_str_radix(&rest[1..1 + 1 + more], 8).ok()?;
			Some((Some(char::from_u32(value)?), 2 + more))
		}
		'x' => {
			let value = hex(tail.get(..2)?)?;
			Some((Some(char::from_u32(value)?), after + 2))
		}
		'u' => unicode(tail).map(|(decoded, length)| (Some(decoded), after + length)),
		'\n' | '\u{2028}' | '\u{2029}' => Some((None, after)),
		'\r' => Some((None, after + usize::from(tail.starts_with('\n')))),
		other => one(other),
	}
}

/// Decodes what follows `\u`: `{…}` or four hex digits, joining a UTF-16 surrogate pair. Returns
/// the char and the number of bytes consumed after the `u`.
fn unicode(tail: &str) -> Option<(char, usize)> {
	if let Some(braced) = tail.strip_prefix('{') {
		let close = braced.find('}')?;
		return Some((char::from_u32(hex(&braced[..close])?)?, close + 2));
	}
	let unit = hex(tail.get(..4)?)?;
	if (0xD800..0xDC00).contains(&unit) {
		let low = tail.get(4..)?.strip_prefix("\\u")?;
		let low = hex(low.get(..4)?)?;
		if !(0xDC00..0xE000).contains(&low) {
			return None;
		}
		let combined = 0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
		return Some((char::from_u32(combined)?, 4 + 6));
	}
	Some((char::from_u32(unit)?, 4))
}

/// Parses `digits` as hexadecimal, rejecting signs and the empty string.
fn hex(digits: &str) -> Option<u32> {
	if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		return None;
	}
	u32::from_str_radix(digits, 16).ok()
}

/// Appends the text of `source[range]` with its HTML character references decoded to `text`, and
/// its span map to `segments`.
///
/// This is the value of a JSX attribute string or of a plain markup attribute: no backslash
/// escapes, only `&quot;`, `&amp;`, `&lt;`, `&gt;`, `&apos;` and numeric references (`&#39;`,
/// `&#x27;`). Copied runs map byte for byte and each reference is one segment covering the whole
/// `&…;`. An `&` that does not start one of these stays as it is.
pub(super) fn decode_entities(
	source: &str,
	range: Range<usize>,
	text: &mut String,
	segments: &mut Vec<Segment>,
) {
	let mut at = range.start;
	while at < range.end {
		let rest = &source[at..range.end];
		if rest.starts_with('&')
			&& let Some((decoded, consumed)) = reference(rest)
		{
			segments.push(Segment {
				text_start: text.len(),
				text_len: decoded.len_utf8(),
				host_start: at,
				host_len: consumed,
			});
			text.push(decoded);
			at += consumed;
			continue;
		}
		// The run up to the next `&`, always at least the char at `at`.
		let run =
			rest.char_indices().skip(1).find(|(_, c)| *c == '&').map_or(rest.len(), |(i, _)| i);
		segments.push(Segment {
			text_start: text.len(),
			text_len: run,
			host_start: at,
			host_len: run,
		});
		text.push_str(&rest[..run]);
		at += run;
	}
}

/// The char of the character reference at the start of `rest` (which begins with `&`) and the
/// number of bytes it spans, `None` when it is not one of the supported references.
fn reference(rest: &str) -> Option<(char, usize)> {
	// The longest supported reference is `&#x10FFFF;`.
	let end = rest.bytes().take(12).position(|byte| byte == b';')?;
	let body = &rest[1..end];
	let decoded = match body {
		"quot" => '"',
		"amp" => '&',
		"lt" => '<',
		"gt" => '>',
		"apos" => '\'',
		_ => {
			let digits = body.strip_prefix('#')?;
			let value = match digits.strip_prefix(['x', 'X']) {
				Some(hex_digits) => hex(hex_digits)?,
				None if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) => {
					digits.parse().ok()?
				}
				None => return None,
			};
			char::from_u32(value).filter(|decoded| *decoded != '\0')?
		}
	};
	Some((decoded, end + 1))
}
