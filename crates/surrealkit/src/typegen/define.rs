//! Clause-level parsing of the `DEFINE FIELD` statements `INFO FOR TABLE`
//! returns. No database or IO.
//!
//! SurrealDB renders a field as a header followed by clauses in a fixed order,
//! for example (3.3.0):
//!
//! ```text
//! DEFINE FIELD handle ON person TYPE string DEFAULT ALWAYS 'anon' VALUE string::trim($value) PERMISSIONS FULL
//! DEFINE FIELD weight ON rates TYPE none | float INLINE PERMISSIONS FULL
//! DEFINE FIELD ssn ON person TYPE none | string COMMENT '@acme:policy {…}' PERMISSIONS FOR select WHERE …
//! ```
//!
//! [`parse_field`] splits that into its parts. A clause keyword counts only as a
//! whole upper-case word, after the clause before it in the engine's order (so a
//! field named `type` in a VALUE, ASSERT or PERMISSIONS expression is not a TYPE
//! clause), preceded by whitespace, outside strings (escaped quotes
//! included), quoted identifiers and regexes, and outside `()`, `[]` and `{}` —
//! and, within the `TYPE` clause only, outside `<…>`, so `record<value>` is not
//! mistaken for a `VALUE` clause while `ASSERT $value >= 0` is not mistaken for
//! an opening bracket.

use super::scan::{Token, TokenKind, tokenize};

/// The clause keywords of `DEFINE FIELD` in SurrealDB 3.x, in the order the engine
/// renders them (3.2 and 3.3 alike; 3.3 adds `INLINE` and the `GRAPHQL_*` clauses).
/// The leading `FLEXIBLE` is the order before 3.0 (`FLEXIBLE TYPE object`).
///
/// A keyword starts a clause only after the previous clause's keyword in this order. The
/// engine renders field names unquoted, so in `VALUE type + '!'` or
/// `PERMISSIONS FOR select WHERE type = 'public'` the word `type` names a field: TYPE comes
/// before VALUE and PERMISSIONS, so it cannot start a clause there.
const CLAUSES: &[&str] = &[
	"FLEXIBLE",
	"TYPE",
	"FLEXIBLE",
	"DEFAULT",
	"READONLY",
	"INLINE",
	"VALUE",
	"ASSERT",
	"COMPUTED",
	"REFERENCE",
	"COMMENT",
	"GRAPHQL_ALIAS",
	"GRAPHQL_DEPRECATED",
	"PERMISSIONS",
];

/// The parts of one `DEFINE FIELD` statement. Expressions are the engine's text,
/// trimmed; nothing is evaluated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct FieldDefine<'a> {
	/// The field name as written (`address.city`, `` `odd.name` ``, `tags.*`).
	pub name: &'a str,
	/// The table name as written.
	pub table: &'a str,
	/// The `TYPE` body.
	pub kind: Option<&'a str>,
	pub flexible: bool,
	pub readonly: bool,
	pub inline: bool,
	/// `DEFAULT [ALWAYS] expr`: whether `ALWAYS` was given, and the expression.
	pub default: Option<(bool, &'a str)>,
	pub value: Option<&'a str>,
	pub computed: Option<&'a str>,
	pub assert: Option<&'a str>,
	/// The text after `REFERENCE` (e.g. `ON DELETE CASCADE`), possibly empty.
	pub reference: Option<&'a str>,
	/// The text after `PERMISSIONS`.
	pub permissions: Option<&'a str>,
	/// The `COMMENT` string, unquoted and unescaped.
	pub comment: Option<String>,
}

/// Split a `DEFINE FIELD` statement into its header and clauses. Total: text
/// that does not start with a recognisable header is scanned for clauses from
/// the beginning.
pub(super) fn parse_field(stmt: &str) -> FieldDefine<'_> {
	let tokens = tokenize(stmt);
	let mut out = FieldDefine::default();
	let mut i = match header(stmt, &tokens) {
		Some((name, table, next)) => {
			out.name = name;
			out.table = table;
			next
		}
		None => 0,
	};

	// Find the clause keywords and the token range each one's body covers.
	let mut marks: Vec<(&str, usize, usize)> = Vec::new(); // (keyword, body start byte, keyword start byte)
	let mut depth = 0i32;
	let mut angle = 0i32;
	let mut in_type = false;
	// The position in CLAUSES of the last clause found; only a later one may follow.
	let mut next_clause = 0usize;
	while i < tokens.len() {
		let t = tokens[i];
		match t.kind {
			TokenKind::Open => depth += 1,
			TokenKind::Close => depth -= 1,
			TokenKind::Punct if in_type && t.text(stmt) == "<" => angle += 1,
			TokenKind::Punct if in_type && t.text(stmt) == ">" => angle -= 1,
			TokenKind::Word if depth == 0 && angle <= 0 && is_clause_word(stmt, &tokens, i) => {
				// The engine writes keywords in upper case; a lower-case `comment` is a field.
				let word = t.text(stmt);
				if let Some(at) = CLAUSES[next_clause..].iter().position(|kw| *kw == word) {
					let kw = &CLAUSES[next_clause + at];
					next_clause += at + 1;
					in_type = *kw == "TYPE";
					angle = 0;
					let mut body = t.end;
					// `DEFAULT ALWAYS expr`: ALWAYS belongs to the keyword.
					if *kw == "DEFAULT"
						&& tokens.get(i + 1).is_some_and(|n| n.is_word(stmt, "ALWAYS"))
					{
						i += 1;
						body = tokens[i].end;
						marks.push(("DEFAULT ALWAYS", body, t.start));
					} else {
						marks.push((kw, body, t.start));
					}
				}
			}
			_ => {}
		}
		i += 1;
	}

	for (n, (kw, body_start, _)) in marks.iter().enumerate() {
		let body_end = marks.get(n + 1).map_or(stmt.len(), |next| next.2);
		let body = stmt[*body_start..body_end].trim().trim_end_matches(';').trim();
		match *kw {
			"TYPE" => out.kind = non_empty(body),
			"FLEXIBLE" => out.flexible = true,
			"READONLY" => out.readonly = true,
			"INLINE" => out.inline = true,
			"DEFAULT" => out.default = Some((false, body)),
			"DEFAULT ALWAYS" => out.default = Some((true, body)),
			"VALUE" => out.value = Some(body),
			"COMPUTED" => out.computed = Some(body),
			"ASSERT" => out.assert = Some(body),
			"REFERENCE" => out.reference = Some(body),
			"PERMISSIONS" => out.permissions = Some(body),
			"COMMENT" => out.comment = Some(comment_text(body)),
			_ => {}
		}
	}
	out
}

/// `DEFINE FIELD [OVERWRITE | IF NOT EXISTS] <name> ON [TABLE] <table>`: the
/// name, the table, and the index of the first token after the header.
fn header<'a>(stmt: &'a str, tokens: &[Token]) -> Option<(&'a str, &'a str, usize)> {
	let mut i = 0;
	let word = |i: usize, kw: &str| tokens.get(i).is_some_and(|t| t.is_word(stmt, kw));
	if !(word(0, "DEFINE") && word(1, "FIELD")) {
		return None;
	}
	i += 2;
	if word(i, "OVERWRITE") {
		i += 1;
	} else if word(i, "IF") && word(i + 1, "NOT") && word(i + 2, "EXISTS") {
		i += 3;
	}
	let (name, next) = adjacent_run(stmt, tokens, i)?;
	i = next;
	if !word(i, "ON") {
		return None;
	}
	i += 1;
	if word(i, "TABLE") {
		i += 1;
	}
	let (table, next) = adjacent_run(stmt, tokens, i)?;
	Some((name, unquote_name(table), next))
}

/// The text of the tokens from `i` that touch one another (no whitespace in
/// between): a name such as `phones[*].number` or `` `odd.name` ``.
pub(super) fn adjacent_run<'a>(
	stmt: &'a str,
	tokens: &[Token],
	i: usize,
) -> Option<(&'a str, usize)> {
	let first = tokens.get(i)?;
	let mut end = first.end;
	let mut j = i + 1;
	while let Some(t) = tokens.get(j).filter(|t| t.start == end) {
		end = t.end;
		j += 1;
	}
	Some((&stmt[first.start..end], j))
}

/// A table name without its backtick or `⟨⟩` quoting, borrowed when it had none.
fn unquote_name(name: &str) -> &str {
	name.strip_prefix('`')
		.and_then(|n| n.strip_suffix('`'))
		.or_else(|| name.strip_prefix('\u{27E8}').and_then(|n| n.strip_suffix('\u{27E9}')))
		.unwrap_or(name)
}

/// Whether the word at `i` stands alone as a keyword: preceded by whitespace
/// (or nothing) and not the first part of a path such as `type::is::string` or
/// a record id such as `value:1`.
pub(super) fn is_clause_word(stmt: &str, tokens: &[Token], i: usize) -> bool {
	let t = tokens[i];
	let spaced_before = i == 0 || tokens[i - 1].end < t.start;
	let path_follows = tokens
		.get(i + 1)
		.is_some_and(|n| n.start == t.end && n.kind == TokenKind::Punct && n.text(stmt) == ":");
	spaced_before && !path_follows
}

/// The value of a `COMMENT` clause: its string literal, unquoted. Anything else
/// (which the engine does not produce) is kept as written.
fn comment_text(body: &str) -> String {
	let tokens = tokenize(body);
	match tokens.as_slice() {
		[t] if t.kind == TokenKind::String => super::scan::unquote(t.text(body)),
		_ => body.to_string(),
	}
}

fn non_empty(s: &str) -> Option<&str> {
	if s.is_empty() {
		None
	} else {
		Some(s)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn splits_every_clause_the_engine_renders() {
		let f = parse_field(
			"DEFINE FIELD handle ON person TYPE string DEFAULT ALWAYS 'anon' READONLY \
			 VALUE string::trim($value) ASSERT $value != NONE REFERENCE ON DELETE CASCADE \
			 COMMENT 'hi' PERMISSIONS FOR select FULL, FOR update NONE",
		);
		assert_eq!(f.name, "handle");
		assert_eq!(f.table, "person");
		assert_eq!(f.kind, Some("string"));
		assert_eq!(f.default, Some((true, "'anon'")));
		assert!(f.readonly && !f.flexible && !f.inline);
		assert_eq!(f.value, Some("string::trim($value)"));
		assert_eq!(f.assert, Some("$value != NONE"));
		assert_eq!(f.reference, Some("ON DELETE CASCADE"));
		assert_eq!(f.comment.as_deref(), Some("hi"));
		assert_eq!(f.permissions, Some("FOR select FULL, FOR update NONE"));
	}

	#[test]
	fn flexible_computed_and_inline_end_the_type() {
		let f = parse_field("DEFINE FIELD address ON person TYPE object FLEXIBLE PERMISSIONS FULL");
		assert_eq!(f.kind, Some("object"));
		assert!(f.flexible);
		let f = parse_field(
			"DEFINE FIELD display_typed ON person TYPE string COMPUTED string::concat(name, '!') \
			 PERMISSIONS FULL",
		);
		assert_eq!(f.kind, Some("string"));
		assert_eq!(f.computed, Some("string::concat(name, '!')"));
		let f =
			parse_field("DEFINE FIELD weight ON rates TYPE none | float INLINE PERMISSIONS FULL");
		assert_eq!(f.kind, Some("none | float"));
		assert!(f.inline);
		let f =
			parse_field("DEFINE FIELD referenced_by ON person COMPUTED <~person PERMISSIONS FULL");
		assert_eq!(f.kind, None);
		assert_eq!(f.computed, Some("<~person"));
		assert_eq!(f.permissions, Some("FULL"));
	}

	#[test]
	fn keywords_inside_strings_with_escapes_are_text() {
		// What 3.3.0 renders for the corpus `motto` field: double quotes around a
		// string containing single quotes and keywords, and escaped double quotes.
		let f = parse_field(
			r#"DEFINE FIELD motto ON person TYPE string DEFAULT "TYPE; VALUE 'quoted' DEFAULT" COMMENT "semi; colon \"and\" 'quotes'" PERMISSIONS FULL"#,
		);
		assert_eq!(f.kind, Some("string"));
		assert_eq!(f.default, Some((false, r#""TYPE; VALUE 'quoted' DEFAULT""#)));
		assert_eq!(f.comment.as_deref(), Some(r#"semi; colon "and" 'quotes'"#));
		assert_eq!(f.permissions, Some("FULL"));
		assert!(f.value.is_none());
	}

	#[test]
	fn comparisons_do_not_hide_later_clauses() {
		let f = parse_field(
			"DEFINE FIELD total ON purchase TYPE decimal READONLY ASSERT $value >= 0dec COMMENT \
			 'paid' PERMISSIONS FULL",
		);
		assert_eq!(f.assert, Some("$value >= 0dec"));
		assert!(f.readonly);
		assert_eq!(f.comment.as_deref(), Some("paid"));
		assert_eq!(f.permissions, Some("FULL"));
		let f = parse_field(
			"DEFINE FIELD x ON t TYPE int READONLY ASSERT $value < 10 COMPUTED <~t PERMISSIONS FULL",
		);
		assert_eq!(f.assert, Some("$value < 10"));
		assert_eq!(f.computed, Some("<~t"));
		assert!(f.readonly);
	}

	#[test]
	fn keyword_like_words_in_expressions_are_not_clauses() {
		let f = parse_field(
			"DEFINE FIELD x ON t TYPE string VALUE type::string($value.comment) \
			 ASSERT $value = /^(DEFAULT|VALUE)$/ PERMISSIONS FULL",
		);
		assert_eq!(f.value, Some("type::string($value.comment)"));
		assert_eq!(f.assert, Some("$value = /^(DEFAULT|VALUE)$/"));
		let f = parse_field("DEFINE FIELD y ON t TYPE object DEFAULT { value: 1, comment: 'x' }");
		assert_eq!(f.default, Some((false, "{ value: 1, comment: 'x' }")));
		assert!(f.value.is_none() && f.comment.is_none());
	}

	#[test]
	fn generics_hide_keywords_only_in_the_type_clause() {
		let f = parse_field("DEFINE FIELD r ON t TYPE record<value | comment> VALUE $value");
		assert_eq!(f.kind, Some("record<value | comment>"));
		assert_eq!(f.value, Some("$value"));
		// After TYPE, `<` is a comparison and must not swallow what follows.
		let f = parse_field("DEFINE FIELD n ON t TYPE int ASSERT $value < 5 COMMENT 'c'");
		assert_eq!(f.comment.as_deref(), Some("c"));
	}

	#[test]
	fn names_with_dots_quotes_and_element_markers() {
		let f =
			parse_field("DEFINE FIELD `odd.name` ON person TYPE none | string PERMISSIONS FULL");
		assert_eq!(f.name, "`odd.name`");
		assert_eq!(f.kind, Some("none | string"));
		let f = parse_field("DEFINE FIELD phones[*].number ON TABLE person TYPE string");
		assert_eq!(f.name, "phones[*].number");
		assert_eq!(f.table, "person");
		let f = parse_field("DEFINE FIELD OVERWRITE tags.* ON `my table` TYPE string");
		assert_eq!(f.name, "tags.*");
		assert_eq!(f.table, "my table");
		let f = parse_field("DEFINE FIELD IF NOT EXISTS type ON t TYPE string");
		assert_eq!(f.name, "type");
		assert_eq!(f.kind, Some("string"));
	}

	#[test]
	fn a_field_without_type_has_no_kind() {
		let f = parse_field("DEFINE FIELD notes ON person PERMISSIONS FULL");
		assert_eq!(f.kind, None);
		assert_eq!(f.permissions, Some("FULL"));
	}

	#[test]
	fn reference_then_expression_is_kept_verbatim() {
		let f = parse_field(
			"DEFINE FIELD mentor ON person TYPE none | record<person> REFERENCE ON DELETE THEN \
			 (UPDATE $this SET flagged = true) PERMISSIONS FULL",
		);
		assert_eq!(f.reference, Some("ON DELETE THEN (UPDATE $this SET flagged = true)"));
		let f = parse_field(
			"DEFINE FIELD w ON person TYPE array<record<person>> REFERENCE PERMISSIONS FULL",
		);
		assert_eq!(f.reference, Some(""));
	}

	/// The engine renders a field named `type` (or `value`, `comment`) unquoted wherever an
	/// expression reads it. Only the engine's clause order says where clauses start.
	#[test]
	fn fields_named_like_clauses_inside_expressions_are_not_clauses() {
		let f =
			parse_field("DEFINE FIELD label ON t TYPE string VALUE type + '!' PERMISSIONS FULL");
		assert_eq!(f.kind, Some("string"));
		assert_eq!(f.value, Some("type + '!'"));
		let f = parse_field(
			"DEFINE FIELD amount ON t TYPE int ASSERT type != 'refund' OR $value > 0 PERMISSIONS \
			 FULL",
		);
		assert_eq!(f.kind, Some("int"));
		assert_eq!(f.assert, Some("type != 'refund' OR $value > 0"));
		let f = parse_field(
			"DEFINE FIELD secret ON t TYPE none | string PERMISSIONS FOR select WHERE type = \
			 'public', FOR create, update FULL",
		);
		assert_eq!(f.kind, Some("none | string"));
		assert_eq!(
			f.permissions,
			Some("FOR select WHERE type = 'public', FOR create, update FULL")
		);
		// Without a TYPE clause, a later `type` is still an expression.
		let f = parse_field("DEFINE FIELD n ON t COMPUTED type PERMISSIONS FULL");
		assert_eq!(f.kind, None);
		assert_eq!(f.computed, Some("type"));
		let f = parse_field("DEFINE FIELD o ON t TYPE int VALUE value * 2 PERMISSIONS FULL");
		assert_eq!(f.value, Some("value * 2"));
		let f = parse_field(
			"DEFINE FIELD p ON t TYPE string VALUE comment + readonly PERMISSIONS FULL",
		);
		assert_eq!(f.value, Some("comment + readonly"));
		assert!(!f.readonly);
		assert_eq!(f.comment, None);
	}

	#[test]
	fn graphql_clauses_end_the_comment() {
		let f = parse_field(
			"DEFINE FIELD name ON person TYPE string COMMENT 'hi' GRAPHQL_ALIAS 'n' \
			 GRAPHQL_DEPRECATED 'old' PERMISSIONS FULL",
		);
		assert_eq!(f.comment.as_deref(), Some("hi"));
		assert_eq!(f.permissions, Some("FULL"));
	}

	#[test]
	fn text_without_a_header_is_still_scanned() {
		let f = parse_field("TYPE int DEFAULT 1");
		assert_eq!(f.name, "");
		assert_eq!(f.kind, Some("int"));
		assert_eq!(f.default, Some((false, "1")));
	}
}
