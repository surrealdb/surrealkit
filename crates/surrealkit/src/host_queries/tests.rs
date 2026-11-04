use super::*;

const METHODS: &[&str] = DEFAULT_METHODS;

fn texts(file: &str, source: &str) -> Vec<String> {
	extract(file, source, METHODS).into_iter().map(|query| query.text).collect()
}

fn only(file: &str, source: &str) -> HostQuery {
	let mut queries = extract(file, source, METHODS);
	assert_eq!(queries.len(), 1, "expected one query in {source:?}");
	queries.remove(0)
}

/// The host bytes `text_range` of `query` maps back to.
fn host_text<'a>(source: &'a str, query: &HostQuery, text_range: Range<usize>) -> &'a str {
	&source[query.host_span(text_range)]
}

#[test]
fn quoted_literals_extract_their_contents() {
	let source = "db.query(\"SELECT 1\"); db.query('SELECT 2'); db.query(`SELECT 3`);";
	assert_eq!(texts("a.ts", source), ["SELECT 1", "SELECT 2", "SELECT 3"]);
	let queries = extract("a.ts", source, METHODS);
	assert!(queries.iter().all(HostQuery::keyable));
	assert_eq!(&source[queries[0].host_range.clone()], "SELECT 1");
}

#[test]
fn receivers_do_not_matter_but_a_member_call_is_required() {
	let source = "
this.db.query(\"A\");
tx.query(\"B\");
a?.query(\"C\");
db.query<Row>(\"D\");
query(\"bare\");
db['query'](\"computed\");
tag.query`tagged`;
";
	assert_eq!(texts("a.ts", source), ["A", "B", "C", "D"]);
}

#[test]
fn only_a_literal_first_argument_counts() {
	let source = "
db.query(sql);
db.query(\"a\" + \"b\");
db.query(sql, \"SELECT 1\");
db.query();
db.query({ url: '/api' });
db.query(/* lead */ \"kept\");
";
	assert_eq!(texts("a.ts", source), ["kept"]);
}

#[test]
fn the_method_list_is_configurable() {
	let source = "db.run(\"R\"); db.query(\"Q\"); db.live(\"L\");";
	assert_eq!(texts("a.ts", source), ["Q"]);
	let queries = extract("a.ts", source, &["run", "query"]);
	assert_eq!(queries.iter().map(|query| query.text.as_str()).collect::<Vec<_>>(), ["R", "Q"]);
	assert!(extract("a.ts", source, &[]).is_empty());
}

#[test]
fn a_query_records_the_method_it_was_passed_to() {
	let source = "db.run(\"R\"); db.query(`Q ${x}`); db.query('S');";
	let queries = extract("a.ts", source, &["run", "query"]);
	let methods: Vec<&str> = queries.iter().map(|query| query.method.as_str()).collect();
	assert_eq!(methods, ["run", "query", "query"]);
}

#[test]
fn nested_and_chained_calls_are_found() {
	let source = "const r = await db.query(\"SELECT 1\").collect();\nf(db.query(\"SELECT 2\"));";
	assert_eq!(texts("a.ts", source), ["SELECT 1", "SELECT 2"]);
}

#[test]
fn escapes_decode_to_the_runtime_value() {
	let source = r#"db.query("a\nb\t\\ \' \" \` \$ A \u{1F600} \x41 \0 é \z");"#;
	let query = only("a.ts", source);
	assert_eq!(query.text, "a\nb\t\\ ' \" ` $ A \u{1F600} A \0 é z");
	assert!(query.keyable());
}

#[test]
fn surrogate_pairs_join_and_lone_surrogates_drop_the_query() {
	assert_eq!(only("a.ts", r#"db.query("😀");"#).text, "\u{1F600}");
	assert!(extract("a.ts", r#"db.query("\uD83D");"#, METHODS).is_empty());
	assert!(extract("a.ts", r#"db.query("\u{110000}");"#, METHODS).is_empty());
	assert!(extract("a.ts", r#"db.query("\xZZ");"#, METHODS).is_empty());
}

#[test]
fn line_continuations_vanish_and_octal_escapes_decode_in_strings() {
	let source = "db.query(\"a\\\nb\\\r\nc\\101\");";
	assert_eq!(only("a.ts", source).text, "abcA");
	// An octal escape is a syntax error in a template.
	assert!(extract("a.ts", "db.query(`\\101`);", METHODS).is_empty());
	assert_eq!(only("a.ts", "db.query(`a\\\nb`);").text, "ab");
}

#[test]
fn offsets_map_back_through_decoded_escapes() {
	let source = r#"db.query("SELECT 'a\nb' FROM person");"#;
	let query = only("a.ts", source);
	assert_eq!(query.text, "SELECT 'a\nb' FROM person");

	let person = query.text.find("person").expect("person present");
	let host = query.host_offset(person);
	assert_eq!(&source[host..host + 6], "person");
	assert_eq!(query.embed_offset(host), Some(person));

	// The decoded newline maps to the whole `\n` escape, from either byte of it.
	let newline = query.text.find('\n').expect("newline present");
	assert_eq!(host_text(source, &query, newline..newline + 1), "\\n");
	let backslash = source.find("\\n").expect("escape present");
	assert_eq!(query.embed_offset(backslash), Some(newline));
	assert_eq!(query.embed_offset(backslash + 1), Some(newline));
	assert_eq!(query.host_offset(newline), backslash);

	// A span over text on both sides of the escape covers the escape.
	let start = query.text.find("'a").expect("start present");
	let end = query.text.find("b'").expect("end present") + 2;
	assert_eq!(host_text(source, &query, start..end), "'a\\nb'");
}

#[test]
fn offsets_map_back_through_multibyte_text() {
	let source = "db.query(\"SELECT 'é€' FROM person\");";
	let query = only("a.ts", source);
	let euro = query.text.find('€').expect("euro present");
	assert_eq!(host_text(source, &query, euro..euro + '€'.len_utf8()), "€");
	// A one-byte range inside a multi-byte char widens to the whole char.
	assert_eq!(host_text(source, &query, euro..euro + 1), "€");
}

#[test]
fn substitutions_become_numbered_parameters() {
	let source = "db.query(`SELECT * FROM person WHERE age > ${min} AND name = ${user.name}`);";
	let query = only("a.ts", source);

	assert_eq!(query.text, "SELECT * FROM person WHERE age > $__host0 AND name = $__host1");
	assert!(!query.keyable());
	let [first, second] = query.substitutions.as_slice() else {
		panic!("expected two substitutions");
	};
	assert_eq!(first.param, "__host0");
	assert_eq!(&source[first.host_range.clone()], "${min}");
	assert_eq!(&query.text[first.text_range.clone()], "$__host0");
	assert_eq!(second.param, "__host1");
	assert_eq!(&source[second.host_range.clone()], "${user.name}");
	assert_eq!(&query.text[second.text_range.clone()], "$__host1");

	// A range touching the parameter widens to the whole `${…}`.
	let at = first.text_range.start + 3;
	assert_eq!(host_text(source, &query, at..at + 1), "${min}");
	assert_eq!(query.host_offset(at), first.host_range.start);
	// Text after the substitution still maps verbatim, and the substitution is not addressable.
	let name = query.text.find("name").expect("name present");
	assert_eq!(&source[query.host_offset(name)..][..4], "name");
	assert_eq!(query.embed_offset(first.host_range.start + 2), None);
	assert_eq!(query.embed_offset(0), None);
}

#[test]
fn a_nested_template_stays_inside_its_substitution() {
	let source = "db.query(`SELECT ${a(`x${b}`)} FROM t WHERE ${c}`);";
	let query = only("a.ts", source);
	assert_eq!(query.text, "SELECT $__host0 FROM t WHERE $__host1");
	assert_eq!(&source[query.substitutions[0].host_range.clone()], "${a(`x${b}`)}");
}

#[test]
fn template_chunks_decode_and_normalise_line_endings() {
	let source = "db.query(`a\r\nb\rc\\n${x}\\$d\\`e`);";
	let query = only("a.ts", source);
	assert_eq!(query.text, "a\nb\nc\n$__host0$d`e");
	assert_eq!(&source[query.host_span(0..1)], "a");
	let b = query.text.find('b').expect("b present");
	assert_eq!(&source[query.host_offset(b)..][..1], "b");
	assert_eq!(host_text(source, &query, 1..2), "\r\n");
}

#[test]
fn empty_literals_extract_as_empty_queries() {
	let query = only("a.ts", "db.query(\"\");");
	assert_eq!(query.text, "");
	assert!(query.keyable());
	assert_eq!(query.host_range.start, query.host_range.end);
}

#[test]
fn line_and_column_are_one_based_and_count_chars() {
	let source = "const é = 1;\n\tconst q = db.query(\"SELECT 1\");\n";
	let query = only("a.ts", source);
	// `SELECT` starts after a tab and `const q = db.query("`.
	let expected_column = "\tconst q = db.query(\"".chars().count() + 1;
	assert_eq!((query.line, query.column), (2, u32::try_from(expected_column).expect("fits")));

	let wide = "const é = db.query(\"SELECT 1\");";
	let query = only("a.ts", wide);
	assert_eq!((query.line, query.column), (1, 21));
	assert!(query.host_range.start > 21 - 1, "byte offset exceeds the char column");
}

#[test]
fn crlf_files_keep_lines_and_offsets() {
	let source = "const a = 1;\r\nconst q = db.query(\"SELECT 1\");\r\n";
	let query = only("a.ts", source);
	assert_eq!(query.line, 2);
	assert_eq!(&source[query.host_range], "SELECT 1");
}

#[test]
fn tsx_and_jsx_files_use_the_jsx_grammar() {
	let source = "export const W = () => <div>{db.query(\"SELECT 1\")}</div>;";
	assert_eq!(texts("w.tsx", source), ["SELECT 1"]);
	assert_eq!(texts("w.jsx", source), ["SELECT 1"]);
}

#[test]
fn svelte_scripts_extract_with_whole_file_positions() {
	let source = "<h1>hi</h1>\n<script context=\"module\">\nexport const a = db.query(\"SELECT 1\");\n</script>\n\n<SCRIPT lang=\"ts\">\n\tconst b = db.query('SELECT 2');\n</SCRIPT>\n";
	let queries = extract("p.svelte", source, METHODS);
	assert_eq!(queries.len(), 2);
	assert_eq!(&source[queries[0].host_range.clone()], "SELECT 1");
	assert_eq!((queries[0].line, queries[0].column), (3, 28));
	assert_eq!(&source[queries[1].host_range.clone()], "SELECT 2");
	assert_eq!((queries[1].line, queries[1].column), (7, 22));
	let two = queries[1].text.find('2').expect("2 present");
	assert_eq!(&source[queries[1].host_offset(two)..][..1], "2");
}

#[test]
fn script_blocks_filter_on_type_and_survive_quoted_angles() {
	let source = r#"
<script type="application/json">db.query("json")</script>
<script type="importmap">db.query("map")</script>
<script src="x.js" />
<script type="module" generic="T extends Array<string>">db.query("module")</script>
<script>db.query("plain")</script>
<scripty>db.query("not a script")</scripty>
"#;
	assert_eq!(texts("p.svelte", source), ["module", "plain"]);
}

#[test]
fn vue_script_setup_extracts() {
	let source = "<template>\n  <p>{{ rows }}</p>\n</template>\n\n<script setup lang=\"ts\">\nconst rows = await db.query(\"SELECT * FROM comment\");\n</script>\n";
	let query = only("c.vue", source);
	assert_eq!(query.text, "SELECT * FROM comment");
	assert_eq!(query.line, 6);
	assert_eq!(&source[query.host_range], "SELECT * FROM comment");
}

#[test]
fn astro_frontmatter_and_scripts_extract() {
	let source = "---\nconst a = await db.query(\"SELECT 1\");\n---\n<ul></ul>\n<script>\n\tdb.query(\"SELECT 2\");\n</script>\n";
	let queries = extract("p.astro", source, METHODS);
	assert_eq!(
		queries.iter().map(|query| query.text.as_str()).collect::<Vec<_>>(),
		["SELECT 1", "SELECT 2"]
	);
	assert_eq!((queries[0].line, queries[1].line), (2, 6));
	assert_eq!(&source[queries[1].host_range.clone()], "SELECT 2");

	// No frontmatter fence: only the script blocks count.
	assert_eq!(texts("p.astro", "<script>db.query(\"S\")</script>"), ["S"]);
	// An unclosed fence is not frontmatter.
	assert!(texts("p.astro", "---\ndb.query(\"S\")\n").is_empty());
}

#[test]
fn frontmatter_ranges_exclude_the_fences() {
	let source = "---\nlet a;\n---\n<p/>";
	let range = frameworks::astro_frontmatter(source).expect("frontmatter present");
	assert_eq!(&source[range], "let a;\n");
	assert!(frameworks::astro_frontmatter("<p/>\n---\nx\n---").is_none());
}

/// The text of each comment `comments` finds.
fn comment_texts<'a>(file: &str, source: &'a str) -> Vec<&'a str> {
	comments(file, source).into_iter().map(|range| &source[range]).collect()
}

#[test]
fn comments_are_the_grammars_comment_nodes() {
	let source = "// one\nconst a = 1; // two\n/* three */ const s = \"// not\" + `/* nor */`;\n";
	assert_eq!(comment_texts("a.ts", source), ["// one", "// two", "/* three */"]);
	assert!(comments("a.ts", "const a = 1;\n").is_empty());
	assert_eq!(comment_texts("notes", "// plain\n"), ["// plain"], "unknown extensions are TS");
}

#[test]
fn tsx_comments_are_found_with_the_jsx_grammar() {
	let source =
		"// head\nexport const W = () => <div>{/* in jsx */ db.query(\"S\")}</div>; // tail\n";
	assert_eq!(comment_texts("w.tsx", source), ["// head", "/* in jsx */", "// tail"]);
	assert_eq!(comment_texts("w.jsx", source), ["// head", "/* in jsx */", "// tail"]);
}

#[test]
fn svelte_comments_are_shifted_into_the_whole_file() {
	let source = "<!-- markup -->\n<script>\n// first\nconst a = 1; // second\n</script>\n<p>// text</p>\n<script lang=\"ts\">\n/* third */\n</script>\n";
	let found = comments("p.svelte", source);
	assert_eq!(
		found.iter().map(|range| &source[range.clone()]).collect::<Vec<_>>(),
		["// first", "// second", "/* third */"]
	);
	// Offsets index the whole file, so a query's line can be compared with a comment's.
	assert_eq!(&source[..found[0].start].matches('\n').count(), &2);
}

#[test]
fn vue_and_astro_comments_come_from_their_script_regions() {
	let vue = "<template><p/></template>\n<script setup lang=\"ts\">\n// v\n</script>\n";
	assert_eq!(comment_texts("c.vue", vue), ["// v"]);
	let astro = "---\n// front\nlet a;\n---\n<ul></ul>\n<script>\n// client\n</script>\n";
	assert_eq!(comment_texts("p.astro", astro), ["// front", "// client"]);
	assert!(comments("p.astro", "<p>// no script</p>").is_empty());
}

#[test]
fn unknown_extensions_parse_as_typescript() {
	assert_eq!(texts("notes", "db.query(\"S\")"), ["S"]);
}

#[test]
fn scan_skips_declaration_files_and_build_output() {
	assert!(is_source(Path::new("src/a.ts")));
	assert!(is_source(Path::new("src/A.SVELTE")));
	assert!(!is_source(Path::new("src/types.d.ts")));
	assert!(!is_source(Path::new("src/readme.md")));
	assert!(!is_source(Path::new("src/noext")));
}

#[test]
fn is_source_judges_the_directories_below_the_root() {
	assert!(is_source(Path::new("app.ts")));
	assert!(is_source(Path::new("routes/+page.svelte")));
	assert!(!is_source(Path::new("node_modules/pkg/index.js")));
	assert!(!is_source(Path::new("web/dist/app.js")));
	assert!(!is_source(Path::new(".svelte-kit/generated/root.js")));
	assert!(!is_source(Path::new(".hidden/h.ts")));
	assert!(!is_source(Path::new("lib/.cache/x.ts")));
	// The file's own name is not a directory: a dotfile with a source extension is read.
	assert!(is_source(Path::new("src/.eslintrc.js")));
}
