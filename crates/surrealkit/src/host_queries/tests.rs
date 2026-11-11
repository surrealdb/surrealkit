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

fn all(file: &str, source: &str) -> Vec<HostQuery> {
	extract_with(file, source, &Queries::DEFAULT)
}

fn all_texts(file: &str, source: &str) -> Vec<String> {
	all(file, source).into_iter().map(|query| query.text).collect()
}

fn all_only(file: &str, source: &str) -> HostQuery {
	let mut queries = all(file, source);
	assert_eq!(queries.len(), 1, "expected one query in {source:?}: {queries:#?}");
	queries.remove(0)
}

#[test]
fn bare_and_namespaced_function_calls_carry_a_query() {
	let source = "
createQuery(\"A\");
svelte.createQuery('B');
kit.useQuery(`C ${x}`);
useLive<Row>(\"D\");
const q = useQuery(\"E\", { id });
";
	let queries = all("a.ts", source);
	assert_eq!(
		queries.iter().map(|query| query.text.as_str()).collect::<Vec<_>>(),
		["A", "B", "C $__host0", "D", "E"]
	);
	assert_eq!(queries[0].source, HostQuerySource::Function("createQuery".to_string()));
	assert_eq!(queries[2].source, HostQuerySource::Function("useQuery".to_string()));
	assert_eq!(queries[2].method, "useQuery");
	assert_eq!(&source[queries[2].substitutions[0].host_range.clone()], "${x}");
	// `extract` reads member calls named by `methods` only.
	assert!(extract("a.ts", source, DEFAULT_METHODS).is_empty());
}

#[test]
fn a_bare_query_call_and_a_non_literal_first_argument_are_not_queries() {
	let source = "
query(\"bare\");
createQuery(sql);
createQuery();
createQuery(\"a\" + \"b\");
createQuery(sql, \"SELECT 1\");
other(\"not listed\");
db['createQuery'](\"computed\");
";
	assert!(all("a.ts", source).is_empty());
	// A method still needs a receiver, and wins over a function of the same name.
	let both = Queries {
		methods: &["query"],
		functions: &[QueryFunction::first("query")],
		..Queries::DEFAULT
	};
	let found = extract_with("a.ts", "query(\"F\"); db.query(\"M\");", &both);
	assert_eq!(
		found.iter().map(|query| query.source.clone()).collect::<Vec<_>>(),
		[
			HostQuerySource::Function("query".to_string()),
			HostQuerySource::Method("query".to_string())
		]
	);
}

#[test]
fn function_calls_are_found_in_script_blocks() {
	let svelte =
		"<script>\n\tconst people = createQuery(\"SELECT * FROM person\", { });\n</script>\n";
	let query = all_only("p.svelte", svelte);
	assert_eq!(query.text, "SELECT * FROM person");
	assert_eq!((query.line, query.column), (2, 30));
	assert_eq!(&svelte[query.host_range], "SELECT * FROM person");
	assert_eq!(all_texts("c.vue", "<script setup>\nconst r = useQuery(\"V\");\n</script>"), ["V"]);
	assert_eq!(all_texts("p.astro", "---\nconst r = useLive(\"A\");\n---\n<p/>"), ["A"]);
}

#[test]
fn preload_carries_its_query_in_the_second_argument_by_default() {
	let source = "
preload(db, \"A\");
kit.preload(db, 'B');
preload(db, `C ${x}`, { cache: true });
preload(db /* the client */, /* the query */ \"D\");
const rows = await preload(this.db, \"E\");
";
	let queries = all("a.ts", source);
	assert_eq!(
		queries.iter().map(|query| query.text.as_str()).collect::<Vec<_>>(),
		["A", "B", "C $__host0", "D", "E"]
	);
	assert!(queries.iter().all(|query| {
		query.source == HostQuerySource::Function("preload".to_string())
			&& query.method == "preload"
	}));
	// The range is the literal's contents, so the position indexes the host file.
	assert_eq!(&source[queries[3].host_range.clone()], "D");
	assert_eq!((queries[0].line, queries[0].column), (2, 14));
	for (file, text) in [
		("p.svelte", "<script>\n\tconst rows = preload(db, \"S\");\n</script>\n"),
		("p.vue", "<script setup>\nconst rows = preload(db, \"V\");\n</script>\n"),
		("p.astro", "---\nconst rows = preload(db, \"A\");\n---\n<p/>"),
		("p.tsx", "export const R = () => { preload(db, \"X\"); return <p/>; };"),
		("p.jsx", "export const R = () => { kit.preload(db, \"J\"); return <p/>; };"),
		("p.js", "preload(db, \"M\");"),
	] {
		assert_eq!(all_texts(file, text).len(), 1, "{file}");
	}
}

#[test]
fn a_call_with_too_few_arguments_or_a_non_literal_at_the_position_is_not_a_query() {
	let source = "
preload(db);
preload();
preload(\"not a query\", db);
preload(db, sql);
preload(db, surql`SELECT 1`);
preload(db, 'a' + b);
preload(db, ...rest);
preload(/* only a comment */);
kit.preload(db /* the query is missing */);
";
	assert!(all("a.ts", source).is_empty(), "{:#?}", all("a.ts", source));
	// Position 0 of a position-1 function is not read, and position 1 of a position-0 one is not.
	assert!(all("a.ts", "useQuery(db, \"SELECT 1\");").is_empty());
	assert_eq!(all_texts("a.ts", "useQuery(\"SELECT 1\", db);"), ["SELECT 1"]);
}

#[test]
fn a_function_reads_the_argument_it_names() {
	let third = Queries {
		functions: &[QueryFunction {
			name: "run",
			arg: 2,
		}],
		..Queries::DEFAULT
	};
	let source = "run(a, b, \"C\"); run(a, \"B\"); run(\"A\", b); run(a, b, c, \"D\");";
	let found = extract_with("a.ts", source, &third);
	assert_eq!(found.iter().map(|query| query.text.as_str()).collect::<Vec<_>>(), ["C"]);
	assert_eq!(found[0].source, HostQuerySource::Function("run".to_string()));
	// A method keeps position 0 even when a function of the same name reads another.
	let both = Queries {
		methods: &["run"],
		functions: &[QueryFunction {
			name: "run",
			arg: 1,
		}],
		..Queries::DEFAULT
	};
	let found = extract_with("a.ts", "db.run(\"M\", \"x\"); run(\"x\", \"F\");", &both);
	assert_eq!(
		found.iter().map(|query| (query.text.as_str(), query.source.clone())).collect::<Vec<_>>(),
		[
			("M", HostQuerySource::Method("run".to_string())),
			("F", HostQuerySource::Function("run".to_string()))
		]
	);
	// With no functions configured nothing matches.
	let none = Queries {
		functions: &[],
		..Queries::DEFAULT
	};
	assert!(extract_with("a.ts", "preload(db, \"A\"); useQuery(\"B\");", &none).is_empty());
}

#[test]
fn jsx_attributes_carry_a_query() {
	let source = "\
const a = <Query q=\"SELECT * FROM person WHERE a &gt; 1 &amp;&amp; b = &#39;x&#39;\" />;
const b = <LiveQuery q={\"SELECT 1\"}></LiveQuery>;
const c = <Kit.Query q={`SELECT ${x} FROM t`} />;
const d = <Query q={sql} />;
const e = <Other q=\"no\" />;
const f = <QueryList q=\"no\" />;
const g = <Query q=\"a\\nb\" />;
const h = <Query other=\"no\" q={f(\"x\")} />;
const i = <Query q=\"SELECT 'a > b'\"
	params={{ a: 1 }} />;
";
	let queries = all("w.tsx", source);
	let texts: Vec<&str> = queries.iter().map(|query| query.text.as_str()).collect();
	assert_eq!(
		texts,
		[
			"SELECT * FROM person WHERE a > 1 && b = 'x'",
			"SELECT 1",
			"SELECT $__host0 FROM t",
			"a\\nb",
			"SELECT 'a > b'"
		]
	);
	assert_eq!(all_texts("w.jsx", source), texts);
	assert_eq!(
		queries[0].source,
		HostQuerySource::Attribute {
			component: "Query".to_string(),
			attribute: "q".to_string()
		}
	);
	assert_eq!(queries[1].method, "LiveQuery");
	let Some(HostQuerySource::Attribute {
		component,
		..
	}) = queries.get(2).map(|query| query.source.clone())
	else {
		panic!("expected an attribute");
	};
	assert_eq!(component, "Query", "a member name matches on its last segment");

	assert_eq!(&source[queries[0].host_range.clone()].len(), &queries[0].host_range.len());
	assert_eq!(
		&source[queries[0].host_range.clone()],
		"SELECT * FROM person WHERE a &gt; 1 &amp;&amp; b = &#39;x&#39;"
	);
	assert_eq!((queries[0].line, queries[0].column), (1, 21));
	assert_eq!(&source[queries[1].host_range.clone()], "SELECT 1");
	assert_eq!((queries[1].line, queries[1].column), (2, 26));
	assert_eq!(&source[queries[2].substitutions[0].host_range.clone()], "${x}");
	// A JSX attribute string has no backslash escapes.
	assert_eq!(&source[queries[3].host_range.clone()], "a\\nb");
	assert_eq!((queries[4].line, queries[4].column), (9, 21));
	// Without a component list, or for a custom attribute, nothing matches.
	assert!(extract("w.tsx", source, DEFAULT_METHODS).is_empty());
	let custom = Queries {
		attribute: "sql",
		..Queries::DEFAULT
	};
	assert!(extract_with("w.tsx", source, &custom).is_empty());
	let found = extract_with("w.tsx", "<Query sql=\"S\" q=\"no\" />", &custom);
	assert_eq!(found.len(), 1);
	assert_eq!(found[0].text, "S");
}

#[test]
fn tsx_function_calls_and_attributes_come_in_document_order() {
	let source = "export const V = () => { const { data } = useQuery(\"SELECT * FROM person\"); return <Query q={\"SELECT name FROM person\"} />; };";
	assert_eq!(all_texts("v.tsx", source), ["SELECT * FROM person", "SELECT name FROM person"]);
}

const SVELTE: &str = "\
<script>
	const people = createQuery(\"SELECT * FROM person\", { });
</script>

<Query q=\"SELECT * FROM person WHERE age > {min} AND n = &quot;x&quot;\" params={{ min }} />
<LiveQuery q={`SELECT * FROM post`} />
<Query
	q='SELECT * FROM t'
	params={{ a: 1 }}
>
	<p>hi</p>
</Query>
<Query q={sql} />
<QueryList q=\"SELECT list\" />
<!-- <Query q=\"SELECT commented\" /> -->
<Other title=\"a > b\" q=\"SELECT other\" />
<Foo data=\"<Query q='SELECT nested'>\" />
<Query q=\"SELECT 'a > b'\" />
<Query q={ \"SELECT {nested} }\" } />
<Query q={`SELECT ${fn({ a: \"}\" })} FROM t`} />
<Kit.LiveQuery q=\"SELECT member\" />
<style>.a { color: red }</style>
<Query {q} {...rest} q={sql + \"x\"} />
";

#[test]
fn svelte_attributes_in_every_form() {
	let queries = all("Bindings.svelte", SVELTE);
	let texts: Vec<&str> = queries.iter().map(|query| query.text.as_str()).collect();
	assert_eq!(
		texts,
		[
			"SELECT * FROM person",
			"SELECT * FROM person WHERE age > $__host0 AND n = \"x\"",
			"SELECT * FROM post",
			"SELECT * FROM t",
			"SELECT 'a > b'",
			"SELECT {nested} }",
			"SELECT $__host0 FROM t",
			"SELECT member",
		]
	);
	for query in &queries[1..] {
		assert!(matches!(query.source, HostQuerySource::Attribute { .. }), "{query:?}");
	}

	let interpolated = &queries[1];
	assert_eq!(
		&SVELTE[interpolated.host_range.clone()],
		"SELECT * FROM person WHERE age > {min} AND n = &quot;x&quot;"
	);
	assert_eq!(&SVELTE[interpolated.substitutions[0].host_range.clone()], "{min}");
	assert_eq!((interpolated.line, interpolated.column), (5, 11));

	// A `>` and braces in a quoted value, and nested braces and strings in an expression, do not
	// end the tag.
	assert_eq!(&SVELTE[queries[4].host_range.clone()], "SELECT 'a > b'");
	assert_eq!(&SVELTE[queries[5].host_range.clone()], "SELECT {nested} }");
	assert_eq!(&SVELTE[queries[6].substitutions[0].host_range.clone()], "${fn({ a: \"}\" })}");

	// The multi-line tag.
	assert_eq!(&SVELTE[queries[3].host_range.clone()], "SELECT * FROM t");
	assert_eq!((queries[3].line, queries[3].column), (8, 5));
}

#[test]
fn a_component_name_must_match_exactly() {
	let source =
		"<QueryList q=\"A\" /><MyQuery q=\"B\" /><Query q=\"C\" /><Query2 q=\"D\"></Query2>";
	assert_eq!(all_texts("p.svelte", source), ["C"]);
	assert_eq!(all_texts("p.vue", source), ["C"]);
	assert_eq!(all_texts("p.astro", source), ["C"]);
	let custom = Queries {
		components: &["QueryList"],
		..Queries::DEFAULT
	};
	let found = extract_with("p.svelte", source, &custom);
	assert_eq!(found.len(), 1);
	assert_eq!(found[0].text, "A");
}

#[test]
fn self_closing_and_paired_tags_both_read() {
	let source =
		"<Query q=\"A\"/><Query q=\"B\"></Query><Query q=\"C\">text</Query><Query\n\tq=\"D\"\n/>";
	assert_eq!(all_texts("p.svelte", source), ["A", "B", "C", "D"]);
}

#[test]
fn markup_attribute_ranges_and_positions_index_the_whole_file() {
	let source = "<p/>\n<Query q=\"SELECT 1\" />\n";
	let query = all_only("p.svelte", source);
	assert_eq!(query.host_range, 15..23);
	assert_eq!((query.line, query.column), (2, 11));
	assert_eq!(&source[query.host_range.clone()], query.text);
	assert!(query.keyable());

	let single = all_only("p.vue", "<template>\n  <Query q='SELECT 2' />\n</template>");
	assert_eq!((single.line, single.column), (2, 13));
	let empty = all_only("p.svelte", "<Query q=\"\" />");
	assert_eq!(empty.text, "");
	assert_eq!(empty.host_range, 10..10);
}

#[test]
fn html_references_decode_with_a_span_round_trip() {
	let source = "<Query q=\"a &amp; b &#x41; &lt;&#60; &#39;é&bogus; &#0; & &#xZZ;\" />";
	let query = all_only("p.vue", source);
	assert_eq!(query.text, "a & b A << 'é&bogus; &#0; & &#xZZ;");
	assert!(query.keyable());

	let ampersand = 2;
	assert_eq!(host_text(source, &query, ampersand..ampersand + 1), "&amp;");
	let referenced = source.find("&amp;").expect("reference present");
	assert_eq!(query.embed_offset(referenced), Some(ampersand));
	assert_eq!(query.embed_offset(referenced + 4), Some(ampersand));
	assert_eq!(query.host_offset(ampersand), referenced);

	let b = query.text.find('b').expect("b present");
	assert_eq!(&source[query.host_offset(b)..][..1], "b");
	let upper_a = query.text.find('A').expect("A present");
	assert_eq!(host_text(source, &query, upper_a..upper_a + 1), "&#x41;");
	let angle = query.text.find('<').expect("< present");
	assert_eq!(host_text(source, &query, angle..angle + 2), "&lt;&#60;");
	let quote = query.text.find('\'').expect("' present");
	assert_eq!(host_text(source, &query, quote..quote + 1), "&#39;");
	// Unknown and malformed references stay as written.
	let bogus = query.text.find("&bogus;").expect("bogus present");
	assert_eq!(host_text(source, &query, bogus..bogus + 7), "&bogus;");
	// Every byte of the contents is covered by the span map.
	for host in query.host_range.clone() {
		assert!(query.embed_offset(host).is_some(), "byte {host} is unmapped");
	}

	let jsx = "const q = <Query q=\"x &quot;y&quot;\" />;";
	let query = all_only("w.tsx", jsx);
	assert_eq!(query.text, "x \"y\"");
	assert_eq!(host_text(jsx, &query, 2..3), "&quot;");
}

#[test]
fn a_svelte_brace_inside_quotes_is_a_substitution() {
	let source = "<Query q=\"SELECT * FROM person WHERE name = '{name}' AND a = {a + 1}\" />";
	let query = all_only("p.svelte", source);
	assert_eq!(query.text, "SELECT * FROM person WHERE name = '$__host0' AND a = $__host1");
	assert!(!query.keyable());
	let [first, second] = query.substitutions.as_slice() else {
		panic!("expected two substitutions");
	};
	assert_eq!(&source[first.host_range.clone()], "{name}");
	assert_eq!(&source[second.host_range.clone()], "{a + 1}");
	assert_eq!(&query.text[first.text_range.clone()], "$__host0");
	assert_eq!(host_text(source, &query, first.text_range.clone()), "{name}");
	assert_eq!(query.embed_offset(first.host_range.start + 1), None);
	let quote = query.text.find("' AND").expect("quote present");
	assert_eq!(&source[query.host_offset(quote)..][..1], "'");

	// A quote inside the braces does not close the value; in Vue and Astro a brace is text.
	let nested = "<Query q=\"SELECT {fn(\"x\")} FROM t\" />";
	assert_eq!(all_only("p.svelte", nested).text, "SELECT $__host0 FROM t");
	assert_eq!(all_only("p.vue", "<Query q=\"SELECT {x} FROM t\" />").text, "SELECT {x} FROM t");
	assert_eq!(all_only("p.astro", "<Query q=\"SELECT {x} FROM t\" />").text, "SELECT {x} FROM t");
}

#[test]
fn vue_bound_attributes_are_javascript() {
	let source = "\
<template>
	<Query q=\"SELECT * FROM person WHERE a &lt; 1\" />
	<Query :q=\"`SELECT * FROM t WHERE id = ${id}`\" />
	<LiveQuery v-bind:q=\"'SELECT 2'\" />
	<Query :q=\"sql\" />
	<Query :q=\"'a' + b\" />
	<Query q=\"SELECT {{ not }} interpolated\" />
	<QueryList :q=\"'x'\" />
	<Query @q=\"'no'\" :other=\"'no'\" />
</template>
<script setup>
const r = useQuery(\"SELECT 3\");
</script>
";
	let queries = all("Bindings.vue", source);
	assert_eq!(
		queries.iter().map(|query| query.text.as_str()).collect::<Vec<_>>(),
		[
			"SELECT * FROM person WHERE a < 1",
			"SELECT * FROM t WHERE id = $__host0",
			"SELECT 2",
			"SELECT {{ not }} interpolated",
			"SELECT 3",
		]
	);
	let template = &queries[1];
	assert_eq!(&source[template.host_range.clone()], "SELECT * FROM t WHERE id = ${id}");
	assert_eq!(&source[template.substitutions[0].host_range.clone()], "${id}");
	assert_eq!(&source[queries[2].host_range.clone()], "SELECT 2");
	assert_eq!((queries[2].line, queries[2].column), (4, 24));
}

#[test]
fn astro_attributes_in_every_form() {
	let source = "\
---
const a = useQuery(\"SELECT frontmatter\");
const s = '<Query q=\"in a string\" />';
---
<Query q=\"SELECT * FROM person WHERE a &amp; b\" />
<Query q={\"SELECT braces\"} />
<Query q=`SELECT ${x} tpl` />
<ul>{items.map((i) => <LiveQuery q={`SELECT ${i}`} />)}</ul>
<Query q={sql} />
<Query q=sql />
<script>
	const live = createLive(\"SELECT script\");
</script>
";
	assert_eq!(
		all_texts("bindings.astro", source),
		[
			"SELECT frontmatter",
			"SELECT * FROM person WHERE a & b",
			"SELECT braces",
			"SELECT $__host0 tpl",
			"SELECT $__host0",
			"SELECT script",
		]
	);
	let queries = all("bindings.astro", source);
	assert_eq!(&source[queries[3].host_range.clone()], "SELECT ${x} tpl");
	assert_eq!(&source[queries[3].substitutions[0].host_range.clone()], "${x}");
	assert_eq!(&source[queries[2].host_range.clone()], "SELECT braces");
}

#[test]
fn script_style_and_comment_regions_are_not_markup() {
	let source = "\
<script>
	const s = '<Query q=\"in script\" />';
</script>
<SCRIPT lang=\"ts\">const t = '<Query q=\"in upper\" />';</SCRIPT>
<script type=\"text/template\"><Query q=\"in template script\" /></script>
<style>/* <Query q=\"in style\" /> */</style>
<!-- <Query q=\"in comment\" /> -->
<!-- unterminated comments do not matter after this one -->
<Query q=\"real\" />
";
	assert_eq!(all_texts("p.svelte", source), ["real"]);
	assert_eq!(all_texts("p.vue", source), ["real"]);
	assert_eq!(all_texts("p.astro", source), ["real"]);
	// An unterminated tag, quote or brace ends the scan of that tag, not of the file.
	assert!(all_texts("p.svelte", "<Query q=\"never closed />").is_empty());
	assert_eq!(all_texts("p.svelte", "<Query q=\"A\" /><Query q={ unclosed"), ["A"]);
}

#[test]
fn markup_is_not_read_without_components_or_in_other_hosts() {
	let source = "<Query q=\"SELECT 1\" />";
	assert!(extract("p.svelte", source, DEFAULT_METHODS).is_empty());
	assert!(all("p.ts", source).is_empty());
	let none = Queries {
		components: &[],
		..Queries::DEFAULT
	};
	assert!(extract_with("p.svelte", source, &none).is_empty());
}

#[test]
fn comments_include_javascript_comments_in_template_braces() {
	let svelte = "\
<!-- // not a comment -->
{/* surreal: allow(K9201) */}
<p>// text</p>
<Query q={ // line
	`SELECT 1` } title=\"{a /* inline */}\" />
{#if x}<b>{'// string'}</b>{/if}
<script>
// script
</script>
";
	let found: Vec<&str> =
		comments("p.svelte", svelte).into_iter().map(|range| &svelte[range]).collect();
	assert_eq!(found, ["/* surreal: allow(K9201) */", "// line", "/* inline */", "// script"]);

	let astro = "---\n// front\n---\n<p>{/* in astro */ x}</p>\n<!-- // html -->\n";
	assert_eq!(comment_texts("p.astro", astro), ["// front", "/* in astro */"]);
	// A Vue template has no `{…}` expressions here.
	let vue = "<template><p>{{ x /* in vue */ }}</p></template>\n<script>\n// s\n</script>\n";
	assert_eq!(comment_texts("c.vue", vue), ["// s"]);
}
