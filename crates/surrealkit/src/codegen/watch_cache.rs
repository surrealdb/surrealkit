//! The engine's per-query types, kept between the runs of `compile --watch`.
//!
//! The engine types each query on its own, so an answer stays right until the schema changes
//! or the query's own text does. [`QueryCache`] holds the answers, says which of the queries a
//! run has still to ask about ([`QueryCache::untyped`]), and rebuilds the [`TypesDocument`]
//! [`section::assemble`](super::section::assemble) reads, in [`Collected::inputs`] order, so the
//! result is what asking about every query again would give. It holds no engine and no files.

use std::collections::HashMap;

use super::document::{QueryTypes, TypesDocument};
use super::section::Collected;

/// What identifies an answer: the query's name (`None` for an inline query) and its text. A
/// named query whose body changed is a new key, so its old answer is never served for it.
type Key = (Option<String>, String);

fn key_of(name: Option<&String>, text: &str) -> Key {
	(name.cloned(), text.to_string())
}

/// The engine's answers for the queries of one schema.
#[derive(Debug, Clone, Default)]
pub struct QueryCache {
	answers: HashMap<Key, QueryTypes>,
	/// The engine's document without its `queries`, from the latest answer: what
	/// [`QueryCache::document`] puts them in.
	header: Option<TypesDocument>,
}

impl QueryCache {
	pub fn new() -> Self {
		Self::default()
	}

	/// Forget every answer. The types depend on the schema, so a schema change calls this.
	pub fn clear(&mut self) {
		self.answers.clear();
		self.header = None;
	}

	/// The number of answers held.
	pub fn len(&self) -> usize {
		self.answers.len()
	}

	pub fn is_empty(&self) -> bool {
		self.answers.is_empty()
	}

	/// The inputs of `collected` the cache has no answer for, as
	/// [`engine::types_document`](super::engine::types_document) takes them, in
	/// [`Collected::inputs`] order.
	pub fn untyped(&self, collected: &Collected) -> Vec<(Option<String>, String)> {
		collected
			.inputs()
			.into_iter()
			.filter(|(name, text)| !self.answers.contains_key(&key_of(name.as_ref(), text)))
			.collect()
	}

	/// Keep the answers in `document`: one per query it holds.
	pub fn insert(&mut self, mut document: TypesDocument) {
		for query in std::mem::take(&mut document.queries) {
			self.answers.insert(key_of(query.name.as_ref(), &query.text), query);
		}
		self.header = Some(document);
	}

	/// Drop the answers for queries `collected` no longer has.
	pub fn retain(&mut self, collected: &Collected) {
		let live: std::collections::HashSet<Key> = collected
			.inputs()
			.into_iter()
			.map(|(name, text)| key_of(name.as_ref(), &text))
			.collect();
		self.answers.retain(|key, _| live.contains(key));
	}

	/// The [`TypesDocument`] for `collected`: an answer for each input, in
	/// [`Collected::inputs`] order. `None` when the engine has never answered, which is also
	/// what asking it gives when it cannot type queries. An input with no answer is left out,
	/// as it is when the engine leaves one out.
	pub fn document(&self, collected: &Collected) -> Option<TypesDocument> {
		let mut document = self.header.clone()?;
		document.queries = collected
			.inputs()
			.into_iter()
			.filter_map(|(name, text)| self.answers.get(&key_of(name.as_ref(), &text)).cloned())
			.collect();
		Some(document)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::codegen::queries::{QueryOrigin, QuerySource};
	use crate::codegen::section::{CollectedQuery, assemble};
	use crate::typegen::Span;

	const BLOG: &str = include_str!("../../tests/fixtures/codegen/blog.json");

	/// What the engine answers for `inputs`: one of the fixture's typed queries, picked by the
	/// input's text, under the input's own name and text. A function of the input alone, as the
	/// engine's answer for a query is.
	fn engine(inputs: &[(Option<String>, String)]) -> TypesDocument {
		let blog = TypesDocument::from_json(BLOG).expect("the fixture");
		let mut document = TypesDocument {
			queries: Vec::new(),
			..blog.clone()
		};
		for (name, text) in inputs {
			let pick = text.bytes().map(usize::from).sum::<usize>() % blog.queries.len();
			let mut query = blog.queries[pick].clone();
			query.name = name.clone();
			query.text = text.clone();
			document.queries.push(query);
		}
		document
	}

	fn source(path: &str, start: usize) -> QuerySource {
		QuerySource {
			origin: QueryOrigin::Host,
			path: path.to_string(),
			span: Span {
				start,
				end: start + 5,
			},
			line: 1,
			column: 1,
			method: Some("query".to_string()),
		}
	}

	fn named(name: &str, text: &str) -> CollectedQuery {
		CollectedQuery {
			name: Some(name.to_string()),
			text: text.to_string(),
			sources: vec![QuerySource {
				origin: QueryOrigin::File,
				path: format!("queries/{name}.surql"),
				..source("", 0)
			}],
		}
	}

	fn inline(text: &str, path: &str, start: usize) -> CollectedQuery {
		CollectedQuery {
			name: None,
			text: text.to_string(),
			sources: vec![source(path, start)],
		}
	}

	fn collected(named: Vec<CollectedQuery>, inline: Vec<CollectedQuery>) -> Collected {
		Collected {
			named,
			inline,
			diagnostics: Vec::new(),
		}
	}

	/// What one run of the watch loop does against `cache`: ask for the untyped inputs only,
	/// and return the document it assembles and how many queries it asked about.
	fn incremental(cache: &mut QueryCache, now: &Collected) -> (usize, Option<TypesDocument>) {
		cache.retain(now);
		let untyped = cache.untyped(now);
		if !untyped.is_empty() {
			cache.insert(engine(&untyped));
		}
		(untyped.len(), cache.document(now))
	}

	fn fresh(now: &Collected) -> TypesDocument {
		engine(&now.inputs())
	}

	#[test]
	fn nothing_is_typed_until_asked_and_a_second_look_asks_for_nothing() {
		let now = collected(vec![named("A", "SELECT 1")], vec![inline("SELECT 2", "src/a.ts", 3)]);
		let mut cache = QueryCache::new();
		assert_eq!(cache.untyped(&now).len(), 2);
		assert!(cache.document(&now).is_none(), "the engine never answered");
		let (asked, document) = incremental(&mut cache, &now);
		assert_eq!(asked, 2);
		assert_eq!(document.expect("typed").queries.len(), 2);
		assert!(cache.untyped(&now).is_empty());
		assert_eq!(cache.len(), 2);
	}

	#[test]
	fn an_edited_query_is_asked_again_and_an_untouched_one_is_not() {
		let before = collected(
			vec![named("A", "SELECT 1")],
			vec![inline("SELECT 2", "src/a.ts", 3), inline("SELECT 3", "src/b.ts", 3)],
		);
		let mut cache = QueryCache::new();
		incremental(&mut cache, &before);
		// A named query's body changes, an inline one's text changes, and one stays.
		let after = collected(
			vec![named("A", "SELECT 10")],
			vec![inline("SELECT 20", "src/a.ts", 3), inline("SELECT 3", "src/b.ts", 40)],
		);
		let untyped = cache.untyped(&after);
		assert_eq!(
			untyped,
			[(Some("A".to_string()), "SELECT 10".to_string()), (None, "SELECT 20".to_string())]
		);
		// Where the unchanged query is written moved; its answer is still good.
		assert!(!untyped.iter().any(|(_, text)| text == "SELECT 3"));
	}

	#[test]
	fn an_answer_whose_query_is_gone_is_dropped() {
		let before =
			collected(vec![named("A", "SELECT 1")], vec![inline("SELECT 2", "src/a.ts", 3)]);
		let mut cache = QueryCache::new();
		incremental(&mut cache, &before);
		let after = collected(vec![], vec![inline("SELECT 2", "src/a.ts", 3)]);
		cache.retain(&after);
		assert_eq!(cache.len(), 1);
		let document = cache.document(&after).expect("typed");
		assert_eq!(document.queries.len(), 1);
		assert_eq!(document.queries[0].text, "SELECT 2");
	}

	#[test]
	fn clearing_forgets_everything() {
		let now = collected(vec![], vec![inline("SELECT 2", "src/a.ts", 3)]);
		let mut cache = QueryCache::new();
		incremental(&mut cache, &now);
		cache.clear();
		assert!(cache.is_empty());
		assert_eq!(cache.untyped(&now).len(), 1);
		assert!(cache.document(&now).is_none());
	}

	#[test]
	fn the_document_follows_the_order_of_the_inputs() {
		let now = collected(
			vec![named("B", "SELECT 1"), named("A", "SELECT 2")],
			vec![inline("SELECT 3", "src/a.ts", 3)],
		);
		let mut cache = QueryCache::new();
		let (_, document) = incremental(&mut cache, &now);
		let order: Vec<&str> =
			document.as_ref().expect("typed").queries.iter().map(|q| q.text.as_str()).collect();
		assert_eq!(order, ["SELECT 1", "SELECT 2", "SELECT 3"]);
	}

	/// The result of a sequence of edits, one run at a time against one cache, is each time what
	/// typing every query from scratch gives, and a run asks only about what changed.
	#[test]
	fn an_incremental_run_equals_a_fresh_one_over_a_sequence_of_edits() {
		let steps: Vec<(Collected, usize)> = vec![
			// Everything is new.
			(
				collected(
					vec![named("List", "SELECT * FROM user")],
					vec![inline("SELECT 1", "src/a.ts", 3), inline("SELECT 2", "src/b.ts", 9)],
				),
				3,
			),
			// A host file gains a call to a query that is already typed: it is one more source.
			(
				collected(
					vec![named("List", "SELECT * FROM user")],
					vec![
						{
							let mut q = inline("SELECT 1", "src/a.ts", 3);
							q.sources.push(source("src/c.ts", 1));
							q
						},
						inline("SELECT 2", "src/b.ts", 9),
					],
				),
				0,
			),
			// An inline query is edited.
			(
				collected(
					vec![named("List", "SELECT * FROM user")],
					vec![inline("SELECT 11", "src/a.ts", 3), inline("SELECT 2", "src/b.ts", 9)],
				),
				1,
			),
			// A named query's body is edited and an inline one is removed.
			(
				collected(
					vec![named("List", "SELECT name FROM user")],
					vec![inline("SELECT 11", "src/a.ts", 3)],
				),
				1,
			),
			// An inline query takes the text of a name: it is not sent, and the named one is.
			(
				collected(
					vec![named("SELECT 11", "SELECT 1")],
					vec![inline("SELECT 11", "src/a.ts", 3)],
				),
				1,
			),
			// Everything is removed.
			(collected(vec![], vec![]), 0),
			// The query comes back: asked about again, not remembered.
			(collected(vec![], vec![inline("SELECT 11", "src/a.ts", 3)]), 1),
		];
		let mut cache = QueryCache::new();
		for (index, (now, expected_asked)) in steps.iter().enumerate() {
			let (asked, document) = incremental(&mut cache, now);
			assert_eq!(asked, *expected_asked, "step {index}");
			let wanted = fresh(now);
			let got = document.map_or_else(Vec::new, |d| d.queries);
			assert_eq!(got, wanted.queries, "step {index}: the answers");
			let typed = TypesDocument {
				queries: got,
				..wanted.clone()
			};
			assert_eq!(
				assemble(now, Some(&typed)),
				assemble(now, Some(&wanted)),
				"step {index}: the assembled section"
			);
		}
	}

	#[test]
	fn an_engine_that_never_answers_leaves_no_document() {
		let now = collected(vec![], vec![inline("SELECT 2", "src/a.ts", 3)]);
		let cache = QueryCache::new();
		assert_eq!(cache.untyped(&now).len(), 1, "asked again on every run");
		assert!(cache.document(&now).is_none());
		assert_eq!(assemble(&now, None).0.len(), 0);
	}
}
