/**
 * Search statements as text and codecs, without an engine: what each
 * builder renders, which literals it refuses, and the references a
 * statement is refused for when it is built. The engine suite
 * (`search-engine.test.ts`) runs them.
 */
import { Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	and,
	block,
	eq,
	evaluate,
	highlight,
	knn,
	knnDistance,
	linear,
	match,
	not,
	offsets,
	or,
	param,
	rrf,
	score,
	select,
} from '../src/statement/index.js';
import { DocTable, PersonTable } from './golden/corpus/index.js';

const terms = param('terms', SurrealSchema.String);
const words = match(DocTable, 'body', terms, { ref: 1 });
const near = knn(DocTable, 'embedding', 'query', { k: 5, ef: 40 });

describe('full-text match', () => {
	it('renders the match, its score, highlights and offsets', () => {
		const statement = select(DocTable)
			.where(() => words)
			.project((row) => ({
				id: row.id,
				score: score(words),
				marked: highlight(words, '<b>', '</b>'),
				spans: offsets(words),
			}))
			.build('by words');
		expect(statement.sql).toBe(
			'SELECT id AS id, search::score(1) AS score, search::highlight(s"<b>", s"</b>", 1) AS marked, search::offsets(1) AS spans FROM ⟨doc⟩ WHERE body @1@ $terms;',
		);
		expect(Object.keys(statement.params)).toEqual(['terms']);
	});

	it('renders an operator, inline terms and a match without a reference', () => {
		expect(
			select(DocTable)
				.where(() =>
					match(DocTable, 'body', 'quick fox', {
						ref: 0,
						operator: 'AND',
					}),
				)
				.build().sql,
		).toBe('SELECT * FROM ⟨doc⟩ WHERE body @0,AND@ s"quick fox";');
		expect(
			select(DocTable)
				.where(() => match(DocTable, 'body', terms, { operator: 'OR' }))
				.build().sql,
		).toBe('SELECT * FROM ⟨doc⟩ WHERE body @OR@ $terms;');
		expect(
			select(DocTable)
				.where(() => match(DocTable, 'body', terms))
				.build().sql,
		).toBe('SELECT * FROM ⟨doc⟩ WHERE body @@ $terms;');
	});

	it('refuses a reference outside 0 to 255 and a field without a FULLTEXT index', () => {
		expect(() =>
			match(DocTable, 'body', terms, { ref: 256 as never }),
		).toThrow(/not a match reference/);
		expect(() =>
			match(DocTable, 'body', terms, { ref: 1.5 as never }),
		).toThrow(/not a match reference/);
		expect(() => match(DocTable, 'title' as never, terms)).toThrow(
			'`doc` has no FULLTEXT index on `title`',
		);
	});

	it('refuses a statement that reads a reference no match in it defines', () => {
		const other = match(DocTable, 'body', terms, { ref: 2 });
		const reading = select(DocTable)
			.where(() => words)
			.project((row) => ({ id: row.id, score: score(other) }));
		expect(() => reading.build('stray score')).toThrow(
			'statement `stray score` reads the score of match reference 2, which no match in it defines (it defines 1)',
		);
		expect(() =>
			select(DocTable)
				.project(() => ({ score: score(words) }))
				.build('no match'),
		).toThrow(/reads the score of match reference 1, which no match/);
	});

	it('keeps a subquery’s references inside it', () => {
		const inner = select(DocTable)
			.where(() => words)
			.project((row) => ({ id: row.id, score: score(words) }));
		const outer = select(DocTable)
			.where((row) => eq(row.title, 'x'))
			.project(() => ({ found: inner.toExpr() }));
		expect(() => outer.build('outer')).not.toThrow();
		const orphan = select(DocTable).project(() => ({
			score: score(words),
		}));
		expect(() => orphan.toExpr()).toThrow(
			/the subquery .* reads the score/,
		);
	});

	it('refuses one reference on two different fields', () => {
		const Twice = {
			...DocTable,
			indexes: {
				...DocTable.indexes,
				ft_title: {
					type: 'fulltext',
					columns: [['title']],
					fulltext: { analyzer: 'simple', highlights: false },
				},
			},
		} as const;
		const byTitle = match(Twice, 'title', terms, { ref: 1 });
		expect(() => and(words, byTitle)).toThrow(
			'match reference 1 is defined on both `body` and `title`; give each match its own reference number',
		);
		expect(() =>
			or(words, match(Twice, 'title', terms, { ref: 2 })),
		).not.toThrow();
	});
});

describe('nearest neighbours', () => {
	it('renders the indexed and the brute-force forms and the distance', () => {
		expect(
			select(DocTable)
				.where(() => near)
				.project((row) => ({ id: row.id, distance: knnDistance(near) }))
				.build().sql,
		).toBe(
			'SELECT id AS id, vector::distance::knn() AS distance FROM ⟨doc⟩ WHERE embedding <|5,40|> $query;',
		);
		const brute = knn(DocTable, 'embedding', 'query', {
			k: 3,
			distance: ['MINKOWSKI', 3],
		});
		expect(
			select(DocTable)
				.where(() => brute)
				.build().sql,
		).toBe('SELECT * FROM ⟨doc⟩ WHERE embedding <|3,MINKOWSKI 3|> $query;');
		expect(
			select(DocTable)
				.where(() =>
					knn(DocTable, 'embedding', 'query', {
						k: 3,
						distance: 'COSINE',
					}),
				)
				.build().sql,
		).toBe('SELECT * FROM ⟨doc⟩ WHERE embedding <|3,COSINE|> $query;');
	});

	it('refuses counts and distances the engine does not parse', () => {
		for (const k of [0, 1.5, -1, 4_294_967_296]) {
			expect(() =>
				knn(DocTable, 'embedding', 'q', { k: k as never, ef: 40 }),
			).toThrow(/the neighbour count takes a positive integer/);
		}
		expect(() =>
			knn(DocTable, 'embedding', 'q', { k: 2, ef: 0 as unknown as 40 }),
		).toThrow(/the search effort takes a positive integer/);
		expect(() =>
			knn(DocTable, 'embedding', 'q', {
				k: 2,
				distance: 'NEAR' as never,
			}),
		).toThrow(/is not a distance/);
		expect(() =>
			knn(DocTable, 'embedding', 'q', {
				k: 2,
				distance: ['MINKOWSKI', 0],
			}),
		).toThrow(/is not a distance/);
	});

	it('refuses JACCARD and PEARSON, whose distances and nearest rows differ between SurrealDB 3.3 and 4.0', () => {
		expect(() =>
			knn(DocTable, 'embedding', 'q', {
				k: 2,
				distance: 'PEARSON' as never,
			}),
		).toThrow(
			'PEARSON is refused: SurrealDB 3.3 answers the Pearson correlation itself as the distance, so its nearest rows are the least correlated, and 4.0 answers one minus it; the same search finds other rows on each',
		);
		expect(() =>
			knn(DocTable, 'embedding', 'q', {
				k: 2,
				distance: 'JACCARD' as never,
			}),
		).toThrow(
			'JACCARD is refused: SurrealDB 3.3 answers a measure of the sets of values (0 for most vectors of floats) and 4.0 one minus the weighted Jaccard similarity; the same search finds other rows on each',
		);
	});

	it('checks the query vector against the index dimension when encoding', () => {
		const statement = select(DocTable)
			.where(() => near)
			.build();
		const encode = Schema.encodeUnknownSync(statement.params.query);
		expect(encode([1, 0, 0, 0])).toEqual([1, 0, 0, 0]);
		expect(() => encode([1, 0, 0])).toThrow(/4/);
		const own = param('mine', Schema.Array(SurrealSchema.Float));
		const withOwn = select(DocTable)
			.where(() => knn(DocTable, 'embedding', own, { k: 1, ef: 10 }))
			.build();
		expect(() =>
			Schema.encodeUnknownSync(withOwn.params.mine)([1, 2]),
		).toThrow(/4/);
	});

	it('refuses a distance with no search, two searches, and a search under OR or NOT', () => {
		expect(() =>
			select(DocTable)
				.project(() => ({ distance: knnDistance(near) }))
				.build('lost'),
		).toThrow(
			'statement `lost` reads the nearest-neighbour distance (`vector::distance::knn()`) but makes no nearest-neighbour search',
		);
		const other = knn(DocTable, 'embedding', 'other', { k: 1, ef: 10 });
		expect(() =>
			select(DocTable)
				.where(() => near)
				.where(() => other),
		).toThrow(
			/a SELECT takes one nearest-neighbour search, and this one has 2/,
		);
		expect(() => or(near, eq(words, true))).toThrow(
			/is under OR; SurrealDB takes one only at the top level of WHERE/,
		);
		expect(() => not(near)).toThrow(/is under NOT/);
		expect(() => and(near, words)).not.toThrow();
	});

	it('takes a binding of its own only as a bare binding', () => {
		expect(() =>
			knn(DocTable, 'embedding', SurrealSchema.String as never, {
				k: 1,
				ef: 1,
			}),
		).toThrow();
	});
});

describe('fused rankings', () => {
	const meaning = select(DocTable)
		.where(() => near)
		.project((row) => ({ id: row.id, distance: knnDistance(near) }))
		.toExpr();
	const text = select(DocTable)
		.where(() => words)
		.project((row) => ({
			id: row.id,
			body: row.body,
			ft_score: score(words),
		}))
		.toExpr();

	it('renders rrf and linear', () => {
		const fused = evaluate(rrf([meaning, text], { limit: 10 }), 'fused');
		expect(fused.sql).toBe(
			'RETURN search::rrf([(SELECT id AS id, vector::distance::knn() AS distance FROM ⟨doc⟩ WHERE embedding <|5,40|> $query), (SELECT id AS id, body AS body, search::score(1) AS ft_score FROM ⟨doc⟩ WHERE body @1@ $terms)], 10, 60);',
		);
		expect(Object.keys(fused.params).sort()).toEqual(['query', 'terms']);
		expect(fused.writes).toBeUndefined();
		const weighted = evaluate(
			linear([meaning, text], {
				weights: [2, 1.5],
				limit: param('limit', SurrealSchema.Int),
				normalisation: 'zscore',
			}),
		);
		expect(weighted.sql).toBe(
			'RETURN search::linear([(SELECT id AS id, vector::distance::knn() AS distance FROM ⟨doc⟩ WHERE embedding <|5,40|> $query), (SELECT id AS id, body AS body, search::score(1) AS ft_score FROM ⟨doc⟩ WHERE body @1@ $terms)], [ 2, 1.5 ], $limit, s"zscore");',
		);
	});

	it('decodes fused rows strictly: an id, every list field optional, and the score', () => {
		const fused = evaluate(rrf([meaning, text], { limit: 3, k: 0 }));
		const decode = Schema.decodeUnknownSync(fused.result);
		const id = new RecordId('doc', 'a');
		expect(
			decode([
				{ id, distance: 0, rrf_score: 1 },
				{
					id,
					body: 'fox',
					ft_score: 0.5,
					distance: undefined,
					rrf_score: 0.5,
				},
			]),
		).toEqual([
			{ id, distance: 0, rrf_score: 1 },
			{ id, body: 'fox', ft_score: 0.5, rrf_score: 0.5 },
		]);
		expect(() => decode([{ distance: 0, rrf_score: 1 }])).toThrow();
		expect(() => decode([{ id, distance: 0 }])).toThrow();
		expect(() => decode([{ id, body: 3, rrf_score: 1 }])).toThrow();
	});

	it('refuses bad limits, weights and lists', () => {
		expect(() => rrf([meaning, text], { limit: 0 })).toThrow(
			/the fused limit takes an integer of at least 1/,
		);
		expect(() =>
			linear([meaning, text], {
				weights: [1] as never,
				limit: 2,
				normalisation: 'minmax',
			}),
		).toThrow('there are 2 lists and 1 weights; give one weight per list');
		const titles = select(DocTable)
			.project((row) => ({ title: row.title }))
			.toExpr();
		expect(() => rrf([meaning, titles as never], { limit: 2 })).toThrow(
			/has rows without an `id`/,
		);
		const scored = select(DocTable)
			.project((row) => ({ id: row.id, rrf_score: row.title }))
			.toExpr();
		expect(() => rrf([meaning, scored], { limit: 2 })).toThrow(
			/a fused list has a field `rrf_score`/,
		);
	});
});

describe('searches in a block', () => {
	it('binds one search per statement, each resolved on its own, so two nearest-neighbour searches in two locals are no conflict', () => {
		const other = knn(DocTable, 'embedding', 'second', { k: 3, ef: 40 });
		const statement = block()
			.let('words', () =>
				select(DocTable)
					.where(() => words)
					.project((row) => ({ id: row.id, score: score(words) }))
					.build(),
			)
			.let('near', () =>
				select(DocTable)
					.where(() => near)
					.project((row) => ({
						id: row.id,
						distance: knnDistance(near),
					}))
					.build(),
			)
			.let('other', () =>
				select(DocTable)
					.where(() => other)
					.project((row) => ({
						id: row.id,
						distance: knnDistance(other),
					}))
					.build(),
			)
			.return(($) => ({ words: $.words, near: $.near, other: $.other }))
			.build('three searches');
		expect(statement.sql).toContain('WHERE embedding <|5,40|> $query');
		expect(statement.sql).toContain('WHERE embedding <|3,40|> $second');
		expect(Object.keys(statement.params).sort()).toEqual([
			'query',
			'second',
			'terms',
		]);
		const built = select(DocTable)
			.where(() => near)
			.project((row) => ({ id: row.id, distance: knnDistance(near) }))
			.build();
		expect(Object.getOwnPropertySymbols(built.params)).toEqual([]);
	});
});

describe('ordering by a score or a distance', () => {
	it('sorts by an alias of the projection, after or before a field path', () => {
		const statement = select(DocTable)
			.where(() => and(words, near))
			.project((row) => ({
				id: row.id,
				title: row.title,
				score: score(words),
				distance: knnDistance(near),
			}))
			.orderByAlias('score', 'DESC')
			.orderByAlias('distance')
			.orderBy((row) => row.title)
			.limit(3)
			.build('hybrid');
		expect(statement.sql).toBe(
			'SELECT id AS id, title AS title, search::score(1) AS score, vector::distance::knn() AS distance FROM ⟨doc⟩ WHERE body @1@ $terms AND embedding <|5,40|> $query ORDER BY score DESC, distance ASC, title ASC LIMIT 3;',
		);
		expect(select(DocTable).orderByAlias('title', 'DESC').build().sql).toBe(
			'SELECT * FROM ⟨doc⟩ ORDER BY title DESC;',
		);
	});

	it('refuses a name the result row does not hold', () => {
		const untyped = (f: unknown) =>
			f as (...args: ReadonlyArray<unknown>) => unknown;
		const scored = select(DocTable)
			.where(() => words)
			.project((row) => ({ id: row.id, score: score(words) }));
		expect(() => untyped(scored.orderByAlias)('title')).toThrow(
			'`title` is not a name the result row holds',
		);
		expect(() =>
			untyped(select(DocTable).omit('body').orderByAlias)('body'),
		).toThrow('`body` is not a name the result row holds');
		expect(() =>
			untyped(select(DocTable).value((row) => row.title).orderByAlias)(
				'title',
			),
		).toThrow('`title` is not a name the result row holds');
	});

	it('checks each alias against what the finished statement returns, so a later projection cannot drop it', () => {
		const untyped = (f: unknown) =>
			f as (...args: ReadonlyArray<unknown>) => unknown;
		// The alias is checked again when the statement is built or used.
		const dropped = select(DocTable)
			.orderByAlias('title')
			.project((row) => ({ id: row.id }));
		expect(() => dropped.build()).toThrow(
			'ORDER BY `title`: `title` is not a name the result row holds',
		);
		expect(() => dropped.toExpr()).toThrow(
			'ORDER BY `title`: `title` is not a name the result row holds',
		);
		expect(() =>
			select(DocTable).orderByAlias('title').omit('title').build(),
		).toThrow(
			'ORDER BY `title`: `title` is not a name the result row holds',
		);
		expect(() =>
			select(DocTable)
				.orderByAlias('title')
				.value((row) => row.title)
				.build(),
		).toThrow(/SELECT VALUE returns one value per row, with no names/);
		// A projection that keeps the name keeps the sort key.
		expect(
			select(DocTable)
				.orderByAlias('title', 'DESC')
				.project((row) => ({ id: row.id, title: row.title }))
				.build().sql,
		).toBe(
			'SELECT id AS id, title AS title FROM ⟨doc⟩ ORDER BY title DESC;',
		);
		// SELECT VALUE of an object holds its keys, but they are no aliases.
		expect(() =>
			untyped(
				select(PersonTable).value((row) => row.address).orderByAlias,
			)('city'),
		).toThrow(
			'`city` is not a name the result row holds: SELECT VALUE returns one value per row, with no names to order by; order by a field path of the stored row with `orderBy`',
		);
	});
});
