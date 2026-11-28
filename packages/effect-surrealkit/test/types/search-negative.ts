/**
 * Negative type fixtures for search: each `@ts-expect-error` must be an
 * actual error, and `test/diagnostics.test.ts` pins its message.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import {
	highlight,
	knn,
	linear,
	match,
	param,
	rrf,
	score,
	select,
} from '../../src/statement/index.js';
import { DocTable, PersonTable } from '../golden/corpus/index.js';

const terms = param('terms', SurrealSchema.String);

/** `doc` with a FULLTEXT index on `title` that keeps no highlights. */
const Plain = {
	...DocTable,
	indexes: {
		ft_title: {
			type: 'fulltext',
			columns: [['title']],
			fulltext: { analyzer: 'simple', highlights: false },
		},
	},
} as const;

export const rejectedMatches = (reference: number) => {
	// @ts-expect-error [search/match-without-fulltext-index] no FULLTEXT index covers `title`
	match(DocTable, 'title', terms, { ref: 1 });
	// @ts-expect-error [search/match-reference-out-of-range] SurrealDB parses references 0 to 255
	match(DocTable, 'body', terms, { ref: 256 });
	// @ts-expect-error [search/match-reference-not-literal] the reference is a literal, never a binding
	match(DocTable, 'body', terms, { ref: reference });
	// @ts-expect-error [search/match-terms-not-a-string] the terms are a string
	match(DocTable, 'body', param('n', SurrealSchema.Int), { ref: 1 });
};

export const rejectedScores = () => {
	const unnamed = match(DocTable, 'body', terms);
	// @ts-expect-error [search/score-without-reference] a score names its match by reference
	score(unnamed);
	const title = match(Plain, 'title', terms, { ref: 2 });
	// @ts-expect-error [search/highlight-without-highlights] the index keeps no HIGHLIGHTS
	highlight(title, '<b>', '</b>');
};

export const rejectedNeighbours = (count: number) => {
	// @ts-expect-error [search/knn-without-vector-index] the indexed form needs an HNSW or DISKANN index
	knn(DocTable, 'title', 'query', { k: 3, ef: 40 });
	// @ts-expect-error [search/knn-not-a-vector] a brute-force search needs an array of numbers
	knn(DocTable, 'title', 'query', { k: 3, distance: 'COSINE' });
	// @ts-expect-error [search/knn-pearson] SurrealDB 3.3 and 4.0 rank PEARSON differently
	knn(DocTable, 'embedding', 'query', { k: 3, distance: 'PEARSON' });
	// @ts-expect-error [search/knn-jaccard] SurrealDB 3.3 and 4.0 rank JACCARD differently
	knn(DocTable, 'embedding', 'query', { k: 3, distance: 'JACCARD' });
	// @ts-expect-error [search/knn-count-not-literal] the neighbour count is a literal
	knn(DocTable, 'embedding', 'query', { k: count, ef: 40 });
	// @ts-expect-error [search/knn-count-fractional] the neighbour count is a positive integer
	knn(DocTable, 'embedding', 'query', { k: 2.5, ef: 40 });
	// @ts-expect-error [search/knn-effort-zero] the search effort is a positive integer
	knn(DocTable, 'embedding', 'query', { k: 2, ef: 0 });
	const Three = Schema.Tuple([
		SurrealSchema.Float,
		SurrealSchema.Float,
		SurrealSchema.Float,
	]);
	// @ts-expect-error [search/knn-vector-dimension] the index dimension is 4
	knn(DocTable, 'embedding', param('three', Three), { k: 2, ef: 40 });
};

export const rejectedFusions = () => {
	const ids = select(DocTable)
		.project((row) => ({ id: row.id }))
		.toExpr();
	const titles = select(DocTable)
		.project((row) => ({ title: row.title }))
		.toExpr();
	// @ts-expect-error [search/fusion-list-without-id] fused rows are merged by `id`
	rrf([ids, titles], { limit: 10 });
	linear([ids, ids], {
		// @ts-expect-error [search/linear-weights-per-list] one weight per list
		weights: [1, 2, 3],
		limit: 10,
		normalisation: 'minmax',
	});
};

export const rejectedAliasOrders = () => {
	const fox = match(DocTable, 'body', 'fox', { ref: 1 });
	const scored = select(DocTable)
		.where(() => fox)
		.project((row) => ({ id: row.id, score: score(fox) }));
	// @ts-expect-error [search/order-by-unknown-alias] the projection has no `rank`
	scored.orderByAlias('rank');
	const titles = select(DocTable).value((row) => row.title);
	// @ts-expect-error [search/order-by-value-alias] SELECT VALUE has no names
	titles.orderByAlias('title');
	// A value that is an object has keys, but they are not names of the row.
	const addresses = select(PersonTable).value((row) => row.address);
	// @ts-expect-error [search/order-by-value-object-key] SELECT VALUE of an object has no names either
	addresses.orderByAlias('city');
	// @ts-expect-error [search/order-by-value-object-key-later] nor after another method
	addresses.limit(3).orderByAlias('city');
};
