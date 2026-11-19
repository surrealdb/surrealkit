/**
 * Positive type fixtures for search: matches, scores, nearest neighbours
 * and fused rankings over the corpus `doc` table, whose descriptor carries
 * a FULLTEXT index on `body` and HNSW and DISKANN indexes on `embedding`.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import {
	and,
	evaluate,
	highlight,
	knn,
	knnDistance,
	linear,
	match,
	offsets,
	param,
	rrf,
	score,
	select,
} from '../../src/statement/index.js';
import { DocTable } from '../golden/corpus/index.js';

type Exact<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;

const words = match(DocTable, 'body', param('terms', SurrealSchema.String), {
	ref: 1,
});

export const byWords = select(DocTable)
	.where(() => words)
	.project((row) => ({
		id: row.id,
		title: row.title,
		score: score(words),
		marked: highlight(words, '<b>', '</b>'),
		spans: offsets(words),
	}))
	.build('by words');

const wordsInput: Exact<(typeof byWords.params.terms)['Type'], string> = true;
type WordsRow = (typeof byWords.result.Type)[number];
const wordsRow: Exact<
	WordsRow,
	{
		readonly id: typeof DocTable.id.Type;
		readonly title: string;
		readonly score: number;
		readonly marked: string;
		readonly spans:
			| {
					readonly [x: string]: ReadonlyArray<{
						readonly s: number;
						readonly e: number;
					}>;
			  }
			| undefined;
	}
> = true;

export const plainMatch = select(DocTable)
	.where(() => match(DocTable, 'body', 'fox', { operator: 'AND' }))
	.build('plain match');

const near = knn(DocTable, 'embedding', 'query', { k: 5, ef: 40 });
export const byMeaning = select(DocTable)
	.where(() => near)
	.project((row) => ({ id: row.id, distance: knnDistance(near) }))
	.build('by meaning');
const meaningInput: Exact<
	(typeof byMeaning.params.query)['Type'],
	ReadonlyArray<number>
> = true;

export const bruteForce = select(DocTable)
	.where(() =>
		knn(DocTable, 'embedding', 'query', {
			k: 3,
			distance: ['MINKOWSKI', 3],
		}),
	)
	.build('brute force');

const Four = Schema.Tuple([
	SurrealSchema.Float,
	SurrealSchema.Float,
	SurrealSchema.Float,
	SurrealSchema.Float,
]);
export const tupleVector = knn(DocTable, 'embedding', param('four', Four), {
	k: 2,
	ef: 10,
});

const meaning = select(DocTable)
	.where(() => near)
	.project((row) => ({ id: row.id, distance: knnDistance(near) }))
	.toExpr();
const text = select(DocTable)
	.where(() => words)
	.project((row) => ({ id: row.id, body: row.body, ft_score: score(words) }))
	.toExpr();

export const fused = evaluate(rrf([meaning, text], { limit: 10 }), 'fused');
type FusedRowType = (typeof fused.result.Type)[number];
const fusedRow: Exact<
	FusedRowType,
	{
		readonly id: typeof DocTable.id.Type;
		readonly distance?: number;
		readonly body?: string;
		readonly ft_score?: number;
		readonly rrf_score: number;
	}
> = true;
const fusedInput: Exact<keyof typeof fused.params, 'query' | 'terms'> = true;

export const weighted = evaluate(
	linear([meaning, text], {
		weights: [2, 1],
		limit: 5,
		normalisation: 'minmax',
	}),
);
const weightedRow: Exact<
	(typeof weighted.result.Type)[number]['linear_score'],
	number
> = true;

export const shownRow = null as unknown as WordsRow;
void [wordsInput, wordsRow, meaningInput, fusedRow, fusedInput, weightedRow];

/** A hybrid search ordered by its score, then its distance. */
export const hybrid = select(DocTable)
	.where(() => and(words, near))
	.project((row) => ({
		id: row.id,
		score: score(words),
		distance: knnDistance(near),
	}))
	.orderByAlias('score', 'DESC')
	.orderByAlias('distance')
	.limit(3)
	.build('hybrid');
const hybridRow: Exact<
	(typeof hybrid.result.Type)[number],
	{
		readonly id: typeof DocTable.id.Type;
		readonly score: number;
		readonly distance: number;
	}
> = true;
void hybridRow;
