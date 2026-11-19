/**
 * Search statements on a disposable engine: the corpus schema (its `doc`
 * table has a FULLTEXT index on `body` and HNSW and DISKANN indexes on
 * `embedding`) with statements built from the golden `DocTable`. Every
 * result decodes strictly with the codec its statement derived, and the
 * testing subpath's `expectIndex` proves from the engine's plan that each
 * search reads its index. What each engine does without one is pinned in
 * `conformance.test.ts`.
 */
import { readFileSync } from 'node:fs';

import { Effect, type Layer } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { run, type SurrealClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	and,
	type Distance,
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
} from '../src/statement/index.js';
import {
	expectIndex,
	expectNoIndex,
	explain,
	layerFrom,
	type TestDatabase,
} from '../src/testing/index.js';
import { bestMatch } from './blocks.js';
import { corpusPath, readJson } from './documents.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { DocTable } from './golden/corpus/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] search engine suite skipped: ${engineSkipReason}`,
	);
}

interface CorpusModule {
	readonly path: string;
	readonly engines?: ReadonlyArray<string>;
	readonly experimental?: ReadonlyArray<string>;
}

/** Applies the corpus modules meant for this engine, as its manifest says. */
const applyCorpus = async (engine: Engine): Promise<void> => {
	const minor = /(\d+\.\d+)\./u.exec(engine.version)?.[1] ?? '';
	const { modules } = readJson(corpusPath('corpus.json')) as unknown as {
		readonly modules: ReadonlyArray<CorpusModule>;
	};
	for (const module of modules) {
		if (module.experimental !== undefined) continue;
		if (module.engines !== undefined && !module.engines.includes(minor)) {
			continue;
		}
		await engine.db.query(readFileSync(corpusPath(module.path), 'utf8'));
	}
};

const words = match(DocTable, 'body', param('terms', SurrealSchema.String), {
	ref: 1,
});
const near = knn(DocTable, 'embedding', 'query', { k: 2, ef: 40 });

const byWords = select(DocTable)
	.where(() => words)
	.project((row) => ({
		id: row.id,
		score: score(words),
		marked: highlight(words, '[', ']'),
		spans: offsets(words),
	}))
	.build('by words');
const byMeaning = select(DocTable)
	.where(() => near)
	.project((row) => ({ id: row.id, distance: knnDistance(near) }))
	.build('by meaning');
const bruteForce = select(DocTable)
	.where(() =>
		knn(DocTable, 'embedding', 'query', { k: 2, distance: 'EUCLIDEAN' }),
	)
	.project((row) => ({ id: row.id }))
	.build('brute force');

const meaningList = select(DocTable)
	.where(() => near)
	.project((row) => ({
		id: row.id,
		title: row.title,
		distance: knnDistance(near),
	}))
	.toExpr();
const wordsList = select(DocTable)
	.where(() => words)
	.project((row) => ({ id: row.id, body: row.body, ft_score: score(words) }))
	.toExpr();
const fused = evaluate(rrf([meaningList, wordsList], { limit: 10 }), 'rrf');
const weighted = evaluate(
	linear([meaningList, wordsList], {
		weights: [1, 1],
		limit: 10,
		normalisation: 'minmax',
	}),
	'linear',
);

describe.skipIf(!engineConfigured)('search on a disposable engine', () => {
	let engine: Engine;
	let layer: Layer.Layer<TestDatabase | SurrealClient>;

	const runHere = <A, E>(
		effect: Effect.Effect<A, E, TestDatabase | SurrealClient>,
	): Promise<A> =>
		within(Effect.runPromise(effect.pipe(Effect.provide(layer))), 20_000);

	beforeAll(async () => {
		engine = await startEngine();
		await applyCorpus(engine);
		const filler = [0, 1, 2, 3, 4, 5].map(
			(i) =>
				`CREATE doc:f${i} SET title = 'filler ${i}', body = 'common words only', code = 'f${i}', owner = person:owner, embedding = [0.0, 0.0, 1.0, ${i}.0];`,
		);
		await engine.db.query(
			[
				"CREATE person:owner SET name = 'Owner', email = 'owner@example.com', bio = NULL, score = 1, address = { city: 'Leeds' };",
				...filler,
				"CREATE doc:fox SET title = 'fox', body = 'the quick fox and another fox', code = 'fox', owner = person:owner, embedding = [1.0, 0.0, 0.0, 0.0];",
				"CREATE doc:dog SET title = 'dog', body = 'a quick dog', code = 'dog', owner = person:owner, embedding = [0.9, 0.1, 0.0, 0.0];",
			].join('\n'),
		);
		layer = layerFrom(engine.db, {
			namespace: engine.namespace,
			database: engine.namespace,
		});
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('a match finds, scores, highlights and locates the terms', async () => {
		const [hit, ...rest] = await runHere(run(byWords, { terms: 'fox' }));
		expect(rest).toEqual([]);
		expect(hit?.id.id).toBe('fox');
		expect(hit?.score).toBeGreaterThan(0);
		expect(hit?.marked).toBe('the quick [fox] and another [fox]');
		expect(hit?.spans).toEqual({
			0: [
				{ s: 10, e: 13 },
				{ s: 26, e: 29 },
			],
		});
	});

	it('the match reads its FULLTEXT index', async () => {
		await runHere(expectIndex(byWords, { terms: 'fox' }, 'ft_body'));
	});

	it('the nearest neighbours come with their distance, through a vector index', async () => {
		const rows = await runHere(run(byMeaning, { query: [1, 0, 0, 0] }));
		expect(rows.map((row) => row.id.id)).toEqual(['fox', 'dog']);
		expect(rows[0]?.distance).toBe(0);
		expect(rows[1]?.distance).toBeGreaterThan(0);
		const { indexes } = await runHere(
			explain(byMeaning, { query: [1, 0, 0, 0] }),
		);
		// The corpus defines both an HNSW and a DISKANN index on the field;
		// the planner reads one of them.
		expect(indexes).toHaveLength(1);
		expect(['hnsw_embedding', 'diskann_embedding']).toContain(indexes[0]);
		await runHere(
			expectIndex(byMeaning, { query: [1, 0, 0, 0] }, indexes[0] ?? ''),
		);
	});

	it('the brute-force search reads no index', async () => {
		const rows = await runHere(run(bruteForce, { query: [1, 0, 0, 0] }));
		expect(rows.map((row) => row.id.id)).toEqual(['fox', 'dog']);
		for (const index of ['hnsw_embedding', 'diskann_embedding']) {
			await runHere(
				expectNoIndex(bruteForce, { query: [1, 0, 0, 0] }, index),
			);
		}
	});

	/** Each brute-force distance as SurrealDB 3.3 and 4.0 both compute it. */
	const formulas: ReadonlyArray<
		readonly [
			Distance,
			(a: ReadonlyArray<number>, b: ReadonlyArray<number>) => number,
		]
	> = (() => {
		const diffs = (a: ReadonlyArray<number>, b: ReadonlyArray<number>) =>
			a.map((x, i) => Math.abs(x - (b[i] ?? 0)));
		const dot = (a: ReadonlyArray<number>, b: ReadonlyArray<number>) =>
			a.reduce((sum, x, i) => sum + x * (b[i] ?? 0), 0);
		const norm = (a: ReadonlyArray<number>) => Math.sqrt(dot(a, a));
		return [
			['EUCLIDEAN', (a, b) => Math.hypot(...diffs(a, b))],
			['MANHATTAN', (a, b) => diffs(a, b).reduce((x, y) => x + y, 0)],
			['CHEBYSHEV', (a, b) => Math.max(...diffs(a, b))],
			[
				['MINKOWSKI', 3],
				(a, b) =>
					diffs(a, b).reduce((x, y) => x + y ** 3, 0) ** (1 / 3),
			],
			['HAMMING', (a, b) => diffs(a, b).filter((d) => d !== 0).length],
			['COSINE', (a, b) => 1 - dot(a, b) / (norm(a) * norm(b))],
			// Assumes unit vectors: one minus the dot product.
			['COSINE_NORMALIZED', (a, b) => 1 - dot(a, b)],
			['INNER_PRODUCT', (a, b) => -dot(a, b)],
		];
	})();

	it.each(
		formulas.map(([distance, formula]) => [
			JSON.stringify(distance),
			distance,
			formula,
		]),
	)('a brute-force %s search finds the nearest rows by the distance both engines compute', async (_, distance, formula) => {
		for (const query of [
			[0.8, 0.15, 0.45, 1.3],
			[0.9, 0.1, 0.0, 0.0],
		]) {
			const nearest = (
				hit: ReturnType<
					typeof knn<typeof DocTable, 'embedding', 'query', number>
				>,
			) =>
				select(DocTable)
					.where(() => hit)
					.project((row) => ({
						id: row.id,
						embedding: row.embedding,
						distance: knnDistance(hit),
					}))
					.build();
			const all = await runHere(
				run(
					nearest(
						knn(DocTable, 'embedding', 'query', { k: 8, distance }),
					),
					{ query },
				),
			);
			expect(all).toHaveLength(8);
			for (const row of all) {
				expect(row.distance).toBeCloseTo(
					formula(row.embedding, query),
					9,
				);
			}
			// The three nearest are the three least distant, when no tie
			// straddles the third.
			const sorted = all.map((row) => row.distance).sort((a, b) => a - b);
			const three = await runHere(
				run(
					nearest(
						knn(DocTable, 'embedding', 'query', { k: 3, distance }),
					),
					{ query },
				),
			);
			expect(three).toHaveLength(3);
			const third = sorted[2] ?? Number.NaN;
			if (third < (sorted[3] ?? Number.POSITIVE_INFINITY) - 1e-12) {
				expect(three.map((row) => row.id.id).sort()).toEqual(
					all
						.filter((row) => row.distance <= third + 1e-12)
						.map((row) => row.id.id)
						.sort(),
				);
			} else {
				for (const row of three) {
					expect(row.distance).toBeLessThanOrEqual(third + 1e-12);
				}
			}
		}
	});

	it('a query vector of the wrong length is refused before it is sent', async () => {
		const error = await runHere(
			Effect.flip(run(byMeaning, { query: [1, 0, 0] })),
		);
		expect(error._tag).toBe('StatementEncodeError');
	});

	it('orders by a score alias and a distance alias, either way, and a hybrid search by its score', async () => {
		const scored = (direction: 'ASC' | 'DESC') =>
			select(DocTable)
				.where(() => words)
				.project((row) => ({ id: row.id, score: score(words) }))
				.orderByAlias('score', direction)
				.build();
		const down = await runHere(run(scored('DESC'), { terms: 'quick' }));
		const up = await runHere(run(scored('ASC'), { terms: 'quick' }));
		const scores = down.map((row) => row.score);
		expect(scores).toHaveLength(2);
		expect(scores[0]).toBeGreaterThan(
			scores[1] ?? Number.POSITIVE_INFINITY,
		);
		expect(up.map((row) => row.id.id)).toEqual(
			down.map((row) => row.id.id).reverse(),
		);
		const far = select(DocTable)
			.where(() => near)
			.project((row) => ({ id: row.id, distance: knnDistance(near) }))
			.orderByAlias('distance', 'DESC')
			.build();
		const distant = await runHere(run(far, { query: [1, 0, 0, 0] }));
		expect(distant.map((row) => row.id.id)).toEqual(['dog', 'fox']);
		const hybrid = select(DocTable)
			.where(() => and(words, near))
			.project((row) => ({
				id: row.id,
				score: score(words),
				distance: knnDistance(near),
			}))
			.orderByAlias('score', 'DESC')
			.build();
		const hits = await runHere(
			run(hybrid, { terms: 'quick', query: [1, 0, 0, 0] }),
		);
		expect(hits.map((row) => row.id.id).sort()).toEqual(['dog', 'fox']);
		expect(hits.map((row) => row.id.id)).toEqual(
			down.map((row) => row.id.id),
		);
		for (const hit of hits) expect(hit.distance).toBeTypeOf('number');
	});

	it('runs the README example: a hybrid search in a block, ordered by its score, with functions and a link', async () => {
		const found = await runHere(
			run(bestMatch, { terms: 'quick', query: [1, 0, 0, 0] }),
		);
		expect(found.matches).toBe(2);
		expect(found.best?.owner).toBe('Owner');
		expect(['FOX', 'DOG']).toContain(found.best?.title);
		const ordered = await runHere(
			run(
				select(DocTable)
					.where(() => words)
					.project((row) => ({ id: row.id, score: score(words) }))
					.orderByAlias('score', 'DESC')
					.build(),
				{ terms: 'quick' },
			),
		);
		expect(found.best?.id).toEqual(ordered[0]?.id);
		const none = await runHere(
			Effect.flip(
				run(bestMatch, { terms: 'zebra', query: [1, 0, 0, 0] }),
			),
		);
		expect(none).toMatchObject({ message: 'No match' });
	});

	it('rrf and linear fuse the lists, rows merged by id, with their score', async () => {
		const input = { query: [1, 0, 0, 0], terms: 'quick' };
		const ranked = await runHere(run(fused, input));
		expect(ranked.map((row) => row.id.id).sort()).toEqual(['dog', 'fox']);
		for (const row of ranked) {
			expect(row.rrf_score).toBeGreaterThan(0);
			expect(row.title).toBeTypeOf('string');
			expect(row.body).toBeTypeOf('string');
			expect(row.distance).toBeTypeOf('number');
			expect(row.ft_score).toBeTypeOf('number');
		}
		const linearly = await runHere(run(weighted, input));
		expect(linearly.map((row) => row.id.id).sort()).toEqual(['dog', 'fox']);
		for (const row of linearly) {
			expect(row.linear_score).toBeTypeOf('number');
		}
	});
});
