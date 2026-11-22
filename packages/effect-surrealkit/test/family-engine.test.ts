/**
 * Every variant of the fixture families, run through the client on a real
 * engine: each text parses and runs, an absent filter leaves rows in, a
 * present one takes them out, and the nearest-neighbour branch runs only in
 * the variants that choose it.
 */
import { Effect } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type Client, make as makeClient } from '../src/client/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { listings, summary } from './types/family.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] family engine suite skipped: ${engineSkipReason}`,
	);
}

const schema = `
DEFINE TABLE shelf SCHEMALESS;
DEFINE TABLE listing SCHEMALESS;
DEFINE ANALYZER words TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX listing_title ON listing FIELDS title FULLTEXT ANALYZER words BM25;
DEFINE INDEX listing_embedding ON listing FIELDS embedding HNSW DIMENSION 2 DIST EUCLIDEAN;
`;

const rows = `
CREATE shelf:main SET version = 1;
CREATE listing:a SET shelf = shelf:main, title = 'red lamp', price = 10, region = 'north', embedding = [0.0, 1.0], tags = [];
CREATE listing:b SET shelf = shelf:main, title = 'blue lamp', price = 50, region = 'south', embedding = [1.0, 0.0], tags = [];
`;

interface Found {
	readonly status: string;
	readonly data?: {
		readonly words: ReadonlyArray<{ readonly title: string }>;
		readonly near: ReadonlyArray<{ readonly title: string }>;
		readonly readiness: string;
	};
}

describe.skipIf(!engineConfigured)('a statement family on an engine', () => {
	let engine: Engine;
	let client: Client;

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		// The rows go in after the indexes are defined, in a request of their own.
		await within(Promise.resolve(engine.db.query(schema)));
		await within(Promise.resolve(engine.db.query(rows)));
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('runs every variant, each filtering only by what its key selects', async () => {
		for (const { key, statement } of listings.variants()) {
			const input = {
				tenant: 'main',
				terms: 'lamp',
				maxPrice: 20,
				region: 'north',
				vector: [0.0, 1.0],
			};
			const found = (await within(
				Effect.runPromise(client.run(statement, input as never)),
			)) as Found;
			expect(found.status).toBe('ready');
			const titles = (found.data?.words ?? [])
				.map(({ title }) => title)
				.sort();
			expect(titles, JSON.stringify(key)).toEqual(
				key.price || key.region
					? ['red lamp']
					: ['blue lamp', 'red lamp'],
			);
			expect(found.data?.readiness).toBe(key.vector ? 'ready' : 'empty');
			// For this prefilter on a local's field (`shelf = $shelf.id`), 3.3
			// returns the nearest neighbours and 4.0 returns an empty array.
			const near = (found.data?.near ?? [])
				.map(({ title }) => title)
				.sort();
			expect(near, JSON.stringify({ key, found })).toEqual(
				key.vector && engine.majorVersion === 3 ? titles : [],
			);
		}
	});

	it('runs a family whose fills read locals and define names', async () => {
		const found = (await within(
			Effect.runPromise(
				client.run(summary.select({}), { tenant: 'main', offset: 1 }),
			),
		)) as {
			readonly data: {
				readonly total: number;
				readonly titles: ReadonlyArray<string>;
			};
		};
		expect(found.data.total).toBe(2);
		expect([...found.data.titles].sort()).toEqual([
			'BLUE LAMP',
			'RED LAMP',
		]);
	});
});
