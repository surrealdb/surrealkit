/**
 * The analyzer checked against a real engine.
 *
 * Every complete authored fixture is sent once, with no bindings: the engine
 * accepts it without a parse error and returns exactly the number of results
 * the analyzer derived as `slots`. A part the `statements` command generated
 * is also run through the client, and its THROW surfaces as the declared
 * typed error.
 */
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join, relative } from 'node:path';

import { Effect } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { analyzeSync } from '../src/analyze/index.js';
import {
	type Client,
	make as makeClient,
	StatementThrown,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import { sequence } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { counter } from './golden/statements/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] analyzer engine suite skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('a generated part on an engine', () => {
	let engine: Engine;
	let client: Client;

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const step = sequence(counter.step).define({
		name: 'counter.step',
		params: { boundStart: SurrealSchema.Int },
		result: SurrealSchema.Int,
	});

	it('returns the derived number of results', async () => {
		expect(counter.step.slots).toBe(5);
		const results = await within(
			Promise.resolve(
				engine.db.query<unknown[]>(counter.step.sql, { boundStart: 1 }),
			),
		);
		expect(results).toHaveLength(counter.step.slots);
		// The client checks the count and decodes the last result.
		expect(
			await within(
				Effect.runPromise(client.run(step, { boundStart: 1 })),
			),
		).toBe(4);
	});

	it('maps the derived THROW message to a typed error', async () => {
		const thrown = await within(
			Effect.runPromise(
				Effect.flip(client.run(step, { boundStart: 100 })),
			),
		);
		expect(thrown).toBeInstanceOf(StatementThrown);
		expect(thrown).toMatchObject({
			statement: 'counter.step',
			reason: 'Too large',
		});
	});
});

const fixturesRoot = join(import.meta.dirname, 'fixtures');
const fixtureDirectories = [
	'statements',
	'statements-binds',
	'statements-presence',
];

const surqlFiles = (directory: string): ReadonlyArray<string> =>
	readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
		const path = join(directory, entry.name);
		if (entry.isDirectory()) return surqlFiles(path);
		return entry.name.endsWith('.surql') ? [path] : [];
	});

/** Every authored fixture, relative to `test/fixtures`, sorted. */
const fixtures = fixtureDirectories
	.flatMap((directory) => surqlFiles(join(fixturesRoot, directory)))
	.map((path) => relative(fixturesRoot, path))
	.sort();

/**
 * The fixtures that cannot be sent as they stand, each with its reason.
 * Every other fixture is a complete text and is sent to the engine.
 */
const notSendable: Readonly<Record<string, string>> = {
	'statements/accounts/listWhere.surql':
		'a template: `__FILTER__` is filled before it runs',
	'statements/accounts/ownerFilter.surql':
		'a fragment: a condition that only fills a hole',
	'statements/catalog/byVector.surql':
		'a template: `__WHERE__` and `__LIMIT__` are filled before it runs',
	'statements/catalog/byWords.surql':
		'a template: `__WHERE__` is filled before it runs',
	'statements/catalog/guarded.surql':
		'a template: `__BODY__` is filled before it runs',
	'statements/catalog/noRows.surql':
		'a fragment: an expression that only fills a hole',
	'statements/catalog/notReady.surql':
		'a fragment: an expression that only fills a hole',
	'statements/catalog/priceCap.surql':
		'a fragment: a condition that only fills a hole',
	'statements/catalog/readiness.surql':
		'a fragment: an expression that only fills a hole',
	'statements/catalog/regionIs.surql':
		'a fragment: a condition that only fills a hole',
	'statements/catalog/rowTitle.surql':
		'a fragment: an expression that only fills a hole',
	'statements/catalog/search.surql':
		'a template: `__WORDS__`, `__READINESS__` and `__NEAR__` are filled before it runs',
	'statements/catalog/summary.surql':
		'a template: `__COUNTS__` and `__TITLE__` are filled before it runs',
	'statements/catalog/visible.surql':
		'a template and a fragment: a condition with `__PRICE__` and `__REGION__`',
};

const analysed = fixtures.map((name) => {
	const analysis = analyzeSync(
		readFileSync(join(fixturesRoot, name), 'utf8'),
	);
	return {
		name,
		sql: analysis.sql,
		slots: analysis.slots,
		complete: analysis.kind === 'part' && analysis.holes.length === 0,
	};
});

const complete = analysed.filter((fixture) => fixture.complete);

describe('the authored fixtures', () => {
	it('names exactly the files that cannot be sent, each of which exists', () => {
		for (const name of Object.keys(notSendable)) {
			expect(existsSync(join(fixturesRoot, name)), name).toBe(true);
		}
		expect(
			analysed
				.filter((fixture) => !fixture.complete)
				.map((fixture) => fixture.name),
		).toEqual(Object.keys(notSendable).sort());
		expect(complete.length).toBeGreaterThan(0);
	});
});

/**
 * Sends a text once with no bindings and returns what disagrees with the
 * analyzer: a parse error or another number of results than `slots`.
 */
const resultCountProblem = async (
	engine: Engine,
	sql: string,
	slots: number,
): Promise<string | undefined> => {
	let responses: ReadonlyArray<unknown>;
	try {
		responses = await within(
			Promise.resolve(engine.db.query(sql).responses()),
		);
	} catch (error) {
		return `the engine refused the text: ${error instanceof Error ? error.message : String(error)}`;
	}
	return responses.length === slots
		? undefined
		: `the engine returned ${responses.length} results, the analyzer derived ${slots}`;
};

describe.skipIf(!engineConfigured)(
	'every complete fixture on an engine',
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
		});
		afterAll(async () => {
			await engine?.stop();
		});

		it.each(
			complete,
		)('$name returns the derived number of results', async ({
			sql,
			slots,
		}) => {
			expect(
				await resultCountProblem(engine, sql, slots),
			).toBeUndefined();
		});

		it('fails the comparison when a derived count is off by one', async () => {
			for (const { name, sql, slots } of complete) {
				for (const wrong of [slots - 1, slots + 1]) {
					if (wrong < 0) continue;
					expect(
						await resultCountProblem(engine, sql, wrong),
						`${name} with ${wrong} slots`,
					).toMatch(
						/^the engine returned \d+ results, the analyzer derived/u,
					);
				}
			}
		});
	},
);
