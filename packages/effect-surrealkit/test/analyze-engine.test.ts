/**
 * A part the `statements` command generated, run through the client on a
 * real engine: the engine returns exactly the number of results the
 * analyzer derived, and its THROW surfaces as the declared typed error.
 */
import { Effect } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

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
