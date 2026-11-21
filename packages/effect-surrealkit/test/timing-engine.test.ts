import { Duration, Effect, Result, Schema } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import { bind, make as makeClient, timed } from '../src/client/index.js';
import { sdkTransport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] timing engine suite skipped: ${engineSkipReason}`,
	);
}

const read = make({
	name: 'read, answer',
	sql: 'NONE; 42;',
	params: {},
	result: Schema.Number,
	slots: 2,
	writes: false,
});

const readList = make({
	name: 'read, answer',
	sql: '[1, 2];',
	params: {},
	result: Schema.Array(Schema.Number),
	writes: false,
});

describe.skipIf(!engineConfigured)('timing on a disposable engine', () => {
	let engine: Engine;
	let retryChecks = 0;

	beforeAll(async () => {
		engine = await startEngine({
			retry: {
				attempts: 2,
				retryDelay: 1,
				retryDelayJitter: 0,
				retryable: (error) => {
					if (
						!(error instanceof Error) ||
						error.message !==
							'An error occurred: timing retry probe'
					)
						return false;
					retryChecks++;
					return true;
				},
			},
		});
		console.log(
			`[effect-surrealkit] timing: ${engine.mode} engine ${engine.version}`,
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('V3.1: collected run and atomic transaction have client timing only', async () => {
		const client = makeClient(engine.db);
		const [values, timings] = await Effect.runPromise(
			timed(
				Effect.all([
					client.run(read),
					client.transaction([bind(read), bind(readList)]),
				]),
			),
		);
		expect(values).toEqual([42, [42, [1, 2]]]);
		expect(timings).toMatchObject([
			{ kind: 'run', sends: 1, statements: [{ statement: read.name }] },
			{
				kind: 'transaction',
				sends: 1,
				statements: [
					{ statement: read.name },
					{ statement: readList.name, rows: 2 },
				],
			},
		]);
		for (const timing of timings) {
			expect(timing).not.toHaveProperty('server');
			expect(
				timing.statements.every(
					(part) => !Object.hasOwn(part, 'server'),
				),
			).toBe(true);
			expect(Duration.isPositive(timing.wait)).toBe(true);
		}
	});

	it.each([
		'statement',
		'transaction',
	] as const)('V3.1/V3.2: pipeline with each=%s sums the engine slot durations', async (each) => {
		const transport = sdkTransport(engine.db);
		const expected: Array<Duration.Duration> = [];
		const client = makeWith({
			...transport,
			send: async (request, signal) => {
				const answers = await transport.send(request, signal);
				for (const { offset, slots } of request.parts) {
					let duration = Duration.zero;
					for (const answer of answers.slice(
						offset,
						offset + slots,
					)) {
						expect(answer.server).toBeDefined();
						if (answer.server !== undefined)
							duration = Duration.sum(duration, answer.server);
					}
					expected.push(duration);
				}
				return answers;
			},
		});
		const [results, timings] = await Effect.runPromise(
			timed(client.pipeline([bind(read), bind(readList)], { each })),
		);
		expect(results).toEqual([Result.succeed(42), Result.succeed([1, 2])]);
		expect(timings).toMatchObject([
			{
				kind: 'pipeline',
				sends: 1,
				statements: [
					{ statement: read.name, server: expected[0] },
					{ statement: readList.name, server: expected[1], rows: 2 },
				],
			},
		]);
		const [timing] = timings;
		if (timing?.server === undefined)
			throw new Error('pipeline server duration is missing');
		expect(timing.server).toEqual(
			expected.reduce(
				(sum, duration) => Duration.sum(sum, duration),
				Duration.zero,
			),
		);
		console.log(
			`[effect-surrealkit] timing each=${each}: ${JSON.stringify(timing)}`,
		);
		if (engine.mode === 'embedded') {
			// Effect uses the nanosecond-resolution process clock in Node.
			const limit = Duration.sum(timing.wait, Duration.nanos(1n));
			expect(Duration.isGreaterThan(timing.server, limit)).toBe(false);
		}
	});

	it('counts one transport attempt when the SDK retries internally', async () => {
		await engine.db.query('CREATE ONLY timing_probe:counter SET n = 0;');
		const statement = make({
			name: 'sdk retry probe',
			sql: "UPDATE ONLY timing_probe:counter SET n += 1 RETURN NONE; THROW 'timing retry probe';",
			params: {},
			result: Schema.Undefined,
			throws: ['timing retry probe'],
			slots: 2,
			writes: true,
		});
		const client = makeClient(engine.db);
		const [outcome, timings] = await Effect.runPromise(
			timed(Effect.result(client.run(statement))),
		);
		expect(Result.isFailure(outcome)).toBe(true);
		expect(timings).toMatchObject([
			{ kind: 'run', name: statement.name, sends: 1 },
		]);
		expect(timings[0]).not.toHaveProperty('server');
		expect(retryChecks).toBe(3);
		expect(
			await engine.db.query(
				'SELECT VALUE n FROM ONLY timing_probe:counter;',
			),
		).toEqual([3]);
	});
});
