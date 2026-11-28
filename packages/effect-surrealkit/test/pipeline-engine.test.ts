import { setTimeout as delay } from 'node:timers/promises';
import { Effect, Result, Schema } from 'effect';
import {
	afterAll,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
	vi,
} from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	bind,
	make as makeClient,
	type Queryable,
	StatementConflict,
	StatementRequestError,
	StatementThrown,
	StatementWriteOutcomeError,
} from '../src/client/index.js';
import {
	type SlotAnswer,
	sdkTransport,
} from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import { conflictingSession } from '../src/testing/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] pipeline engine suite skipped: ${engineSkipReason}`,
	);
}

const first = make({
	name: 'write before',
	sql: 'UPSERT pipeline_row:first SET n = 1 RETURN NONE;',
	params: {},
	result: Schema.Tuple([]),
	writes: true,
});
const middle = make({
	name: 'write then throw',
	sql: "UPSERT pipeline_row:middle SET n = 2 RETURN NONE; THROW 'middle'; NONE;",
	params: {},
	result: Schema.Undefined,
	slots: 3,
	throws: ['middle'],
	throwSites: { middle: { source: 'pipeline/middle.surql', lines: [2] } },
	writes: true,
});
const last = make({
	name: 'write after',
	sql: 'UPSERT pipeline_row:last SET n = 3 RETURN NONE;',
	params: {},
	result: Schema.Tuple([]),
	writes: true,
});
const beforeConflict = make({
	...first,
	sql: 'UPDATE ONLY pipeline_left:one SET n += 1 RETURN VALUE n;',
	result: Schema.Number,
});
const afterConflict = make({
	...last,
	sql: 'UPDATE ONLY pipeline_right:one SET n += 1 RETURN VALUE n;',
	result: Schema.Number,
});
const writeUntilConflict = make({
	name: 'write until conflict',
	sql: 'LET $locked = (SELECT * FROM ONLY pipeline_lock:held FOR UPDATE); UPSERT pipeline_middle:one SET n = $locked.n RETURN NONE; SLEEP 1s;',
	defines: ['locked'],
	params: {},
	result: Schema.Undefined,
	slots: 3,
	writes: true,
});

const run = <A, E>(effect: Effect.Effect<A, E>) =>
	within(Effect.runPromise(effect), 20_000);

const failedAs = <A, E>(result: Result.Result<A, E>) => {
	expect(Result.isFailure(result)).toBe(true);
	return Result.isFailure(result) ? result.failure : undefined;
};

const waitForFirstCommit = async (engine: Engine): Promise<void> => {
	for (let attempt = 0; attempt < 100; attempt++) {
		const [n] = await engine.db.query(
			'SELECT VALUE n FROM ONLY pipeline_left:one;',
		);
		if (n === 1) return;
		await delay(5);
	}
	throw new Error(
		'the first pipeline block did not commit during the conflict probe',
	);
};

describe.skipIf(!engineConfigured)('pipelines on a disposable engine', () => {
	let engine: Engine;

	beforeAll(async () => {
		engine = await startEngine();
		await engine.db.query(
			'DEFINE TABLE pipeline_row; DEFINE TABLE pipeline_lock; DEFINE TABLE pipeline_left; DEFINE TABLE pipeline_middle; DEFINE TABLE pipeline_right;',
		);
		console.info(
			`[effect-surrealkit] pipeline: ${engine.mode} engine ${engine.version}`,
		);
	});
	afterAll(async () => engine?.stop());
	beforeEach(async () => {
		await engine.db.query('DELETE pipeline_row;');
	});

	const counted = () => {
		const calls = { query: 0, transaction: 0 };
		const db: Queryable = {
			// A Vitest spy would observe the lazy SDK Query with .then() and send it too.
			query: ((...args: Array<unknown>) => {
				calls.query++;
				return Reflect.apply(engine.db.query, engine.db, args);
			}) as Queryable['query'],
			transaction: <R extends Array<unknown>>(
				...args: Parameters<Queryable['transaction']>
			) => {
				calls.transaction++;
				return engine.db.transaction<R>(...args);
			},
		};
		const transport = sdkTransport(db);
		const send = vi.fn(transport.send);
		return {
			calls,
			send,
			client: makeWith({ ...transport, send }),
		};
	};
	const numbers = async () => {
		const [values] = await engine.db.query(
			'SELECT VALUE n FROM pipeline_row ORDER BY n;',
		);
		return values;
	};

	it('V1.1: continues after a throw, keeping the preceding writes including writes in the failed statement', async () => {
		const { client, calls } = counted();
		const [left, failed, right] = await run(
			client.pipeline([bind(first), bind(middle), bind(last)]),
		);
		expect(left).toEqual(Result.succeed([]));
		expect(failedAs(failed)).toBeInstanceOf(StatementThrown);
		expect(failedAs(failed)).toMatchObject({
			statement: middle.name,
			reason: 'middle',
			source: 'pipeline/middle.surql',
			lines: [2],
		});
		expect(right).toEqual(Result.succeed([]));
		expect(await numbers()).toEqual([1, 2, 3]);
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});

	it('V1.2: returns NONE for successful controls and selects the throw after a retroactively failed slot', async () => {
		const { client, send, calls } = counted();
		const [left, failed, right] = await run(
			client.pipeline([bind(first), bind(middle), bind(last)], {
				each: 'transaction',
			}),
		);
		expect(left).toEqual(Result.succeed([]));
		expect(failedAs(failed)).toBeInstanceOf(StatementThrown);
		expect(failedAs(failed)).toMatchObject({
			statement: middle.name,
			reason: 'middle',
		});
		expect(right).toEqual(Result.succeed([]));
		expect(await numbers()).toEqual([1, 3]);
		const slots: ReadonlyArray<SlotAnswer> | undefined =
			await send.mock.results[0]?.value;
		expect(slots).toHaveLength(11);
		for (const index of [0, 2, 3, 8, 10]) {
			expect(slots?.[index]).toMatchObject({
				ok: true,
				value: undefined,
				server: expect.anything(),
			});
		}
		expect(slots?.slice(4, 8)).toMatchObject([
			{
				ok: false,
				error: {
					message: expect.stringMatching(/failed transaction/u),
				},
			},
			{ ok: false, error: { kind: 'Thrown' } },
			{
				ok: false,
				error: {
					message: expect.stringMatching(/cancelled transaction/u),
				},
			},
			{
				ok: false,
				error: {
					message: expect.stringMatching(
						/aborted due to a prior error/u,
					),
				},
			},
		]);
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});

	it('V1.3: reports a real commit conflict in its own block while the other blocks commit', async () => {
		await engine.db.query(`
			CREATE ONLY pipeline_left:one SET n = 0;
			CREATE ONLY pipeline_right:one SET n = 0;
			CREATE ONLY pipeline_lock:held SET n = 2;
		`);
		const { client, send, calls } = counted();
		const outcome = run(
			client.pipeline(
				[
					bind(beforeConflict),
					bind(writeUntilConflict),
					bind(afterConflict),
				],
				{ each: 'transaction' },
			),
		);
		const [[left, failed, right]] = await Promise.all([
			outcome,
			(async () => {
				await waitForFirstCommit(engine);
				// The middle block reads FOR UPDATE immediately, then pauses inside the transaction.
				await delay(100);
				await engine.db.query('UPDATE pipeline_lock:held SET n += 1;');
			})(),
		]);
		const slots: ReadonlyArray<SlotAnswer> | undefined =
			await send.mock.results[0]?.value;
		const written = await engine.db.query(`
			SELECT VALUE n FROM ONLY pipeline_left:one;
			SELECT VALUE n FROM ONLY pipeline_middle:one;
			SELECT VALUE n FROM ONLY pipeline_right:one;
			SELECT VALUE n FROM ONLY pipeline_lock:held;
		`);
		expect(left).toEqual(Result.succeed(1));
		expect(failedAs(failed)).toBeInstanceOf(StatementConflict);
		expect(failedAs(failed)).toMatchObject({
			statement: writeUntilConflict.name,
		});
		expect(right).toEqual(Result.succeed(1));
		expect(written).toEqual([1, undefined, 1, 3]);
		expect(slots?.slice(4, 7)).toEqual(
			Array.from({ length: 3 }, () =>
				expect.objectContaining({
					ok: false,
					error: expect.objectContaining({ isNotExecuted: true }),
				}),
			),
		);
		expect(slots?.[7]).toMatchObject({
			ok: false,
			error: { isTransactionConflict: true },
		});
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});

	it.each([
		'statement',
		'transaction',
	] as const)('V1.4: an unrenamed LET is visible later in the %s pipeline', async (each) => {
		const define = make({
			name: 'define shared',
			sql: 'LET $shared = 42;',
			defines: ['shared'],
			params: {},
			result: Schema.Undefined,
			writes: false,
		});
		const read = make({
			name: 'read shared',
			sql: '$shared;',
			params: {},
			result: Schema.Number,
			writes: false,
		});
		const { client, calls } = counted();
		expect(
			await run(client.pipeline([bind(define), bind(read)], { each })),
		).toEqual([Result.succeed(undefined), Result.succeed(42)]);
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});

	it('runs a statement’s own transaction in statement mode only', async () => {
		const own = make({
			name: 'transactional read',
			sql: 'BEGIN; 2; COMMIT;',
			params: {},
			result: Schema.Number,
			slots: 3,
			valueSlot: 2,
			transactional: true,
			writes: false,
		});
		const { client, calls } = counted();
		const statements = [bind(own), bind(last)] as const;
		const refused = await run(
			Effect.flip(client.pipeline(statements, { each: 'transaction' })),
		);
		expect(refused).toMatchObject({
			_tag: 'StatementScopeError',
			statement: own.name,
		});
		expect(calls).toEqual({ query: 0, transaction: 0 });
		expect(
			await run(client.pipeline(statements, { each: 'statement' })),
		).toEqual([Result.succeed(2), Result.succeed([])]);
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});

	it('reports a result codec failure after its block commits and preserves the other block', async () => {
		const undecodable = make({ ...first, result: Schema.Number });
		const { client, calls } = counted();
		const [failed, right] = await run(
			client.pipeline([bind(undecodable), bind(last)], {
				each: 'transaction',
			}),
		);
		expect(failedAs(failed)).toBeInstanceOf(StatementWriteOutcomeError);
		expect(failedAs(failed)).toMatchObject({
			request: first.name,
			cause: { committed: true, slot: 1 },
		});
		expect(right).toEqual(Result.succeed([]));
		expect(await numbers()).toEqual([1, 3]);
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});

	it('places an injected conflict in the matching result, with no part sent', async () => {
		const injected = make({
			...last,
			name: 'injected write',
			sql: '/* pipeline conflict marker */ UPSERT pipeline_row:last SET n = 3 RETURN NONE;',
		});
		const { session, matched } = conflictingSession(engine.db, {
			marker: 'pipeline conflict marker',
			times: 1,
		});
		const client = makeClient(session);
		const entries = [bind(first), bind(injected)] as const;
		const [other, failed] = await run(
			client.pipeline(entries, { each: 'transaction' }),
		);
		expect(failedAs(other)).toBeInstanceOf(StatementRequestError);
		expect(failedAs(other)).toMatchObject({
			statement: first.name,
			cause: { isNotExecuted: true },
		});
		expect(failedAs(failed)).toBeInstanceOf(StatementConflict);
		expect(failedAs(failed)).toMatchObject({ statement: injected.name });
		expect(matched.count).toBe(1);
		expect(await numbers()).toEqual([]);
		expect(
			await run(client.pipeline(entries, { each: 'transaction' })),
		).toEqual([Result.succeed([]), Result.succeed([])]);
		expect(matched.count).toBe(2);
		expect(await numbers()).toEqual([1, 3]);
	});

	it.each([
		'statement',
		'transaction',
	] as const)('sends eight statements in exactly one SDK query in %s mode', async (each) => {
		const read = make({
			name: 'read eight',
			sql: '42;',
			params: {},
			result: Schema.Number,
			writes: false,
		});
		const { client, calls } = counted();
		const outcomes = await run(
			client.pipeline(
				Array.from({ length: 8 }, () => bind(read)),
				{ each },
			),
		);
		expect(outcomes).toEqual(
			Array.from({ length: 8 }, () => Result.succeed(42)),
		);
		expect(calls).toEqual({ query: 1, transaction: 0 });
	});
});
