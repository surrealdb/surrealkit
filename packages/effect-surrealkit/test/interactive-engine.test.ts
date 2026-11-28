import { Deferred, Duration, Effect, Fiber, Schedule, Schema } from 'effect';
import {
	QueryError,
	type SurrealSession,
	UnsupportedFeatureError,
} from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	bind,
	make as makeClient,
	run,
	StatementConflict,
	StatementThrown,
	StatementUnsupported,
	SurrealClient,
	withTransaction,
} from '../src/client/index.js';
import { requestPart } from '../src/client/internal/join.js';
import { sdkTransport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	interactiveTransactions,
	startEngine,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] interactive engine suite skipped: ${engineSkipReason}`,
	);
}
if (!interactiveTransactions) {
	console.warn(
		'[effect-surrealkit] HTTP does not support interactive transactions; testing its refusal',
	);
}

const inspect = make({
	name: 'inspect counter',
	sql: 'SELECT VALUE n FROM ONLY account:counter;',
	params: {},
	result: Schema.Number,
	writes: false,
});
const increment = make({
	name: 'increment counter',
	sql: 'UPDATE ONLY account:counter SET n += 1 RETURN VALUE n;',
	params: {},
	result: Schema.Number,
	writes: true,
});
const witness = make({
	name: 'inspect witness',
	sql: 'SELECT VALUE n FROM ONLY account:witness;',
	params: {},
	result: Schema.Number,
	writes: false,
});
const markWitness = make({
	name: 'increment witness',
	sql: 'UPDATE ONLY account:witness SET n += 1 RETURN VALUE n;',
	params: {},
	result: Schema.Number,
	writes: true,
});
const throws = make({
	name: 'throw in the body',
	sql: "THROW 'example failure';",
	params: {},
	result: Schema.Undefined,
	throws: ['example failure'],
	writes: false,
});
const assignLocal = make({
	name: 'assign local',
	sql: 'LET $l4_local = 7;',
	params: {},
	defines: ['l4_local'],
	result: Schema.Undefined,
});
const readLocal = make({
	name: 'read local',
	sql: '$l4_local;',
	params: {},
	result: Schema.Undefined,
});

describe.skipIf(!engineConfigured || !interactiveTransactions)(
	'interactive transactions on a disposable engine',
	() => {
		let engine: Engine;
		let another: SurrealSession;

		beforeAll(async () => {
			engine = await startEngine();
			another = await engine.db.forkSession();
			await engine.db.query(
				'CREATE ONLY account:counter SET n = 0; CREATE ONLY account:witness SET n = 0;',
			);
			console.log(
				`[effect-surrealkit] interactive: ${engine.mode} engine ${engine.version}`,
			);
		});
		afterAll(async () => {
			try {
				await another?.closeSession();
			} finally {
				await engine?.stop();
			}
		});

		const reset = () =>
			engine.db.query('UPDATE account SET n = 0 RETURN NONE;');
		const outside = () =>
			Effect.runPromise(makeClient(another).run(inspect));

		it('keeps writes private until commit and discards a cancelled body', async () => {
			await reset();
			const client = makeClient(engine.db);
			const existing = run(inspect);
			const program = Effect.gen(function* () {
				yield* run(increment);
				const n = yield* existing;
				expect(n).toBe(1);
				expect(yield* Effect.promise(outside)).toBe(0);
				return n;
			});
			expect(
				await Effect.runPromise(
					withTransaction(program).pipe(
						Effect.provideService(SurrealClient, client),
					),
				),
			).toBe(1);
			expect(await outside()).toBe(1);

			const failed = client.withTransaction((tx) =>
				Effect.andThen(tx.run(increment), tx.run(throws)),
			);
			expect(await Effect.runPromise(Effect.flip(failed))).toBeInstanceOf(
				StatementThrown,
			);
			expect(await outside()).toBe(1);
		});

		it('keeps the transaction open after a caught statement failure on both engines', async () => {
			await reset();
			const client = makeClient(engine.db);
			const result = await Effect.runPromise(
				client.withTransaction((tx) =>
					Effect.gen(function* () {
						yield* tx.run(increment);
						const error = yield* Effect.flip(tx.run(throws));
						expect(error).toBeInstanceOf(StatementThrown);
						expect(error).toHaveProperty(
							'reason',
							'example failure',
						);
						const [n, read] = yield* tx.transaction([
							bind(increment),
							bind(inspect),
						]);
						expect(yield* Effect.promise(outside)).toBe(0);
						return [n, read];
					}),
				),
			);
			expect(result).toEqual([2, 2]);
			expect(await outside()).toBe(2);
		});

		it('does not carry a LET binding into the next request', async () => {
			const value = await Effect.runPromise(
				makeClient(engine.db).withTransaction((tx) =>
					Effect.andThen(tx.run(assignLocal), tx.run(readLocal)),
				),
			);
			expect(value).toBeUndefined();
		});

		it('classifies the second writer’s actual commit conflict as determinate', async () => {
			await reset();
			const first = await another.beginTransaction();
			try {
				await first.query(increment.sql);
				const failed = makeClient(engine.db).withTransaction((tx) =>
					Effect.andThen(
						tx.run(increment),
						Effect.promise(() => first.commit()),
					),
				);
				const error = await Effect.runPromise(Effect.flip(failed));
				expect(error).toBeInstanceOf(StatementConflict);
				expect(error).toHaveProperty('cause', expect.any(QueryError));
				expect(error).toMatchObject({
					cause: {
						isTransactionConflict: true,
						details: { kind: 'TransactionConflict' },
					},
				});
				expect(await outside()).toBe(1);
			} finally {
				await first.cancel().catch(() => undefined);
			}
		});

		it('replays from a fresh snapshot after a commit conflict and persists only one body write', async () => {
			await reset();
			const first = await another.beginTransaction();
			const observed: Array<number> = [];
			try {
				await first.query(increment.sql);
				const effect = makeClient(engine.db).withTransaction(
					(tx) =>
						Effect.gen(function* () {
							observed.push(yield* tx.run(witness));
							yield* tx.run(increment);
							const written = yield* tx.run(markWitness);
							if (observed.length === 1)
								yield* Effect.promise(() => first.commit());
							return written;
						}),
					{ retry: Schedule.recurs(1) },
				);
				expect(await Effect.runPromise(effect)).toBe(1);
				expect(observed).toEqual([0, 0]);
				expect(await outside()).toBe(2);
				expect(
					await Effect.runPromise(makeClient(another).run(witness)),
				).toBe(1);
			} finally {
				await first.cancel().catch(() => undefined);
			}
		});

		it('cancels a body interrupted after writing so another session can write', async () => {
			await reset();
			const client = makeClient(engine.db);
			await Effect.runPromise(
				Effect.gen(function* () {
					const written = yield* Deferred.make<void>();
					const fiber = yield* Effect.forkChild(
						client.withTransaction((tx) =>
							Effect.gen(function* () {
								yield* tx.run(increment);
								yield* Deferred.succeed(written, undefined);
								return yield* Effect.never;
							}),
						),
					);
					yield* Deferred.await(written);
					expect(yield* Effect.promise(outside)).toBe(0);
					yield* Fiber.interrupt(fiber);
				}),
			);
			expect(await outside()).toBe(0);
			const outsider = makeClient(another)
				.run(increment)
				.pipe(Effect.timeout('1 second'));
			expect(await Effect.runPromise(outsider)).toBe(1);
		});

		it('maps the SDK transaction’s slot responses with server timing', async () => {
			const signal = new AbortController().signal;
			const handle = await sdkTransport(engine.db).begin?.(signal);
			if (handle === undefined)
				throw new Error('missing SDK transaction method');
			try {
				const slots = await handle.send(
					{
						mode: 'query',
						sql: inspect.sql,
						bindings: {},
						parts: [requestPart(inspect, {}, 0, 1)],
					},
					signal,
				);
				expect(slots).toHaveLength(1);
				expect(slots[0]?.ok).toBe(true);
				expect(Duration.isDuration(slots[0]?.server)).toBe(true);
			} finally {
				await handle.cancel();
			}
		});
	},
);

describe.skipIf(!engineConfigured || interactiveTransactions)(
	'interactive transactions over HTTP',
	() => {
		it('returns StatementUnsupported without invoking the body', async () => {
			const engine = await startEngine();
			try {
				const result = await Effect.runPromise(
					Effect.flip(
						makeClient(engine.db).withTransaction(() =>
							Effect.die('HTTP invoked the transaction body'),
						),
					),
				);
				expect(result).toBeInstanceOf(StatementUnsupported);
				expect(result).toMatchObject({
					feature: 'interactive transaction',
					cause: expect.any(UnsupportedFeatureError),
				});
			} finally {
				await engine.stop();
			}
		});
	},
);
