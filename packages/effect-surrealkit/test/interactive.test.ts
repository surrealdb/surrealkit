import {
	Cause,
	Deferred,
	Effect,
	Exit,
	Fiber,
	Schedule,
	Schema,
	Stream,
	Tracer,
} from 'effect';
import { TestClock } from 'effect/testing';
import {
	CallTerminatedError,
	Features,
	QueryError,
	Surreal,
	UnsupportedFeatureError,
} from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	bind,
	make as makeClient,
	run,
	StatementConflict,
	StatementDecodeError,
	StatementRequestError,
	StatementScopeError,
	StatementUnsupported,
	SurrealClient,
	TransactionOutcomeUnknown,
	withTransaction,
} from '../src/client/index.js';
import type { TransactionHandle } from '../src/client/internal/transport.js';
import { LiveStatementTypeId, make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { conflict, failure, fake, unsupported } from '../src/testing/index.js';

const read = make({
	name: 'read',
	sql: '1;',
	params: {},
	result: Schema.Number,
	writes: false,
});
const change = make({
	name: 'change',
	sql: 'UPDATE item SET n = 1;',
	params: {},
	result: Schema.Number,
	writes: true,
});
const rows = make({
	name: 'rows',
	sql: '[1];',
	params: {},
	result: Schema.Array(Schema.Number),
	writes: false,
});
const ownTransaction = make({
	name: 'own control',
	sql: 'BEGIN; 1; COMMIT;',
	params: {},
	result: Schema.Number,
	slots: 3,
	valueSlot: 2,
	transactional: true,
});
const liveRead = {
	[LiveStatementTypeId]: LiveStatementTypeId,
	name: 'live rows',
	sql: 'LIVE SELECT * FROM item;',
	params: {},
	row: Schema.Number,
	id: Schema.Number,
} as const;

const fail = <A, E>(effect: Effect.Effect<A, E>) =>
	Effect.runPromise(Effect.flip(effect));

const usingHandle = (handle: TransactionHandle) =>
	makeWith({ ...scriptedTransport({}).transport, begin: async () => handle });

describe('interactive transactions', () => {
	it('commits a successful body and joins multiple bound statements in it', async () => {
		const client = fake({ read: 1, change: 2 });
		expect(
			await Effect.runPromise(
				client.withTransaction((tx) =>
					Effect.gen(function* () {
						const single = yield* tx.run(read);
						const batch = yield* tx.transaction([
							bind(read),
							bind(change),
						]);
						return { single, batch };
					}),
				),
			),
		).toEqual({ single: 1, batch: [1, 2] });
		expect(
			client.calls.map(({ statement, interactive }) => [
				statement,
				interactive,
			]),
		).toEqual([
			['begin', 'transaction 1'],
			['read', 'transaction 1'],
			['read', 'transaction 1'],
			['change', 'transaction 1'],
			['commit', 'transaction 1'],
		]);
	});

	it('cancels on a typed failure, a synchronous body defect, and interruption', async () => {
		const typed = fake({});
		const failure = new Error('body failed');
		expect(
			await fail(typed.withTransaction(() => Effect.fail(failure))),
		).toBe(failure);
		expect(typed.calls.map(({ operation }) => operation)).toEqual([
			'begin',
			'cancel',
		]);

		const defective = fake({});
		const exit = await Effect.runPromise(
			Effect.exit(
				defective.withTransaction(() => {
					throw failure;
				}),
			),
		);
		expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
		expect(defective.calls.map(({ operation }) => operation)).toEqual([
			'begin',
			'cancel',
		]);

		const interrupted = fake({});
		await Effect.runPromise(
			Effect.gen(function* () {
				const began = yield* Deferred.make<void>();
				const fiber = yield* Effect.forkChild(
					interrupted.withTransaction(() =>
						Effect.andThen(
							Deferred.succeed(began, undefined),
							Effect.never,
						),
					),
				);
				yield* Deferred.await(began);
				yield* Fiber.interrupt(fiber);
				const exit = fiber.pollUnsafe();
				expect(
					exit !== undefined &&
						Exit.isFailure(exit) &&
						Cause.hasInterrupts(exit.cause),
				).toBe(true);
			}),
		);
		expect(interrupted.calls.map(({ operation }) => operation)).toEqual([
			'begin',
			'cancel',
		]);
	});

	it('cancels a handle acquired after interruption without starting the body', async () => {
		await Effect.runPromise(
			Effect.gen(function* () {
				const beginEntered = yield* Deferred.make<void>();
				const releaseBegin = yield* Deferred.make<void>();
				const messages: Array<string> = [];
				const { transport } = scriptedTransport({});
				const client = makeWith({
					...transport,
					begin: async () => {
						await Effect.runPromise(
							Deferred.succeed(beginEntered, undefined),
						);
						await Effect.runPromise(Deferred.await(releaseBegin));
						return {
							send: async () => [],
							commit: async () => {
								messages.push('commit');
							},
							cancel: async () => {
								messages.push('cancel');
							},
						};
					},
				});
				const fiber = yield* Effect.forkChild(
					client.withTransaction(() =>
						Effect.sync(() => messages.push('body')),
					),
				);
				yield* Deferred.await(beginEntered);
				fiber.interruptUnsafe();
				expect(fiber.pollUnsafe()).toBeUndefined();
				yield* Deferred.succeed(releaseBegin, undefined);
				yield* Fiber.interrupt(fiber);
				expect(messages).toEqual(['cancel']);
			}),
		);
	});

	it('preserves a body failure and records failed cancellation on its span', async () => {
		const client = fake({ cancel: failure('cancel rejected') });
		const spans: Array<Tracer.Span> = [];
		const tracer = Tracer.make({
			span: (options) => {
				const span = new Tracer.NativeSpan(options);
				spans.push(span);
				return span;
			},
		});
		const bodyError = new Error('body failure');
		expect(
			await fail(
				client
					.withTransaction(() => Effect.fail(bodyError))
					.pipe(Effect.withTracer(tracer)),
			),
		).toBe(bodyError);
		expect(
			spans
				.find((span) => span.name === 'withTransaction')
				?.attributes.get('effect_surrealkit.cancel_failed'),
		).toBe(true);
		expect(client.calls.map(({ operation }) => operation)).toEqual([
			'begin',
			'cancel',
		]);
	});

	it('retries a conflicting statement by replaying the whole body on a new handle', async () => {
		let runs = 0;
		const client = fake({
			read: 1,
			change: () => (runs === 1 ? conflict() : 2),
		});
		expect(
			await Effect.runPromise(
				client.withTransaction(
					(tx) =>
						Effect.gen(function* () {
							runs++;
							yield* tx.run(read);
							return yield* tx.run(change);
						}),
					{ retry: Schedule.recurs(1) },
				),
			),
		).toBe(2);
		expect(runs).toBe(2);
		expect(
			client.calls.map(({ statement, interactive }) => [
				statement,
				interactive,
			]),
		).toEqual([
			['begin', 'transaction 1'],
			['read', 'transaction 1'],
			['change', 'transaction 1'],
			['cancel', 'transaction 1'],
			['begin', 'transaction 2'],
			['read', 'transaction 2'],
			['change', 'transaction 2'],
			['commit', 'transaction 2'],
		]);
	});

	it('reports scoped decoding while writes are still provisional', async () => {
		const client = fake({ change: 'wrong codec' });
		const error = await fail(
			client.withTransaction((tx) => tx.run(change)),
		);
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(error).toMatchObject({ committed: false });
		expect(client.calls.at(-1)?.operation).toBe('cancel');
	});

	it('refuses inner control, nested bodies, and ended handles before sending', async () => {
		const client = fake({ read: 1 });
		expect(
			await fail(client.withTransaction((tx) => tx.run(ownTransaction))),
		).toBeInstanceOf(StatementScopeError);
		expect(
			await fail(
				client.withTransaction((tx) =>
					tx.transaction([bind(ownTransaction)]),
				),
			),
		).toBeInstanceOf(StatementScopeError);
		expect(
			await fail(
				client.withTransaction(() =>
					client.withTransaction(() => Effect.void),
				),
			),
		).toBeInstanceOf(StatementScopeError);
		const escaped = await Effect.runPromise(
			client.withTransaction(Effect.succeed),
		);
		expect(await fail(escaped.run(read))).toBeInstanceOf(
			StatementScopeError,
		);
		expect(client.calls.map(({ operation }) => operation)).toEqual([
			'begin',
			'cancel',
			'begin',
			'cancel',
			'begin',
			'cancel',
			'begin',
			'commit',
		]);
	});

	it('provides scoped run and prevents nested operations via the context client', async () => {
		const client = fake({ read: 1 });
		const existing = run(read);
		const checked = Effect.gen(function* () {
			const scoped = yield* SurrealClient;
			const operations: ReadonlyArray<
				Effect.Effect<unknown, Error, SurrealClient>
			> = [
				scoped.pipeline([bind(read)]),
				withTransaction(Effect.void),
				Stream.runCollect(scoped.stream(rows)),
				Stream.runCollect(scoped.live(liveRead)),
				Effect.scoped(scoped.liveScoped(liveRead)),
			];
			for (const effect of operations) {
				const error = yield* Effect.flip(effect);
				expect(error).toBeInstanceOf(StatementScopeError);
				expect(error.message).toMatch(
					/cannot run inside an interactive transaction/u,
				);
			}
			return yield* existing;
		});
		expect(
			await Effect.runPromise(
				withTransaction(checked).pipe(
					Effect.provideService(SurrealClient, client),
				),
			),
		).toBe(1);
		expect(client.calls.map(({ statement }) => statement)).toEqual([
			'begin',
			'read',
			'commit',
		]);
	});

	it('maps responding server rejections separately from a lost commit reply', async () => {
		let cancels = 0;
		let attempts = 0;
		const rejection = new QueryError({
			kind: 'Query',
			message: 'commit refused',
		});
		const rejected = usingHandle({
			send: async () => [],
			commit: async () => {
				throw rejection;
			},
			cancel: async () => {
				cancels++;
			},
		});
		const error = await fail(
			rejected.withTransaction(() => Effect.void, {
				retry: Schedule.recurs(2),
			}),
		);
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error).toMatchObject({ cause: rejection });
		expect(cancels).toBe(1);

		const lost = new CallTerminatedError();
		const ambiguous = usingHandle({
			send: async () => [],
			commit: async () => {
				attempts++;
				throw lost;
			},
			cancel: async () => {
				cancels++;
			},
		});
		const unknown = await fail(
			ambiguous.withTransaction(() => Effect.void, {
				retry: Schedule.recurs(2),
			}),
		);
		expect(unknown).toBeInstanceOf(TransactionOutcomeUnknown);
		expect(unknown).toMatchObject({ cause: lost });
		expect(attempts).toBe(1);
		expect(cancels).toBe(1);
	});

	it('waits exactly 30 seconds for a missing commit acknowledgement before failing once', async () => {
		let attempts = 0;
		let didCancel = false;
		let reply!: () => void;
		const noReply = new Promise<void>((resolve) => {
			reply = resolve;
		});
		const client = usingHandle({
			send: async () => [],
			commit: () => {
				attempts++;
				return noReply;
			},
			cancel: async () => {
				didCancel = true;
			},
		});
		const error = await Effect.runPromise(
			Effect.gen(function* () {
				const fiber = yield* Effect.forkChild(
					Effect.flip(
						client.withTransaction(() => Effect.void, {
							retry: Schedule.recurs(2),
						}),
					),
				);
				yield* TestClock.adjust('29 seconds');
				expect(attempts).toBe(1);
				expect(fiber.pollUnsafe()).toBeUndefined();
				yield* TestClock.adjust('1 second');
				const error = yield* Fiber.join(fiber);
				reply();
				return error;
			}).pipe(Effect.provide(TestClock.layer())),
		);
		expect(error).toBeInstanceOf(TransactionOutcomeUnknown);
		expect(attempts).toBe(1);
		expect(didCancel).toBe(false);
	});

	it('accepts a commit reply before its acknowledgement deadline', async () => {
		let reply!: () => void;
		const waiting = new Promise<void>((resolve) => {
			reply = resolve;
		});
		const client = usingHandle({
			send: async () => [],
			commit: () => waiting,
			cancel: async () => undefined,
		});
		expect(
			await Effect.runPromise(
				Effect.gen(function* () {
					const fiber = yield* Effect.forkChild(
						client.withTransaction(() => Effect.succeed(3)),
					);
					yield* TestClock.adjust('29 seconds');
					expect(fiber.pollUnsafe()).toBeUndefined();
					reply();
					return yield* Fiber.join(fiber);
				}).pipe(Effect.provide(TestClock.layer())),
			),
		).toBe(3);
	});

	it('rejects an unavailable interactive transaction before starting the body', async () => {
		const sdk = new Surreal();
		const missing = makeClient({
			query: sdk.query.bind(sdk),
			transaction: sdk.transaction.bind(sdk),
		});
		const unavailable = fake({
			begin: unsupported('interactive transaction'),
		});
		const body = () => Effect.die('body must not run');
		expect(await fail(missing.withTransaction(body))).toBeInstanceOf(
			StatementUnsupported,
		);
		const refused = await fail(unavailable.withTransaction(body));
		expect(refused).toBeInstanceOf(StatementUnsupported);
		expect(refused).toMatchObject({
			feature: 'interactive transaction',
			cause: new UnsupportedFeatureError(Features.Transactions),
		});
	});

	it('retries a structured commit conflict and cancels its first handle', async () => {
		let count = 0;
		const client = fake({
			commit: () => (++count === 1 ? conflict() : undefined),
		});
		let runs = 0;
		expect(
			await Effect.runPromise(
				client.withTransaction(() => Effect.sync(() => ++runs), {
					retry: Schedule.recurs(1),
				}),
			),
		).toBe(2);
		expect(client.calls.map(({ operation }) => operation)).toEqual([
			'begin',
			'commit',
			'cancel',
			'begin',
			'commit',
		]);
		const noRetry = fake({ commit: conflict() });
		expect(
			await fail(noRetry.withTransaction(() => Effect.void)),
		).toBeInstanceOf(StatementConflict);
	});
});
