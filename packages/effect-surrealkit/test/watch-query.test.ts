import { setImmediate } from 'node:timers/promises';
import { Effect, Fiber, Option, Schema, Stream } from 'effect';
import { NotAllowedError, RecordId } from 'surrealdb';
import { expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import { bindLive } from '../src/client/index.js';
import { Int, RecordIdFor } from '../src/index.js';
import { live, select } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';

const CounterId = RecordIdFor('watch_counter');
const counter = {
	name: 'watch_counter',
	id: CounterId,
	select: Schema.Struct({ id: CounterId, n: Int }),
} as const;
const snapshot = select(counter).build('counter snapshot');
const changes = live(counter)
	.project((row) => ({ id: row.id }))
	.build('counter changes');
const id = new RecordId('watch_counter', 'one');
const current = (n: number) => ({ _tag: 'Current', value: [{ id, n }] });
const gate = () => {
	let release = () => {};
	const promise = new Promise<void>((resolve) => {
		release = resolve;
	});
	return { promise, release };
};

it.each([
	1, 2,
])('coalesces a burst while snapshot %s is in flight', async (blockedRead) => {
	const { transport } = scriptedTransport({});
	const invalidate = gate();
	const refreshStarted = gate();
	const emitBurst = gate();
	const drained = gate();
	const finishRefresh = gate();
	const closed = gate();
	const readValues: number[] = [];
	let value = 0;
	let activeReads = 0;
	let concurrentReads = 0;
	let registrations = 0;
	let releases = 0;
	let watchSubscriptions = 0;
	const notification = {
		action: 'UPDATE',
		id,
		value: { id },
	} as const;
	const client = makeWith({
		...transport,
		watch: (notify) => {
			watchSubscriptions++;
			notify('connected');
			return () => {
				watchSubscriptions--;
			};
		},
		collect: async (request) => {
			expect(request.sql).toBe(snapshot.sql);
			expect(registrations).toBe(1);
			const read = value;
			readValues.push(read);
			activeReads++;
			concurrentReads = Math.max(concurrentReads, activeReads);
			try {
				if (readValues.length === blockedRead) {
					refreshStarted.release();
					await finishRefresh.promise;
				}
				return [[{ id, n: read }]];
			} finally {
				activeReads--;
			}
		},
		live: async (request) => {
			expect(request.sql).toBe(changes.sql);
			registrations++;
			return {
				messages: {
					async *[Symbol.asyncIterator]() {
						await invalidate.promise;
						yield notification;
						await emitBurst.promise;
						for (let index = 0; index < 2_048; index++)
							yield notification;
						drained.release();
						await closed.promise;
					},
				},
				kill: async () => {
					releases++;
					closed.release();
				},
			};
		},
	});

	await Effect.runPromise(
		Effect.scoped(
			Effect.gen(function* () {
				const pull = yield* client
					.watchQuery(snapshot, {
						sources: [bindLive(changes)],
						reconcileEvery: '1 hour',
					})
					.pipe(Stream.rechunk(1), Stream.toPull);
				const next = Effect.map(pull, ([state]) => state);
				expect(yield* next).toEqual({ _tag: 'Loading' });
				if (blockedRead === 2) {
					expect(yield* next).toEqual(current(0));
					value = 1;
					invalidate.release();
				}
				yield* Effect.promise(() => refreshStarted.promise);
				value = 2;
				invalidate.release();
				emitBurst.release();
				yield* Effect.promise(() => drained.promise);
				expect(readValues).toEqual(blockedRead === 1 ? [0] : [0, 1]);
				expect(concurrentReads).toBe(1);
				finishRefresh.release();
				const fresh = yield* Effect.repeat(next, {
					until: (state) =>
						state._tag === 'Current' && state.value[0]?.n === 2,
				});
				expect(fresh).toEqual(current(2));
				expect(
					yield* next.pipe(Effect.timeoutOption('50 millis')),
				).toEqual(Option.none());
			}).pipe(
				Effect.ensuring(
					Effect.sync(() => {
						invalidate.release();
						emitBurst.release();
						finishRefresh.release();
						closed.release();
					}),
				),
			),
		).pipe(Effect.timeout('5 seconds')),
	);
	expect(readValues).toEqual(blockedRead === 1 ? [0, 2] : [0, 1, 2]);
	expect(concurrentReads).toBe(1);
	expect(releases).toBe(1);
	expect(watchSubscriptions).toBe(0);
});

it('releases a source whose registration finishes during scope interruption', async () => {
	const { transport } = scriptedTransport({});
	const registering = gate();
	const finishRegistration = gate();
	let registrationsKilled = 0;
	let reads = 0;
	const client = makeWith({
		...transport,
		watch: (notify) => {
			notify('connected');
			return () => {};
		},
		collect: async () => {
			reads++;
			return [[]];
		},
		live: async () => {
			registering.release();
			await finishRegistration.promise;
			return {
				messages: { async *[Symbol.asyncIterator]() {} },
				kill: async () => {
					registrationsKilled++;
				},
			};
		},
	});
	const fiber = Effect.runFork(
		Stream.runDrain(
			client.watchQuery(snapshot, { sources: [bindLive(changes)] }),
		),
	);
	const interrupt = () => Effect.runPromise(Fiber.interrupt(fiber));
	try {
		await Effect.runPromise(
			Effect.promise(() => registering.promise).pipe(
				Effect.timeout('3 seconds'),
			),
		);
		const interrupted = interrupt();
		await setImmediate();
		expect(fiber.pollUnsafe()).toBeUndefined();
		finishRegistration.release();
		await interrupted;
		expect(registrationsKilled).toBe(1);
		expect(reads).toBe(0);
	} finally {
		finishRegistration.release();
		await interrupt();
	}
});

it('clears a previously visible snapshot on a structured server session-expiry response', async () => {
	const { transport } = scriptedTransport({});
	const expire = gate();
	const released = gate();
	let read = false;
	const client = makeWith({
		...transport,
		watch: (notify) => {
			notify('connected');
			return () => {};
		},
		collect: async () => {
			if (read) {
				throw new NotAllowedError({
					kind: 'NotAllowed',
					message: 'fixture session expired',
					details: {
						kind: 'Auth',
						details: { kind: 'SessionExpired' },
					},
				});
			}
			read = true;
			return [[{ id, n: 1 }]];
		},
		live: async () => ({
			messages: {
				async *[Symbol.asyncIterator]() {
					await expire.promise;
					yield { action: 'UPDATE', id, value: { id } };
					await released.promise;
				},
			},
			kill: async () => released.release(),
		}),
	});
	await Effect.runPromise(
		Effect.scoped(
			Effect.gen(function* () {
				const pull = yield* client
					.watchQuery(snapshot, {
						sources: [bindLive(changes)],
						reconcileEvery: '1 hour',
					})
					.pipe(Stream.rechunk(1), Stream.toPull);
				const next = Effect.map(pull, ([state]) => state);
				expect(yield* next).toEqual({ _tag: 'Loading' });
				expect(yield* next).toEqual(current(1));
				expire.release();
				expect(yield* next).toEqual({ _tag: 'Stale' });
				expect(yield* Effect.flip(next)).toMatchObject({
					_tag: 'WatchQueryClosed',
					reason: 'authentication',
				});
			}).pipe(
				Effect.ensuring(
					Effect.sync(() => {
						expire.release();
						released.release();
					}),
				),
			),
		).pipe(Effect.timeout('5 seconds')),
	);
});

it('detects a closed child on the timer while a refresh is still in flight', async () => {
	const { transport } = scriptedTransport({});
	const refreshing = gate();
	const finishRefresh = gate();
	const released = gate();
	let valid = true;
	let reads = 0;
	const client = makeWith({
		...transport,
		watch: (notify) => {
			notify('connected');
			return () => {};
		},
		isWatchSessionValid: () => valid,
		collect: async () => {
			reads++;
			if (reads === 2) {
				refreshing.release();
				await finishRefresh.promise;
			}
			return [[{ id, n: reads }]];
		},
		live: async () => ({
			messages: {
				[Symbol.asyncIterator]() {
					return {
						next: async () => {
							await released.promise;
							return { done: true as const, value: undefined };
						},
					};
				},
			},
			kill: async () => released.release(),
		}),
	});
	try {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const pull = yield* client
						.watchQuery(snapshot, {
							sources: [bindLive(changes)],
							reconcileEvery: '25 millis',
						})
						.pipe(Stream.rechunk(1), Stream.toPull);
					const next = Effect.map(pull, ([state]) => state);
					expect(yield* next).toEqual({ _tag: 'Loading' });
					expect(yield* next).toEqual(current(1));
					yield* Effect.promise(() => refreshing.promise);
					valid = false;
					expect(yield* next).toEqual({ _tag: 'Stale' });
					expect(yield* Effect.flip(next)).toMatchObject({
						_tag: 'WatchQueryClosed',
						reason: 'session-closed',
					});
				}),
			).pipe(Effect.timeout('5 seconds')),
		);
		expect(reads).toBe(2);
	} finally {
		finishRefresh.release();
		released.release();
	}
});
