import { setImmediate } from 'node:timers/promises';
import { Deferred, Effect, Fiber, Schema, Stream } from 'effect';
import { CallTerminatedError, CborCodec, Surreal, Uuid } from 'surrealdb';
import { expect, it } from 'vitest';

import {
	BulkWriteError,
	LiveQueryClosed,
	make as makeClient,
	StatementRequestError,
} from '../src/client/index.js';
import { live, make } from '../src/statement/index.js';

const endpoint = 'ws://127.0.0.1:8000/rpc';
const increment = make({
	name: 'increment',
	sql: 'UPDATE counter:one SET n += array::len($rows) RETURN VALUE n;',
	params: { rows: Schema.Array(Schema.Number) },
	result: Schema.Array(Schema.Number),
	writes: true,
});
const listener = live({
	name: 'counter',
	id: Schema.Unknown,
	select: Schema.Struct({ n: Schema.Number }),
}).build('counter changes');

const run = <A, E>(effect: Effect.Effect<A, E>) =>
	Effect.runPromise(effect.pipe(Effect.timeout('2 seconds')));

type Rpc = {
	id: string;
	method: string;
	params?: [string, { rows: number[] }];
};

function bufferedConnection() {
	const codec = CborCodec.DEFAULT;
	const liveId = Uuid.v7();
	const state = { opens: 0, closes: 0, counter: 0, registrations: 0 };
	const writes: Array<{ socket: number; id: string; counter: number }> = [];
	class Socket extends EventTarget {
		static CONNECTING = 0;
		static OPEN = 1;
		static CLOSING = 2;
		static CLOSED = 3;
		readyState = Socket.CONNECTING;
		binaryType = 'arraybuffer';
		#number = 0;

		constructor(url: string, protocol: string) {
			super();
			expect(url).toBe(endpoint);
			expect(protocol).toBe('cbor');
			queueMicrotask(() => {
				if (this.readyState !== Socket.CONNECTING) return;
				this.#number = ++state.opens;
				this.readyState = Socket.OPEN;
				this.dispatchEvent(new Event('open'));
			});
		}

		send(data: Uint8Array) {
			const { id, method, params } = codec.decode<Rpc>(data);
			if (method === 'version') return this.respond(id, '3.3.4');
			expect(method).toBe('query');
			const [sql, bindings] = params ?? [];
			if (sql?.startsWith('LIVE SELECT ')) {
				state.registrations++;
				return this.answer(id, liveId);
			}
			if (sql === increment.sql) {
				expect(bindings?.rows).toEqual([1]);
				state.counter += 1;
				writes.push({
					socket: this.#number,
					id,
					counter: state.counter,
				});
				// Apply the first write, then close the socket before acknowledging it.
				if (writes.length === 1) return this.close();
				return this.answer(id, [state.counter]);
			}
			if (sql === 'RETURN 99;') return this.answer(id, 99);
			expect(sql).toMatch(/^KILL /u);
			return this.answer(id, undefined);
		}

		answer(id: string, result: unknown) {
			this.respond(id, [{ status: 'OK', result, time: '1ns' }]);
		}

		respond(id: string, result: unknown) {
			const data = codec.encode({ id, result });
			queueMicrotask(() => {
				if (this.readyState === Socket.OPEN)
					this.dispatchEvent(new MessageEvent('message', { data }));
			});
		}

		close() {
			if (this.readyState >= Socket.CLOSING) return;
			this.readyState = Socket.CLOSING;
			queueMicrotask(() => {
				this.readyState = Socket.CLOSED;
				state.closes++;
				this.dispatchEvent(new Event('close'));
			});
		}
	}
	const db = new Surreal({
		streaming: false,
		// The SDK only uses EventTarget, send, close and readyState in this test.
		websocketImpl: Socket as unknown as typeof WebSocket,
	});
	return { db, state, writes };
}

it.each([
	false,
	true,
])('handles a lost buffered WebSocket acknowledgement with reconnect=%s', async (reconnect) => {
	const { db, state, writes } = bufferedConnection();
	const client = makeClient(db);
	const ready = Effect.runSync(Deferred.make<void>());
	const listen = Effect.scoped(
		Effect.gen(function* () {
			const stream = yield* client.liveScoped(listener);
			yield* Deferred.succeed(ready, undefined);
			return yield* Stream.runDrain(stream);
		}),
	);
	let fiber: Fiber.Fiber<void, Effect.Error<typeof listen>> | undefined;
	try {
		await run(
			Effect.promise(() =>
				db.connect(endpoint, {
					pingInterval: 0,
					reconnect: reconnect
						? { attempts: 1, retryDelay: 1, retryDelayJitter: 0 }
						: false,
				}),
			),
		);
		fiber = Effect.runFork(listen);
		await run(Deferred.await(ready));
		const bulk = client.bulk(
			increment,
			{ rows: [1] },
			{ over: 'rows', atomic: false },
		);
		if (reconnect) {
			expect(await run(bulk)).toEqual({
				chunks: 1,
				rows: 1,
				results: [[2]],
			});
			expect(
				writes.map(({ socket, counter }) => [socket, counter]),
			).toEqual([
				[1, 1],
				[2, 2],
			]);
			expect(writes[0]?.id).toBe(writes[1]?.id);
			expect(
				await run(
					Effect.promise(() => db.query('RETURN 99;').collect()),
				),
			).toEqual([99]);
			// Cross an event-loop turn after the new socket answers.
			await setImmediate();
			expect(fiber.pollUnsafe()).toBeUndefined();
			await run(Effect.promise(() => db.close()));
		} else {
			const failure = await run(Effect.flip(bulk));
			expect(failure).toBeInstanceOf(BulkWriteError);
			expect(failure).toMatchObject({
				chunksCommitted: 0,
				rowsCommitted: 0,
				cause: expect.any(StatementRequestError),
			});
			expect(failure).toMatchObject({
				cause: { cause: expect.any(CallTerminatedError) },
			});
			expect(
				writes.map(({ socket, counter }) => [socket, counter]),
			).toEqual([[1, 1]]);
		}
		const closed = await run(Effect.flip(Fiber.join(fiber)));
		expect(closed).toBeInstanceOf(LiveQueryClosed);
		expect(closed).toMatchObject({ reason: 'disconnected' });
		expect(state.opens).toBe(reconnect ? 2 : 1);
		expect(state.registrations).toBe(1);
	} finally {
		try {
			await run(Effect.promise(() => db.close()));
		} finally {
			if (fiber) await run(Fiber.interrupt(fiber));
		}
	}
	expect(state.closes).toBe(state.opens);
});
