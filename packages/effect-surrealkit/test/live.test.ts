import { Deferred, Effect, Fiber, Schema, Stream } from 'effect';
import { RecordId } from 'surrealdb';
import { expect, it } from 'vitest';
import { makeWith } from '../src/client/client.js';
import {
	LiveQueryClosed,
	StatementDecodeError,
	StatementEncodeError,
	StatementRequestError,
	StatementUnsupported,
	timed,
} from '../src/client/index.js';
import type { LiveHandle } from '../src/client/internal/transport.js';
import * as S from '../src/index.js';
import { gt, live, param, select } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { events, failure, fake, unsupported } from '../src/testing/index.js';

const ItemId = S.RecordIdFor('live_item');
const Item = {
	name: 'live_item',
	id: ItemId,
	select: Schema.Struct({ id: ItemId, n: S.Int }),
} as const;
const record = new RecordId('live_item', 'one');
const basic = live(Item).build('watch');
const filtered = live(Item)
	.where((row) => gt(row.n, param('min', S.Int)))
	.project((row) => ({ count: row.n }))
	.build('watch');

it('builds the same where and projection as select with a distinct live descriptor', () => {
	const ordinary = select(Item)
		.where((row) => gt(row.n, param('min', S.Int)))
		.project((row) => ({ count: row.n }))
		.build('watch');
	expect(filtered.sql).toBe(
		'LIVE SELECT n AS count FROM ⟨live_item⟩ WHERE n > $min;',
	);
	expect(filtered.sql).toBe(`LIVE ${ordinary.sql}`);
	expect(filtered.params).toEqual({ min: S.Int });
	expect(filtered.id).toBe(ItemId);
	expect(Object.keys(live(Item))).toEqual(['where', 'project', 'build']);
	expect(Schema.decodeUnknownSync(filtered.row)({ count: 7 })).toEqual({
		count: 7,
	});
});

it('acquires before the first pull, preserves typed order, and kills once on scope close', async () => {
	const changes = [
		{ action: 'CREATE', id: record, value: { count: 7 } },
		{ action: 'UPDATE', id: record, value: { count: 8 } },
		{ action: 'DELETE', id: record, value: { count: 8 } },
	] as const;
	const client = fake({ watch: events(changes) });
	await Effect.runPromise(
		Effect.scoped(
			Effect.gen(function* () {
				const subscribed = yield* client.liveScoped(filtered, {
					min: 3,
				});
				expect(client.calls).toEqual([
					{ statement: 'watch', bindings: { min: 3 } },
				]);
				yield* Effect.yieldNow;
				const received = yield* subscribed.pipe(
					Stream.take(changes.length),
					Stream.runCollect,
				);
				expect(received).toEqual(changes);
				expect(client.calls).toHaveLength(1);
			}),
		),
	);
	expect(client.calls).toEqual([
		{ statement: 'watch', bindings: { min: 3 } },
		{ statement: 'watch', bindings: {}, operation: 'kill' },
	]);
});

it.each([
	{ action: 'CREATE', id: record, value: { count: 7, leaked: true } },
	{ action: 'CREATE', id: new RecordId('other', 'one'), value: { count: 7 } },
] as const)('fails on a bad event row or id and automatically kills its subscription', async (invalid) => {
	const client = fake({ watch: events([invalid]) });
	const error = await Effect.runPromise(
		Effect.flip(Stream.runCollect(client.live(filtered, { min: 0 }))),
	);
	expect(error).toBeInstanceOf(StatementDecodeError);
	expect(error).toMatchObject({
		statement: 'watch',
		slot: 1,
		committed: false,
	});
	expect(client.calls.at(-1)?.operation).toBe('kill');
});

it('preserves a buffered change before reporting server-side KILLED', async () => {
	const client = fake({
		watch: events([
			{ action: 'CREATE', id: record, value: { id: record, n: 7 } },
			{ action: 'KILLED' },
		]),
	});
	const seen: unknown[] = [];
	const error = await Effect.runPromise(
		Effect.flip(
			Effect.scoped(
				Effect.gen(function* () {
					const changes = yield* client.liveScoped(basic);
					yield* Effect.yieldNow;
					yield* Stream.runForEach(changes, (event) =>
						Effect.sync(() => {
							seen.push(event);
						}),
					);
				}),
			),
		),
	);
	expect(seen).toEqual([
		{ action: 'CREATE', id: record, value: { id: record, n: 7 } },
	]);
	expect(error).toBeInstanceOf(LiveQueryClosed);
	expect(error).toMatchObject({ statement: 'watch', reason: 'killed' });
	expect(client.calls.at(-1)?.operation).toBe('kill');
});

it('reports an ended notification iterator as a disconnected live query', async () => {
	const client = fake({ watch: events([]) });
	const error = await Effect.runPromise(
		Effect.flip(Stream.runCollect(client.live(basic))),
	);
	expect(error).toMatchObject({
		_tag: 'LiveQueryClosed',
		reason: 'disconnected',
	});
	expect(client.calls.at(-1)?.operation).toBe('kill');
});

it('refuses an invalid binding before asking the transport to attach', async () => {
	const client = fake({ watch: events([]) });
	const broken = { min: 'oops' } as unknown as { min: number };
	const error = await Effect.runPromise(
		Effect.flip(Effect.scoped(client.liveScoped(filtered, broken))),
	);
	expect(error).toBeInstanceOf(StatementEncodeError);
	expect(error).toMatchObject({ binding: 'min' });
	expect(client.calls).toHaveLength(0);
});

it('preserves an attach refusal as a typed error and never kills an absent handle', async () => {
	for (const [reply, expected] of [
		[failure('could not attach'), StatementRequestError],
		[unsupported('live query'), StatementUnsupported],
	] as const) {
		const client = fake({ watch: reply });
		const error = await Effect.runPromise(
			Effect.flip(Effect.scoped(client.liveScoped(basic))),
		);
		expect(error).toBeInstanceOf(expected);
		expect(client.calls).toHaveLength(1);
	}
	const { transport } = scriptedTransport({});
	const { live: _, ...withoutLive } = transport;
	const client = makeWith(withoutLive);
	const error = await Effect.runPromise(
		Effect.flip(Effect.scoped(client.liveScoped(basic))),
	);
	expect(error).toMatchObject({
		_tag: 'StatementUnsupported',
		feature: 'live query',
	});
});

it('kills a handle that arrives after the acquiring fiber is interrupted', async () => {
	const { transport } = scriptedTransport({});
	const entered = Effect.runSync(Deferred.make<void>());
	const finish = Effect.runSync(Deferred.make<LiveHandle>());
	let killed = 0;
	const client = makeWith({
		...transport,
		live: () =>
			Effect.runPromise(
				Deferred.succeed(entered, undefined).pipe(
					Effect.andThen(Deferred.await(finish)),
				),
			),
	});
	const run = Effect.runFork(Effect.scoped(client.liveScoped(basic)));
	await Effect.runPromise(Deferred.await(entered));
	const interrupted = Effect.runPromise(Fiber.interrupt(run));
	await Effect.runPromise(
		Deferred.succeed(finish, {
			messages: { async *[Symbol.asyncIterator]() {} },
			kill: async () => {
				killed++;
			},
		}),
	);
	await interrupted;
	expect(killed).toBe(1);
});

it('the unscoped stream starts on its first pull and kills when downstream stops', async () => {
	const client = fake({
		watch: events([
			{ action: 'CREATE', id: record, value: { id: record, n: 7 } },
		]),
	});
	const subscribed = client.live(basic);
	expect(client.calls).toHaveLength(0);
	expect(
		await Effect.runPromise(
			subscribed.pipe(Stream.take(1), Stream.runCollect),
		),
	).toHaveLength(1);
	expect(client.calls).toEqual([
		{ statement: 'watch', bindings: {} },
		{ statement: 'watch', bindings: {}, operation: 'kill' },
	]);
});

it('times registration and kill separately without capturing the binding or event value', async () => {
	const statement = live(Item)
		.project(() => ({ content: param('label', S.String) }))
		.build('watch');
	const sent = 'secret parameter used by the caller';
	const received = 'private content emitted by the server';
	const client = fake({
		watch: events([
			{ action: 'CREATE', id: record, value: { content: received } },
		]),
	});
	const [changes, timings] = await Effect.runPromise(
		timed(
			Effect.scoped(
				Effect.gen(function* () {
					const stream = yield* client.liveScoped(statement, {
						label: sent,
					});
					return yield* Stream.runCollect(Stream.take(stream, 1));
				}),
			),
		),
	);
	expect(changes[0]?.value.content).toBe(received);
	expect(
		timings.map(({ kind, name, sends }) => ({ kind, name, sends })),
	).toEqual([
		{ kind: 'run', name: 'live(watch)', sends: 1 },
		{ kind: 'run', name: 'kill(watch)', sends: 1 },
	]);
	const report = JSON.stringify(timings);
	expect(report).not.toContain(sent);
	expect(report).not.toContain(received);
});
