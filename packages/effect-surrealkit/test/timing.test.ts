import {
	Deferred,
	Duration,
	Effect,
	Fiber,
	Metric,
	Schedule,
	Schema,
	SchemaGetter,
	Tracer,
} from 'effect';
import { TestClock } from 'effect/testing';
import { expect, it } from 'vitest';

import { bind, makeWith } from '../src/client/client.js';
import { timed } from '../src/client/index.js';
import { recording, traced } from '../src/client/internal/trace.js';
import type { Transport } from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';

const clientWith = (collect: Transport['collect']) =>
	makeWith({
		collect,
		send: () => Promise.reject(new Error('expected collect')),
		stream: () => {
			throw new Error('expected collect');
		},
	});

const tracing = () => {
	const spans: Array<Tracer.Span> = [];
	const tracer = Tracer.make({
		span: (options) => {
			const span = new Tracer.NativeSpan(options);
			spans.push(span);
			return span;
		},
	});
	return { spans, tracer };
};

const replyGate = Effect.gen(function* () {
	const started = yield* Deferred.make<void>();
	const reply = yield* Deferred.make<unknown, Error>();
	return {
		started: Deferred.await(started),
		succeed: (answer: unknown) => Deferred.succeed(reply, answer),
		fail: (error: Error) => Deferred.fail(reply, error),
		collect: () => {
			Effect.runSync(Deferred.succeed(started, undefined));
			return Effect.runPromise(Deferred.await(reply));
		},
	};
});

const scalar = (name: string) =>
	make({ name, sql: '1;', params: {}, result: Schema.Number, writes: false });

const delayString = (encodeMs: number, decodeMs: number) =>
	Schema.String.pipe(
		Schema.decodeTo(Schema.String, {
			encode: SchemaGetter.transformEffect((value) =>
				TestClock.adjust(Duration.millis(encodeMs)).pipe(
					Effect.as(value),
				),
			),
			decode: SchemaGetter.transformEffect((value) =>
				TestClock.adjust(Duration.millis(decodeMs)).pipe(
					Effect.as(value),
				),
			),
		}),
	);

it('counts retries independently when the same run or transaction effect is reused', async () => {
	const client = clientWith(() => Promise.resolve([1]));
	const statement = scalar('reusable');
	const operations = [
		Effect.asVoid(client.run(statement)),
		Effect.asVoid(client.transaction([bind(statement)])),
	];
	const snapshots = await Effect.runPromise(
		Effect.gen(function* () {
			for (const run of operations) {
				yield* run;
				yield* Effect.all([run, run], { concurrency: 2 });
			}
			return yield* Metric.snapshot;
		}).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
	);
	expect(
		snapshots.filter(
			({ id }) => id === 'effect_surrealkit_statement_retries',
		),
	).toEqual([]);
});

it('measures each phase on the Effect clock and reports the last conflict attempt', async () => {
	const { spans, tracer } = tracing();
	const name = 'read secret';
	const secret = 'private binding 8b85ab';
	const hidden = 'private result 788d23';
	const statement = make({
		name,
		sql: '[$secret];',
		params: { secret: delayString(2, 0) },
		result: Schema.Array(delayString(0, 3)),
	});
	const { measured, snapshots } = await Effect.runPromise(
		Effect.gen(function* () {
			const first = yield* replyGate;
			const last = yield* replyGate;
			const replies = [first, last];
			let sends = 0;
			const client = clientWith(() => {
				const reply = replies[sends++];
				if (reply === undefined) throw new Error('unexpected send');
				return reply.collect();
			});
			const fiber = yield* Effect.forkChild(
				timed(
					client.run(
						statement,
						{ secret },
						{ retry: Schedule.recurs(1) },
					),
				),
			);
			yield* first.started;
			yield* TestClock.adjust('5 millis');
			yield* first.fail(
				Object.assign(new Error('conflict'), {
					isTransactionConflict: true,
				}),
			);
			yield* last.started;
			yield* TestClock.adjust('7 millis');
			yield* last.succeed([[hidden]]);
			const measured = yield* Fiber.join(fiber);
			return { measured, snapshots: yield* Metric.snapshot };
		}).pipe(
			Effect.provide(TestClock.layer()),
			Effect.provideService(Metric.MetricRegistry, new Map()),
			Effect.withTracer(tracer),
		),
	);

	const [value, requests] = measured;
	expect(value).toEqual([hidden]);
	expect(requests).toEqual([
		{
			kind: 'run',
			name,
			sends: 2,
			encode: Duration.nanos(2_000_000n),
			wait: Duration.nanos(7_000_000n),
			decode: Duration.nanos(3_000_000n),
			total: Duration.nanos(17_000_000n),
			statements: [{ statement: name, rows: 1 }],
		},
	]);
	const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
	expect(spans.map(({ name }) => name)).toEqual([name]);
	expect(attributes).toMatchObject({
		'effect_surrealkit.encode_us': 2_000,
		'effect_surrealkit.wait_us': 7_000,
		'effect_surrealkit.decode_us': 3_000,
		'effect_surrealkit.sends': 2,
		'db.response.returned_rows': 1,
	});
	expect(attributes).not.toHaveProperty('effect_surrealkit.server_us');
	expect(snapshots).toEqual(
		expect.arrayContaining([
			expect.objectContaining({
				id: 'effect_surrealkit_statement_encode_duration',
				attributes: { statement: name, time_unit: 'milliseconds' },
				state: expect.objectContaining({ count: 1, sum: 2 }),
			}),
			expect.objectContaining({
				id: 'effect_surrealkit_statement_decode_duration',
				attributes: { statement: name, time_unit: 'milliseconds' },
				state: expect.objectContaining({ count: 1, sum: 3 }),
			}),
			expect.objectContaining({
				id: 'effect_surrealkit_statement_retries',
				attributes: { statement: name },
				state: expect.objectContaining({ count: 1 }),
			}),
		]),
	);
	expect(
		snapshots.some(
			({ id }) => id === 'effect_surrealkit_statement_server_duration',
		),
	).toBe(false);
	const telemetry = JSON.stringify([
		attributes,
		snapshots.map(({ attributes }) => attributes),
	]);
	expect(telemetry).not.toContain(secret);
	expect(telemetry).not.toContain(hidden);
});

it('collects child requests in completion order and reuses nested scopes safely', async () => {
	const program = timed(
		Effect.gen(function* () {
			const slow = yield* replyGate;
			const client = clientWith((request) =>
				request.parts[0]?.statement.name === 'slow'
					? slow.collect()
					: Promise.resolve([1]),
			);
			const fiber = yield* Effect.forkChild(client.run(scalar('slow')));
			yield* slow.started;
			const inner = yield* timed(client.run(scalar('fast')));
			yield* slow.succeed([1]);
			yield* Fiber.join(fiber);
			return inner;
		}),
	);
	for (let iteration = 0; iteration < 2; iteration++) {
		const [[value, inner], outer] = await Effect.runPromise(program);
		expect(value).toBe(1);
		expect(inner.map(({ name }) => name)).toEqual(['fast']);
		expect(outer.map(({ name }) => name)).toEqual(['fast', 'slow']);
	}
});

it('records late detached completions only in collectors still open', async () => {
	const [inner, outer] = await Effect.runPromise(
		timed(
			Effect.gen(function* () {
				const reply = yield* replyGate;
				const client = clientWith(reply.collect);
				const [fiber, inner] = yield* timed(
					Effect.gen(function* () {
						const fiber = yield* Effect.forkDetach(
							client.run(scalar('detached')),
						);
						yield* reply.started;
						return fiber;
					}),
				);
				yield* reply.succeed([1]);
				yield* Fiber.join(fiber);
				return inner;
			}),
		),
	);
	expect(inner).toEqual([]);
	expect(outer.map(({ name }) => name)).toEqual(['detached']);
});

it('returns an empty list when no request is sent and propagates errors unchanged', async () => {
	const error = new Error('original error');
	const client = clientWith(() => Promise.reject(error));
	await expect(Effect.runPromise(timed(Effect.succeed(42)))).resolves.toEqual(
		[42, []],
	);
	await expect(
		Effect.runPromise(timed(client.transaction([]))),
	).resolves.toEqual([[], []]);
	await expect(Effect.runPromise(timed(Effect.fail(error)))).rejects.toBe(
		error,
	);
	const [, failures] = await Effect.runPromise(
		timed(Effect.result(client.run(scalar('failed send')))),
	);
	expect(failures).toMatchObject([{ name: 'failed send', sends: 1 }]);
});

it('keeps duplicate and comma-bearing transaction parts and their own row counts', async () => {
	const client = clientWith(async () => [1, [1, 2], [], 1]);
	const number = scalar('read, one');
	const list = make({
		...number,
		sql: '[1, 2];',
		result: Schema.Array(Schema.Number),
	});
	const empty = make({ ...list, name: 'empty', sql: '[];' });
	const [, requests] = await Effect.runPromise(
		timed(
			client.transaction([
				bind(number),
				bind(list),
				bind(empty),
				bind(number),
			]),
		),
	);
	expect(requests).toMatchObject([
		{
			kind: 'transaction',
			sends: 1,
			statements: [
				{ statement: 'read, one' },
				{ statement: 'read, one', rows: 2 },
				{ statement: 'empty', rows: 0 },
				{ statement: 'read, one' },
			],
		},
	]);
	expect(requests[0]?.statements).toEqual([
		{ statement: 'read, one' },
		{ statement: 'read, one', rows: 2 },
		{ statement: 'empty', rows: 0 },
		{ statement: 'read, one' },
	]);
});

it('updates spans and server timers without a timing collector', async () => {
	const { spans, tracer } = tracing();
	const snapshots = await Effect.runPromise(
		Effect.gen(function* () {
			const measured = recording(
				'pipeline',
				'pipeline(read, read)',
				(recorder) =>
					Effect.gen(function* () {
						yield* recorder.encode(TestClock.adjust('2 millis'));
						yield* recorder.wait(TestClock.adjust('7 millis'));
						yield* recorder.server('read', Duration.millis(1));
						yield* recorder.server('read', Duration.millis(2));
						yield* recorder.decode(TestClock.adjust('3 millis'));
					}),
			);
			yield* Effect.withSpan(measured, 'pipeline(read, read)');
			return yield* Metric.snapshot;
		}).pipe(
			Effect.provide(TestClock.layer()),
			Effect.provideService(Metric.MetricRegistry, new Map()),
			Effect.withTracer(tracer),
		),
	);
	const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
	expect(attributes).toEqual({
		'effect_surrealkit.encode_us': 2_000,
		'effect_surrealkit.wait_us': 7_000,
		'effect_surrealkit.server_us': 3_000,
		'effect_surrealkit.decode_us': 3_000,
		'effect_surrealkit.sends': 1,
	});
	expect(
		snapshots.find(
			({ id }) => id === 'effect_surrealkit_statement_server_duration',
		),
	).toMatchObject({
		attributes: {
			statement: 'pipeline(read, read)',
			time_unit: 'milliseconds',
		},
		state: { count: 1, sum: 3 },
	});
});

it('uses one grouped span and counts each member outcome in the existing runs metric', async () => {
	const { spans, tracer } = tracing();
	const names = ['read, one', 'read, one', 'third read'];
	const grouped = <A>(
		effect: Effect.Effect<A, { readonly _tag: string }>,
		outcome: (result: A, index: number) => string,
	) =>
		traced('group', '1; 2; 3;', undefined, undefined, false, effect, {
			names,
			outcome,
		});
	const snapshots = await Effect.runPromise(
		Effect.gen(function* () {
			yield* grouped(
				Effect.succeed(['success', 'StatementThrown', 'success']),
				(result, index) => result[index] ?? 'missing',
			);
			yield* Effect.result(
				grouped(
					Effect.fail({ _tag: 'StatementRequestError' }),
					() => 'unexpected',
				),
			);
			const interrupted = yield* Effect.forkChild(
				grouped(Effect.interrupt, () => 'unexpected'),
			);
			yield* Fiber.await(interrupted);
			return yield* Metric.snapshot;
		}).pipe(
			Effect.provideService(Metric.MetricRegistry, new Map()),
			Effect.withTracer(tracer),
		),
	);
	expect(spans.map(({ name }) => name)).toEqual(['group', 'group', 'group']);
	const counted = snapshots.filter(
		({ id }) => id === 'effect_surrealkit_statement_runs',
	);
	expect(counted).toHaveLength(7);
	const expectCount = (statement: string, outcome: string, count: number) => {
		expect(
			counted.find(
				({ attributes }) =>
					attributes?.statement === statement &&
					attributes?.outcome === outcome,
			),
		).toMatchObject({ state: { count } });
	};
	expectCount('read, one', 'success', 1);
	expectCount('read, one', 'StatementThrown', 1);
	expectCount('third read', 'success', 1);
	expectCount('read, one', 'StatementRequestError', 2);
	expectCount('third read', 'StatementRequestError', 1);
	expectCount('read, one', 'interrupted', 2);
	expectCount('third read', 'interrupted', 1);
});
