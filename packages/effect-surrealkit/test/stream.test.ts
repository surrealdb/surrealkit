import {
	Deferred,
	Duration,
	Effect,
	Exit,
	Fiber,
	Metric,
	Result,
	Schema,
	Stream,
	Tracer,
} from 'effect';
import { QueryError, ThrownError } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	metrics,
	StatementDecodeError,
	StatementEmptyResult,
	StatementEncodeError,
	StatementThrown,
	StatementUnsupported,
	StatementWriteOutcomeError,
	stream,
	timed,
} from '../src/client/index.js';
import type {
	Request,
	Signal,
	StreamFrame,
	Transport,
} from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { fake, fakeLayer, unsupported } from '../src/testing/index.js';

const numbers = make({
	name: 'numbers',
	sql: 'SELECT VALUE n FROM readings;',
	params: {},
	result: Schema.Array(Schema.Number),
	writes: false,
});
const withFrames = (frames: ReadonlyArray<StreamFrame>) => {
	const requests: Array<{
		readonly request: Request;
		readonly signal: Signal;
	}> = [];
	const client = withSource(async function* (request, signal) {
		requests.push({ request, signal });
		for (const frame of frames) yield frame;
	});
	return { client, requests };
};
const withSource = (stream: Transport['stream']) =>
	makeWith({ ...scriptedTransport({}).transport, stream });
const row = (value: unknown, slot = 0): StreamFrame => ({
	kind: 'row',
	slot,
	value,
});
const scalar = (value: unknown, slot = 0): StreamFrame => ({
	kind: 'value',
	slot,
	value,
});
const done = (slot = 0, server?: Duration.Duration): StreamFrame => ({
	kind: 'done',
	slot,
	...(server === undefined ? {} : { server }),
});
const run = Effect.runPromise;
const collect = (rows: Stream.Stream<unknown, unknown>) =>
	run(Stream.runCollect(rows));
const fail = (rows: Stream.Stream<unknown, unknown>) =>
	run(Effect.flip(Stream.runCollect(rows)));

const setOfNumbers = make({
	...numbers,
	name: 'require rows',
	nonEmpty: true,
});

describe('stream decoding', () => {
	it('decodes rows one at a time with the element codec and preserves input order', async () => {
		const statement = make({
			...numbers,
			result: Schema.Array(Schema.NumberFromString),
		});
		const effect = stream(statement).pipe(
			Stream.runCollect,
			Effect.provide(fakeLayer({ numbers: ['1', '2', '3'] })),
		);
		expect(await run(effect)).toEqual([1, 2, 3]);
	});

	it('starts the transport only when run and sends a single request with encoded bindings', async () => {
		const bound = make({
			...numbers,
			sql: '$n;',
			params: { n: Schema.NumberFromString },
		});
		const { client, requests } = withFrames([row(42), done()]);
		const rows = client.stream(bound, { n: 42 });
		expect(requests).toHaveLength(0);
		expect(await collect(rows)).toEqual([42]);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.request).toMatchObject({
			mode: 'query',
			sql: '$n;',
			bindings: { n: '42' },
			parts: [
				{
					statement: bound,
					offset: 0,
					slots: 1,
					bindings: { n: '42' },
					sent: { n: 'n' },
				},
			],
		});
	});

	it('refuses an unencodable or unsafe input before opening a stream', async () => {
		const bound = make({
			...numbers,
			sql: '$n;',
			params: { n: Schema.Number },
		});
		const broad = make({ ...bound, params: { n: Schema.Unknown } });
		const { client, requests } = withFrames([]);
		for (const stream of [
			client.stream(bound, { n: 'bad' as unknown as number }),
			client.stream(broad, { n: new Map() }),
		]) {
			const failure = await fail(stream);
			expect(failure).toBeInstanceOf(StatementEncodeError);
			expect(failure).toMatchObject({
				statement: numbers.name,
				binding: 'n',
			});
		}
		expect(requests).toHaveLength(0);
	});

	it('decodes from the declared slot and checks trailing slots as well', async () => {
		const statement = make({
			...numbers,
			sql: `NONE; ${numbers.sql} NONE;`,
			slots: 3,
			valueSlot: 2,
		});
		const { client } = withFrames([
			scalar(undefined),
			done(),
			row(1, 1),
			row(2, 1),
			done(1),
			scalar(undefined, 2),
			done(2),
		]);
		expect(await collect(client.stream(statement))).toEqual([1, 2]);
		const bad = withFrames([
			scalar(undefined),
			done(),
			row(1, 1),
			done(1),
			done(2),
		]);
		const error = await fail(bad.client.stream(statement));
		expect(error).toMatchObject({
			_tag: 'StatementDecodeError',
			slot: 3,
			message: expect.stringContaining('must be NONE'),
		});
	});

	it.each([
		undefined,
		null,
		2,
	])('requires the scalar value slot %j to be an array, as run does', async (value) => {
		const client = fake({ numbers: value });
		const error = await fail(client.stream(numbers));
		const collected = await run(Effect.flip(client.run(numbers)));
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(error).toMatchObject({
			slot: 1,
			committed: false,
			message: collected.message,
		});
	});

	it('accepts empty lists unless nonEmpty is declared, and honors nonEmpty for NULL/NONE', async () => {
		expect(await collect(fake({ numbers: [] }).stream(numbers))).toEqual(
			[],
		);
		for (const value of [undefined, null, []]) {
			const client = fake({ [setOfNumbers.name]: value });
			const error = await fail(client.stream(setOfNumbers));
			const collected = await run(Effect.flip(client.run(setOfNumbers)));
			expect(error).toBeInstanceOf(StatementEmptyResult);
			expect(error).toMatchObject({
				slot: 1,
				message: collected.message,
			});
		}
	});

	it('refuses array-wide refinements before sending', async () => {
		const result = Schema.Array(Schema.Number).check(Schema.isMinLength(2));
		const { client, requests } = withFrames([row(1), done()]);
		const error = await fail(client.stream(make({ ...numbers, result })));
		expect(error).toBeInstanceOf(StatementUnsupported);
		expect(error).toHaveProperty(
			'message',
			expect.stringContaining('plain Schema.Array'),
		);
		expect(requests).toHaveLength(0);
	});

	it('validates completed slot counts on exhaustion', async () => {
		const { client } = withFrames([row(1)]);
		const error = await fail(client.stream(numbers));
		expect(error).toMatchObject({
			_tag: 'StatementDecodeError',
			slot: 0,
			message:
				'expected 1 results, one per SurrealQL statement, and received 0',
		});
	});

	it('emits earlier rows before failing the strict element codec', async () => {
		const statement = make({
			...numbers,
			result: Schema.Array(Schema.Struct({ n: Schema.Number })),
		});
		const client = fake({
			numbers: [{ n: 1 }, { n: 2, extra: true }, { n: 3 }],
		});
		const rows: Array<unknown> = [];
		const error = await run(
			Effect.flip(
				Stream.runForEach(client.stream(statement), (row) =>
					Effect.sync(() => {
						rows.push(row);
					}),
				),
			),
		);
		expect(rows).toEqual([{ n: 1 }]);
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(error).toMatchObject({ slot: 1, committed: false });
	});

	it('reports an unknown write outcome when decoding fails before acknowledgment', async () => {
		const writer = make({ ...numbers, writes: true });
		const { client, requests } = withFrames([
			row(1),
			row('undecodable'),
			done(),
		]);
		const error = await fail(client.stream(writer));
		expect(error).toBeInstanceOf(StatementWriteOutcomeError);
		expect(error).toMatchObject({
			request: writer.name,
			cause: {
				slot: 1,
				committed: false,
				message: expect.stringContaining(
					'before the write outcome was acknowledged',
				),
			},
		});
		expect(requests[0]?.signal.aborted).toBe(true);
	});
});

describe('stream failures and lifetime', () => {
	const laterThrow = make({
		...numbers,
		sql: `${numbers.sql} THROW 'stream stopped';`,
		slots: 2,
		valueSlot: 1,
		throws: ['stream stopped'],
	});
	const throwFrame: StreamFrame = {
		kind: 'error',
		slot: 1,
		error: new ThrownError({
			kind: 'Thrown',
			message: 'An error occurred: stream stopped',
		}),
		server: Duration.nanos(5n),
	};

	it('reports a later thrown error after emitted rows and records one failed request', async () => {
		const { client, requests } = withFrames([
			row(7),
			done(0, Duration.nanos(3n)),
			throwFrame,
		]);
		const rows: Array<number> = [];
		const consumed = Stream.runForEach(client.stream(laterThrow), (row) =>
			Effect.sync(() => {
				rows.push(row);
			}),
		);
		const [result, timings] = await run(timed(Effect.result(consumed)));
		expect(rows).toEqual([7]);
		expect(Result.isFailure(result) && result.failure).toBeInstanceOf(
			StatementThrown,
		);
		expect(requests).toHaveLength(1);
		expect(timings).toMatchObject([
			{
				kind: 'stream',
				sends: 1,
				server: Duration.nanos(8n),
				statements: [
					{
						statement: laterThrow.name,
						rows: 1,
						server: Duration.nanos(8n),
					},
				],
			},
		]);
	});

	it('uses the substantive error after a collateral failure', async () => {
		const skipped = new QueryError({
			kind: 'Query',
			message: 'The query was not executed due to a failed transaction',
			details: { kind: 'NotExecuted' },
		});
		const { client } = withFrames([
			{ kind: 'error', slot: 0, error: skipped },
			throwFrame,
		]);
		expect(await fail(client.stream(laterThrow))).toBeInstanceOf(
			StatementThrown,
		);
	});

	it('maps the SDK unsupported stream error', async () => {
		const client = fake({ numbers: unsupported('stream') });
		expect(await fail(client.stream(numbers))).toBeInstanceOf(
			StatementUnsupported,
		);
	});

	it('does not pull while the downstream consumer is suspended', async () => {
		let pulled = 0;
		const client = withSource(async function* () {
			for (const frame of [row(1), row(2), done()]) {
				pulled++;
				yield frame;
			}
		});
		await run(
			Effect.gen(function* () {
				const entered = yield* Deferred.make<void>();
				const release = yield* Deferred.make<void>();
				const fiber = yield* Effect.forkChild(
					Stream.runForEach(client.stream(numbers), (value) =>
						Effect.gen(function* () {
							if (value !== 1) return;
							yield* Deferred.succeed(entered, undefined);
							yield* Deferred.await(release);
						}),
					),
				);
				yield* Deferred.await(entered);
				expect(pulled).toBe(1);
				yield* Deferred.succeed(release, undefined);
				yield* Fiber.join(fiber);
			}),
		);
		expect(pulled).toBe(3);
	});

	it('aborts a pending iterator read and finalizes the generator on interruption', async () => {
		await run(
			Effect.gen(function* () {
				const opened = yield* Deferred.make<Signal>();
				const closed = yield* Deferred.make<void>();
				const client = withSource(async function* (_request, signal) {
					try {
						await new Promise<void>((_resolve, reject) => {
							signal.addEventListener(
								'abort',
								() => reject(signal.reason),
								{ once: true },
							);
							Deferred.doneUnsafe(opened, Effect.succeed(signal));
						});
						yield done();
					} finally {
						Deferred.doneUnsafe(closed, Effect.void);
					}
				});
				const fiber = yield* Effect.forkChild(
					Stream.runCollect(client.stream(numbers)),
				);
				const signal = yield* Deferred.await(opened);
				const exit = yield* Fiber.interrupt(fiber).pipe(
					Effect.andThen(Fiber.await(fiber)),
				);
				expect(Exit.hasInterrupts(exit)).toBe(true);
				expect(signal.aborted).toBe(true);
				yield* Deferred.await(closed);
			}),
		);
	});

	it('records one stream span and the statement outcome without row or binding values', async () => {
		const statement = make({
			...numbers,
			name: 'observable stream',
			sql: '[$secret];',
			params: { secret: Schema.String },
			result: Schema.Array(Schema.String),
		});
		const client = fake({ [statement.name]: ['private answer'] });
		const spans: Array<Tracer.Span> = [];
		const tracer = Tracer.make({
			span: (options) => {
				const span = new Tracer.NativeSpan(options);
				spans.push(span);
				return span;
			},
		});
		const count = await run(
			Effect.gen(function* () {
				yield* Stream.runDrain(
					client.stream(statement, { secret: 'private input' }),
				);
				return (yield* Metric.value(
					Metric.withAttributes(metrics.runs, {
						statement: statement.name,
						outcome: 'success',
					}),
				)).count;
			}).pipe(Effect.withTracer(tracer)),
		);
		expect(count).toBe(1);
		expect(spans.map(({ name }) => name)).toEqual([statement.name]);
		const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
		expect(attributes).toMatchObject({
			'db.response.returned_rows': 1,
			'effect_surrealkit.sends': 1,
		});
		expect(JSON.stringify(attributes)).not.toMatch(
			/private input|private answer/u,
		);
	});

	// Effect ignores attributes on an ended span, so each way a stream ends
	// must record its rows and outcome before the span ends.
	const tracing = () => {
		const spans: Array<Tracer.Span> = [];
		const tracer = Tracer.make({
			span: (options) => {
				const span = new Tracer.NativeSpan(options);
				spans.push(span);
				return span;
			},
		});
		const span = (name: string) => spans.find((span) => span.name === name);
		const attributes = (name: string) =>
			Object.fromEntries(span(name)?.attributes ?? []);
		return { tracer, span, attributes };
	};
	const runs = (statement: string, outcome: string) =>
		Metric.value(
			Metric.withAttributes(metrics.runs, { statement, outcome }),
		).pipe(Effect.map(({ count }) => count));

	it('annotates each concatenated stream span before it ends', async () => {
		const trace = tracing();
		const first = make({ ...numbers, name: 'first of two streams' });
		const second = make({ ...numbers, name: 'second of two streams' });
		const { client } = withFrames([row(7), done()]);
		const rows = await run(
			Stream.runCollect(
				Stream.concat(client.stream(first), client.stream(second)),
			).pipe(Effect.withTracer(trace.tracer)),
		);
		expect(rows).toEqual([7, 7]);
		for (const { name } of [first, second]) {
			expect(trace.attributes(name)).toMatchObject({
				'db.response.returned_rows': 1,
				'effect_surrealkit.sends': 1,
			});
		}
		const ended = trace.span(first.name)?.status;
		const started = trace.span(second.name)?.status;
		expect(ended?._tag).toBe('Ended');
		if (ended?._tag === 'Ended' && started !== undefined)
			expect(ended.endTime <= started.startTime).toBe(true);
	});

	it('records a stream stopped early by its consumer as a success', async () => {
		const trace = tracing();
		const statement = make({ ...numbers, name: 'stream stopped early' });
		const { client } = withFrames([row(1), row(2), row(3), done()]);
		const result = await run(
			Effect.gen(function* () {
				const rows = yield* Stream.runCollect(
					client.stream(statement).pipe(Stream.take(1)),
				);
				return {
					rows,
					success: yield* runs(statement.name, 'success'),
				};
			}).pipe(Effect.withTracer(trace.tracer)),
		);
		expect(result).toEqual({ rows: [1], success: 1 });
		expect(trace.attributes(statement.name)).toMatchObject({
			'db.response.returned_rows': 1,
			'effect_surrealkit.sends': 1,
		});
	});

	it('records an interrupted stream as interrupted on its span and metrics', async () => {
		const trace = tracing();
		const statement = make({ ...numbers, name: 'interrupted stream' });
		const client = withSource(async function* (_request, signal) {
			yield row(1);
			await new Promise<void>((_resolve, reject) => {
				signal.addEventListener('abort', () => reject(signal.reason), {
					once: true,
				});
			});
		});
		const counts = await run(
			Effect.gen(function* () {
				const first = yield* Deferred.make<void>();
				const fiber = yield* Effect.forkChild(
					Stream.runDrain(
						client
							.stream(statement)
							.pipe(
								Stream.tap(() =>
									Deferred.succeed(first, undefined),
								),
							),
					),
				);
				yield* Deferred.await(first);
				const exit = yield* Fiber.interrupt(fiber).pipe(
					Effect.andThen(Fiber.await(fiber)),
				);
				expect(Exit.hasInterrupts(exit)).toBe(true);
				return {
					interrupted: yield* runs(statement.name, 'interrupted'),
					success: yield* runs(statement.name, 'success'),
				};
			}).pipe(Effect.withTracer(trace.tracer)),
		);
		expect(counts).toEqual({ interrupted: 1, success: 0 });
		const attributes = trace.attributes(statement.name);
		expect(attributes).toMatchObject({
			'db.response.returned_rows': 1,
			'effect_surrealkit.sends': 1,
		});
		expect(attributes['error.type']).toBeUndefined();
		const status = trace.span(statement.name)?.status;
		expect(
			status?._tag === 'Ended' && Exit.hasInterrupts(status.exit),
		).toBe(true);
	});
});
