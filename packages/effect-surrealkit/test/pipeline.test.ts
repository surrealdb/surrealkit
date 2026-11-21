import {
	Deferred,
	Duration,
	Effect,
	Exit,
	Fiber,
	Metric,
	Result,
	Schema,
	Tracer,
} from 'effect';
import { describe, expect, it, vi } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	bind,
	metrics,
	pipeline,
	StatementConflict,
	StatementEmptyResult,
	StatementEncodeError,
	StatementRequestError,
	StatementScopeError,
	StatementThrown,
	StatementWriteOutcomeError,
	timed,
} from '../src/client/index.js';
import type {
	SlotAnswer,
	Transport,
} from '../src/client/internal/transport.js';
import { make } from '../src/statement/index.js';
import { scriptedTransport } from '../src/testing/fake.js';
import { conflict, fake, fakeLayer, thrown } from '../src/testing/index.js';

const first = make({
	name: 'first',
	sql: '1;',
	params: {},
	result: Schema.Number,
	writes: false,
});
const word = make({
	name: 'word',
	sql: '$term;',
	params: { term: Schema.String },
	result: Schema.String,
	writes: false,
});
const last = make({
	name: 'last',
	sql: 'true;',
	params: {},
	result: Schema.Boolean,
	writes: false,
});
const mayThrow = make({
	name: 'guarded',
	sql: "THROW 'declared';",
	params: {},
	result: Schema.Undefined,
	throws: ['declared'],
	throwSites: { declared: { source: 'pipeline/guard.surql', lines: [5] } },
	writes: false,
});
const opensOwnTransaction = make({
	name: 'own transaction',
	sql: 'BEGIN; 2; COMMIT;',
	params: {},
	result: Schema.Number,
	valueSlot: 2,
	slots: 3,
	transactional: true,
	writes: false,
});
const writer = make({
	name: 'write with wrong result',
	sql: 'UPSERT test:one SET n = 1 RETURN NONE;',
	params: {},
	result: Schema.Number,
	writes: true,
});

const run = Effect.runPromise;

const withSend = (send: Transport['send']) => {
	const scripted = scriptedTransport({});
	return makeWith({ ...scripted.transport, send });
};

const answered = (value: unknown, server?: Duration.Duration): SlotAnswer => ({
	ok: true,
	value,
	...(server === undefined ? {} : { server }),
});

const failureOf = <A, E>(result: Result.Result<A, E>) => {
	expect(Result.isFailure(result)).toBe(true);
	return Result.isFailure(result) ? result.failure : undefined;
};

describe('pipeline result boundaries', () => {
	it('returns an empty tuple without sending anything', async () => {
		const send = vi.fn<Transport['send']>();
		const client = withSend(send);
		expect(await run(timed(client.pipeline([])))).toEqual([[], []]);
		expect(send).not.toHaveBeenCalled();
	});

	it('returns three different reads through the context accessor in order', async () => {
		const bounds = [
			bind(first),
			bind(word, { term: 'one' }),
			bind(last),
		] as const;
		expect(
			await run(
				pipeline(bounds).pipe(
					Effect.provide(
						fakeLayer({ first: 1, word: 'one', last: true }),
					),
				),
			),
		).toEqual([
			Result.succeed(1),
			Result.succeed('one'),
			Result.succeed(true),
		]);
	});

	it('sends one query, with transaction controls included in part offsets and bindings kept under their own names', async () => {
		const { transport, calls } = scriptedTransport({
			first: 1,
			word: ({ term }: { term: string }) => term,
			last: true,
		});
		const send = vi.fn(transport.send);
		const client = makeWith({ ...transport, send });
		const outcomes = await run(
			client.pipeline(
				[bind(first), bind(word, { term: 'two' }), bind(last)],
				{ each: 'transaction' },
			),
		);
		expect(outcomes).toEqual([
			Result.succeed(1),
			Result.succeed('two'),
			Result.succeed(true),
		]);
		expect(send).toHaveBeenCalledOnce();
		expect(send.mock.calls[0]?.[0]).toMatchObject({
			mode: 'query',
			sql: 'BEGIN;\n1;\nCOMMIT;\nBEGIN;\n$term;\nCOMMIT;\nBEGIN;\ntrue;\nCOMMIT;',
			bindings: { term: 'two' },
			parts: [
				{ offset: 0, slots: 3, bindings: {} },
				{
					offset: 3,
					slots: 3,
					bindings: { term: 'two' },
					sent: { term: 'term' },
				},
				{ offset: 6, slots: 3, bindings: {} },
			],
		});
		expect(calls).toEqual([
			{
				statement: 'first',
				bindings: {},
				pipeline: 'pipeline(first, word, last)',
			},
			{
				statement: 'word',
				bindings: { term: 'two' },
				pipeline: 'pipeline(first, word, last)',
			},
			{
				statement: 'last',
				bindings: {},
				pipeline: 'pipeline(first, word, last)',
			},
		]);
	});

	it('uses only the failed statement’s declared throw site', async () => {
		const left = make({
			...first,
			throws: ['declared'],
			throwSites: { declared: { source: 'elsewhere.surql', lines: [1] } },
		});
		const client = fake({
			first: 1,
			guarded: thrown('declared'),
			last: true,
		});
		const [before, middle, after] = await run(
			client.pipeline([bind(left), bind(mayThrow), bind(last)]),
		);
		expect(before).toEqual(Result.succeed(1));
		const error = failureOf(middle);
		expect(error).toBeInstanceOf(StatementThrown);
		expect(error).toMatchObject({
			statement: 'guarded',
			reason: 'declared',
			source: 'pipeline/guard.surql',
			lines: [5],
		});
		expect(after).toEqual(Result.succeed(true));
	});

	it('uses the first failed slot when every failure in the member is collateral', async () => {
		const firstError = new Error(
			'The query was not executed due to a failed transaction',
		);
		const secondError = new Error(
			'The query was not executed due to a cancelled transaction',
		);
		const client = withSend(async () => [
			answered(undefined),
			{ ok: false, error: firstError },
			{ ok: false, error: secondError },
			answered(undefined),
			answered(true),
			answered(undefined),
		]);
		const [failed, after] = await run(
			client.pipeline([bind(first), bind(last)], { each: 'transaction' }),
		);
		expect(failureOf(failed)).toMatchObject({
			_tag: 'StatementRequestError',
			statement: first.name,
			cause: firstError,
		});
		expect(after).toEqual(Result.succeed(true));
	});

	it('returns a scripted conflict per statement, with no replay', async () => {
		const client = fake({ first: conflict(), last: true });
		const [failed, succeeded] = await run(
			client.pipeline([bind(first), bind(last)]),
		);
		expect(failureOf(failed)).toBeInstanceOf(StatementConflict);
		expect(succeeded).toEqual(Result.succeed(true));
		expect(client.calls.map(({ statement }) => statement)).toEqual([
			'first',
			'last',
		]);
	});

	it('refuses nested transaction controls before sending and accepts them in statement mode', async () => {
		const client = fake({ 'own transaction': 2, last: true });
		const bounds = [bind(opensOwnTransaction), bind(last)] as const;
		const refused = await run(
			Effect.flip(client.pipeline(bounds, { each: 'transaction' })),
		);
		expect(refused).toBeInstanceOf(StatementScopeError);
		expect(refused.statement).toBe('own transaction');
		expect(client.calls).toHaveLength(0);
		expect(
			await run(client.pipeline(bounds, { each: 'statement' })),
		).toEqual([Result.succeed(2), Result.succeed(true)]);
	});

	it('reports an undecodable write as an acknowledged outcome for that statement alone', async () => {
		const client = fake({ [writer.name]: [], last: true });
		const [failed, succeeded] = await run(
			client.pipeline([bind(writer), bind(last)], {
				each: 'transaction',
			}),
		);
		const error = failureOf(failed);
		expect(error).toBeInstanceOf(StatementWriteOutcomeError);
		expect(error).toMatchObject({
			request: writer.name,
			cause: { statement: writer.name, slot: 1, committed: true },
		});
		expect(succeeded).toEqual(Result.succeed(true));
	});

	it('validates NONE slots, non-empty results and excess properties for each statement', async () => {
		const twoSlots = make({
			...first,
			sql: 'NONE; 1;',
			slots: 2,
			valueSlot: 2,
		});
		const nonEmpty = make({
			...last,
			name: 'no empty rows',
			result: Schema.Array(Schema.Number),
			nonEmpty: true,
		});
		const row = make({
			...first,
			name: 'exact row',
			result: Schema.Struct({ n: Schema.Number }),
		});
		const client = withSend(async () => [
			answered(1),
			answered(1),
			answered([]),
			answered({ n: 1, extra: true }),
		]);
		const [wrongNone, empty, excess] = await run(
			client.pipeline([bind(twoSlots), bind(nonEmpty), bind(row)]),
		);
		expect(failureOf(wrongNone)).toMatchObject({
			_tag: 'StatementDecodeError',
			slot: 1,
			committed: false,
		});
		expect(failureOf(empty)).toBeInstanceOf(StatementEmptyResult);
		expect(failureOf(excess)).toMatchObject({
			_tag: 'StatementDecodeError',
			slot: 1,
			committed: false,
		});
	});

	it.each([
		[1],
		[1, 2, 3],
	])('refuses a malformed answer (%j) without attributing slots to members', async (...values) => {
		const client = withSend(async () =>
			values.map((value) => answered(value, Duration.nanos(1n))),
		);
		const [[read, write], timings] = await run(
			timed(client.pipeline([bind(first), bind(writer)])),
		);
		expect(failureOf(read)).toMatchObject({
			_tag: 'StatementDecodeError',
			statement: first.name,
			slot: 0,
			committed: false,
		});
		expect(failureOf(write)).toMatchObject({
			_tag: 'StatementWriteOutcomeError',
			request: writer.name,
			cause: { statement: writer.name, slot: 0, committed: true },
		});
		expect(timings[0]?.server).toBeUndefined();
		expect(timings[0]?.statements).toEqual([
			{ statement: first.name },
			{ statement: writer.name },
		]);
	});
});

describe('pipeline request failures and interruption', () => {
	it('fails before sending if any binding fails its codec', async () => {
		const client = fake({ first: 1, word: 'unset' });
		const invalid = bind(word, { term: 2 as unknown as string });
		const error = await run(
			Effect.flip(client.pipeline([bind(first), invalid])),
		);
		expect(error).toBeInstanceOf(StatementEncodeError);
		expect(error).toMatchObject({ statement: 'word', binding: 'term' });
		expect(client.calls).toHaveLength(0);
	});

	it('checks wire values before sending unless explicitly skipped', async () => {
		const loose = make({ ...word, params: { term: Schema.Unknown } });
		const client = fake({ first: 1, word: 'sent' });
		const statements = [
			bind(first),
			bind(loose, { term: new Map() }),
		] as const;
		const error = await run(Effect.flip(client.pipeline(statements)));
		expect(error).toMatchObject({
			_tag: 'StatementEncodeError',
			statement: 'word',
			binding: 'term',
		});
		expect(client.calls).toHaveLength(0);
		expect(
			await run(
				client.pipeline(statements, { unsafeSkipValueCheck: true }),
			),
		).toEqual([Result.succeed(1), Result.succeed('sent')]);
		expect(client.calls).toHaveLength(2);
	});

	it('reports a rejected send through the outer effect', async () => {
		const lost = new Error('connection closed');
		const send = vi.fn<Transport['send']>().mockRejectedValue(lost);
		const client = withSend(send);
		const error = await run(
			Effect.flip(client.pipeline([bind(first), bind(last)])),
		);
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error).toMatchObject({
			statement: 'pipeline(first, last)',
			message: 'SurrealDB request failed',
			cause: lost,
		});
		expect(send).toHaveBeenCalledOnce();
	});

	it('aborts the one pending request on interruption', async () => {
		const started = await run(Deferred.make<AbortSignal>());
		const send = vi.fn<Transport['send']>((_request, signal) => {
			Deferred.doneUnsafe(started, Effect.succeed(signal));
			return new Promise((_resolve, reject) => {
				signal.addEventListener('abort', () => reject(signal.reason), {
					once: true,
				});
			});
		});
		const client = withSend(send);
		const fiber = Effect.runFork(
			client.pipeline([bind(first), bind(last)]),
		);
		const signal = await run(Deferred.await(started));
		expect(signal.aborted).toBe(false);
		await run(Fiber.interrupt(fiber));
		expect(Exit.hasInterrupts(await run(Fiber.await(fiber)))).toBe(true);
		expect(signal.aborted).toBe(true);
		expect(send).toHaveBeenCalledOnce();
	});
});

describe('pipeline observability', () => {
	it('records one client span and counts the outcome of each statement', async () => {
		const read = make({
			...word,
			name: 'pipeline metered read',
			sources: ['read.surql'],
		});
		const guard = make({
			...mayThrow,
			name: 'pipeline metered guard',
			sources: ['guard.surql'],
		});
		const name = `pipeline(${read.name}, ${guard.name})`;
		const client = fake({
			[read.name]: 'private result',
			[guard.name]: thrown('declared'),
		});
		const spans: Array<Tracer.Span> = [];
		const tracer = Tracer.make({
			span: (options) => {
				const span = new Tracer.NativeSpan(options);
				spans.push(span);
				return span;
			},
		});
		const runs = (statement: string, outcome: string) =>
			Metric.value(
				Metric.withAttributes(metrics.runs, { statement, outcome }),
			).pipe(Effect.map(({ count }) => Number(count)));
		const counts = await run(
			Effect.gen(function* () {
				yield* client.pipeline([
					bind(read, { term: 'private binding' }),
					bind(guard),
				]);
				return [
					yield* runs(read.name, 'success'),
					yield* runs(guard.name, 'StatementThrown'),
					yield* runs(name, 'success'),
				];
			}).pipe(Effect.withTracer(tracer)),
		);
		expect(counts).toEqual([1, 1, 0]);
		expect(spans.map((span) => span.name)).toEqual([name]);
		const attributes = spans[0]?.attributes ?? new Map();
		expect(Object.fromEntries(attributes)).toMatchObject({
			'db.operation.name': name,
			'db.query.text': "$term;\nTHROW 'declared';",
			'code.file.path': 'read.surql, guard.surql',
		});
		expect(JSON.stringify([...attributes])).not.toMatch(
			/private binding|private result/u,
		);
	});

	it('records the sum of all returned slot times, in part order', async () => {
		const list = make({
			...first,
			name: 'list',
			result: Schema.Array(Schema.Number),
		});
		const client = withSend(async () => [
			answered(undefined, Duration.nanos(1n)),
			answered([1, 2], Duration.nanos(2n)),
			answered(undefined, Duration.nanos(3n)),
			answered(undefined, Duration.nanos(4n)),
			{
				ok: false,
				error: new Error('failed'),
				server: Duration.nanos(5n),
			},
			answered(undefined),
		]);
		const [, timings] = await run(
			timed(
				client.pipeline([bind(list), bind(last)], {
					each: 'transaction',
				}),
			),
		);
		expect(timings).toMatchObject([
			{
				kind: 'pipeline',
				sends: 1,
				server: Duration.nanos(15n),
				statements: [
					{ statement: 'list', rows: 2, server: Duration.nanos(6n) },
					{ statement: 'last', server: Duration.nanos(9n) },
				],
			},
		]);
	});
});
