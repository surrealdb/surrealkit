/**
 * The client against a stand-in for the SDK: what is sent, what is refused
 * before sending, and how answers and failures are mapped.
 */
import { Effect, Fiber, Result, Schema, Tracer } from 'effect';
import { DateTime } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import {
	bind,
	layer,
	make as makeClient,
	type Queryable,
	run,
	StatementConflict,
	StatementDecodeError,
	StatementEncodeError,
	StatementRequestError,
	StatementThrown,
	transaction,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import { make, part, sequence } from '../src/statement/index.js';

interface Call {
	readonly sql: string;
	readonly bindings: Record<string, unknown>;
	readonly atomic: boolean;
	readonly retry: unknown;
	readonly signal: AbortSignal | undefined;
}

/** An SDK stand-in that records each request and answers with `answer`. */
const fake = (answer: (call: Call) => Promise<unknown>) => {
	const calls: Array<Call> = [];
	const db = {
		query: (sql: string, bindings: Record<string, unknown> = {}) => {
			let signal: AbortSignal | undefined;
			let retry: unknown;
			const request = {
				signal(value: AbortSignal | undefined) {
					signal = value;
					return request;
				},
				retry(value: unknown) {
					retry = value;
					return request;
				},
				// biome-ignore lint/suspicious/noThenProperty: the SDK's query is a thenable
				then(
					resolve: (value: unknown) => unknown,
					reject: (reason: unknown) => unknown,
				) {
					const call = {
						sql,
						bindings,
						atomic: false,
						retry,
						signal,
					};
					calls.push(call);
					return answer(call).then(resolve, reject);
				},
			};
			return request;
		},
		transaction: (
			queries: ReadonlyArray<{
				readonly query: string;
				readonly bindings: Record<string, unknown>;
			}>,
			options?: {
				readonly retry?: unknown;
				readonly signal?: AbortSignal;
			},
		) => {
			const [first] = queries;
			const call = {
				sql: first?.query ?? '',
				bindings: first?.bindings ?? {},
				atomic: true,
				retry: options?.retry,
				signal: options?.signal,
			};
			calls.push(call);
			return answer(call);
		},
	};
	return { calls, client: makeClient(db as unknown as Queryable), db };
};

const failure = <A, E>(effect: Effect.Effect<A, E>): Promise<E> =>
	Effect.runPromise(Effect.flip(effect));

const serverError = (message: string, fields: Record<string, unknown>) =>
	Object.assign(new Error(message), fields);

const add = make({
	name: 'add',
	sql: 'RETURN $a + $b;',
	params: { a: SurrealSchema.Int, b: SurrealSchema.Int },
	result: SurrealSchema.Int,
});

const guarded = sequence(
	part({
		sql: 'LET $owner = $boundOwner;',
		free: ['boundOwner'],
		defines: ['owner'],
	}),
	part({
		sql: "IF $owner != $auth.id { THROW 'Not the owner' };",
		free: ['owner'],
		throws: ['Not the owner'],
	}),
	part({ sql: 'RETURN $owner;', free: ['owner'] }),
).define({
	name: 'guarded',
	params: { boundOwner: SurrealSchema.String },
	result: SurrealSchema.String,
});

describe('run', () => {
	it('sends the static text with encoded bindings and decodes the answer', async () => {
		const { calls, client } = fake(async () => [3]);
		expect(await Effect.runPromise(client.run(add, { a: 1, b: 2 }))).toBe(
			3,
		);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.sql).toBe('RETURN $a + $b;');
		expect(calls[0]?.bindings).toEqual({ a: 1, b: 2 });
		expect(calls[0]?.atomic).toBe(false);
	});

	it('encodes each binding through its codec', async () => {
		const at = make({
			sql: 'RETURN $at;',
			params: { at: SurrealSchema.DateTimeEffect },
			result: SurrealSchema.DateTimeEffect,
		});
		const sent = new DateTime('2026-01-02T03:04:05Z');
		const { calls, client } = fake(async () => [sent]);
		const decoded = await Effect.runPromise(
			client.run(at, {
				at: Schema.decodeUnknownSync(SurrealSchema.DateTimeEffect)(
					sent,
				),
			}),
		);
		// The caller holds Effect values; the engine receives SDK values.
		expect(calls[0]?.bindings.at).toBeInstanceOf(DateTime);
		expect(decoded).not.toBeInstanceOf(DateTime);
	});

	it('sends NONE for an optional binding that is left out', async () => {
		const optional = make({
			sql: 'RETURN $filter;',
			params: { filter: Schema.UndefinedOr(SurrealSchema.String) },
			result: Schema.UndefinedOr(SurrealSchema.String),
		});
		const { calls, client } = fake(async () => [undefined]);
		expect(await Effect.runPromise(client.run(optional))).toBeUndefined();
		expect(calls[0]?.bindings).toEqual({ filter: undefined });
	});

	it('refuses a value its codec refuses, before anything is sent', async () => {
		const { calls, client } = fake(async () => [3]);
		const error = await failure(client.run(add, { a: 1, b: 2 ** 60 }));
		expect(error).toBeInstanceOf(StatementEncodeError);
		expect(error).toMatchObject({ statement: 'add', binding: 'b' });
		expect(calls).toHaveLength(0);
	});

	it('refuses an answer that is not what the statement declares', async () => {
		const wrongCount = await failure(
			fake(async () => [1, 2]).client.run(add, { a: 1, b: 2 }),
		);
		expect(wrongCount).toBeInstanceOf(StatementDecodeError);
		expect(wrongCount).toMatchObject({ slot: 0 });

		const wrongType = await failure(
			fake(async () => ['3']).client.run(add, { a: 1, b: 2 }),
		);
		expect(wrongType).toBeInstanceOf(StatementDecodeError);
		expect(wrongType).toMatchObject({ slot: 1 });

		// Only the last result is data: an earlier one must be NONE.
		const early = await failure(
			fake(async () => [undefined, 'leak', 'owner']).client.run(guarded, {
				boundOwner: 'owner',
			}),
		);
		expect(early).toBeInstanceOf(StatementDecodeError);
		expect(early).toMatchObject({ slot: 2 });

		const strict = make({
			sql: 'RETURN {};',
			params: {},
			result: Schema.Struct({ a: SurrealSchema.Int }),
		});
		const excess = await failure(
			fake(async () => [{ a: 1, b: 2 }]).client.run(strict),
		);
		expect(excess).toBeInstanceOf(StatementDecodeError);
	});

	it('decodes the last result of a multi-statement text', async () => {
		const { client } = fake(async () => [undefined, undefined, 'owner']);
		expect(
			await Effect.runPromise(
				client.run(guarded, { boundOwner: 'owner' }),
			),
		).toBe('owner');
	});

	it('maps a declared THROW to a typed error, wherever the SDK nests it', async () => {
		const thrown = serverError('An error occurred: Not the owner', {
			kind: 'Thrown',
		});
		const direct = await failure(
			fake(() => Promise.reject(thrown)).client.run(guarded, {
				boundOwner: 'x',
			}),
		);
		expect(direct).toBeInstanceOf(StatementThrown);
		expect(direct).toMatchObject({
			statement: 'guarded',
			reason: 'Not the owner',
			message: 'Not the owner',
		});

		const nested = new Error('query failed', { cause: thrown });
		expect(
			await failure(
				fake(() => Promise.reject(nested)).client.run(guarded, {
					boundOwner: 'x',
				}),
			),
		).toBeInstanceOf(StatementThrown);
	});

	it('leaves an undeclared THROW as a request failure', async () => {
		const other = serverError('An error occurred: Something else', {
			kind: 'Thrown',
		});
		const error = await failure(
			fake(() => Promise.reject(other)).client.run(guarded, {
				boundOwner: 'x',
			}),
		);
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error).toMatchObject({ cause: other });
	});

	it('maps a transaction conflict to its own, retryable error', async () => {
		const conflict = serverError('Cannot COMMIT: Transaction conflict', {
			kind: 'Query',
			isTransactionConflict: true,
		});
		const error = await failure(
			fake(() => Promise.reject(conflict)).client.run(add, {
				a: 1,
				b: 2,
			}),
		);
		expect(error).toBeInstanceOf(StatementConflict);
	});

	it('hands the retry option to the SDK', async () => {
		const { calls, client } = fake(async () => [3]);
		await Effect.runPromise(
			client.run(add, { a: 1, b: 2 }, { retry: { attempts: 3 } }),
		);
		expect(calls[0]?.retry).toEqual({ attempts: 3 });
	});

	it('aborts the request when the effect is interrupted', async () => {
		const { calls, client } = fake(() => new Promise(() => undefined));
		await Effect.runPromise(
			Effect.gen(function* () {
				const fiber = yield* Effect.forkChild(
					client.run(add, { a: 1, b: 2 }),
				);
				yield* Effect.sleep('20 millis');
				expect(calls[0]?.signal?.aborted).toBe(false);
				yield* Fiber.interrupt(fiber);
			}),
		);
		expect(calls[0]?.signal?.aborted).toBe(true);
	});

	it('records a span named after the statement, with text and no values', async () => {
		const spans: Array<Tracer.Span> = [];
		const tracer = Tracer.make({
			span: (options) => {
				const span = new Tracer.NativeSpan(options);
				spans.push(span);
				return span;
			},
		});
		const { client } = fake(async () => [3]);
		await Effect.runPromise(
			client.run(add, { a: 41, b: 1 }).pipe(Effect.withTracer(tracer)),
		);
		expect(spans).toHaveLength(1);
		expect(spans[0]?.name).toBe('add');
		expect(spans[0]?.kind).toBe('client');
		expect(Object.fromEntries(spans[0]?.attributes ?? [])).toEqual({
			'db.system.name': 'surrealdb',
			'db.operation.name': 'add',
			'db.query.text': 'RETURN $a + $b;',
		});
	});
});

describe('transaction', () => {
	const double = make({
		name: 'double',
		sql: 'SELECT VALUE $n * 2 FROM ONLY $n;',
		params: { n: SurrealSchema.Int },
		result: SurrealSchema.Int,
	});

	it('sends every statement as one atomic request and decodes each', async () => {
		const { calls, client } = fake(async () => [
			undefined,
			undefined,
			'owner',
			4,
		]);
		const results = await Effect.runPromise(
			client.transaction(
				[
					bind(guarded, { boundOwner: 'owner' }),
					bind(double, { n: 2 }),
				],
				{ retry: true },
			),
		);
		expect(results).toEqual(['owner', 4]);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.atomic).toBe(true);
		expect(calls[0]?.sql).toBe(`${guarded.sql}\n${double.sql}`);
		expect(calls[0]?.bindings).toEqual({ boundOwner: 'owner', n: 2 });
		expect(calls[0]?.retry).toBe(true);
		expect(calls[0]?.signal).toBeInstanceOf(AbortSignal);
	});

	it('shares one variable scope: a name may repeat only with one value', async () => {
		const same = fake(async () => [4, 4]);
		await Effect.runPromise(
			same.client.transaction([
				bind(double, { n: 2 }),
				bind(double, { n: 2 }),
			]),
		);
		expect(same.calls[0]?.bindings).toEqual({ n: 2 });

		const different = fake(async () => [4, 6]);
		const error = await failure(
			different.client.transaction([
				bind(double, { n: 2 }),
				bind(double, { n: 3 }),
			]),
		);
		expect(error).toBeInstanceOf(StatementEncodeError);
		expect(error).toMatchObject({ statement: 'double', binding: 'n' });
		expect(different.calls).toHaveLength(0);
	});

	it('maps a THROW declared by any of its statements', async () => {
		const thrown = serverError('An error occurred: Not the owner', {
			kind: 'Thrown',
		});
		const error = await failure(
			fake(() => Promise.reject(thrown)).client.transaction([
				bind(double, { n: 2 }),
				bind(guarded, { boundOwner: 'x' }),
			]),
		);
		expect(error).toBeInstanceOf(StatementThrown);
		expect(error).toMatchObject({ reason: 'Not the owner' });
	});

	it('refuses an answer with the wrong number of results', async () => {
		const error = await failure(
			fake(async () => [4]).client.transaction([
				bind(double, { n: 2 }),
				bind(double, { n: 2 }),
			]),
		);
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(error).toMatchObject({ slot: 0 });
	});

	it('answers an empty list without a request', async () => {
		const { calls, client } = fake(async () => []);
		expect(await Effect.runPromise(client.transaction([]))).toEqual([]);
		expect(calls).toHaveLength(0);
	});
});

describe('service', () => {
	it('runs statements through the client in context', async () => {
		const { db } = fake(async (call) => (call.atomic ? [3, 3] : [3]));
		const program = Effect.gen(function* () {
			const one = yield* run(add, { a: 1, b: 2 });
			const [first, second] = yield* transaction([
				bind(add, { a: 1, b: 2 }),
				bind(add, { a: 1, b: 2 }),
			]);
			return one + first + second;
		});
		const result = await Effect.runPromise(
			Effect.result(
				program.pipe(Effect.provide(layer(db as unknown as Queryable))),
			),
		);
		expect(Result.isSuccess(result) && result.success).toBe(9);
	});
});
