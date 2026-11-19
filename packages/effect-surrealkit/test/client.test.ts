/**
 * The client against a stand-in for the SDK: what is sent, what is refused
 * before sending, and how answers and failures are mapped.
 */
import { Effect, Fiber, Result, Schema, Tracer } from 'effect';
import { DateTime, ExpressionError, RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { holeMatcher, lex } from '../src/analyze/lexer.js';

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

/**
 * The first keyword of each top-level statement, as the SDK's own scanner
 * reports it: strings, identifiers and comments skipped, a `;` ending a
 * statement only outside brackets.
 */
const topLevelKeywords = (sql: string): ReadonlyArray<string> => {
	const keywords: Array<string> = [];
	let depth = 0;
	let first: string | undefined;
	for (const token of lex(sql, holeMatcher(undefined)).tokens) {
		if (token.kind === 'punct' && token.value === ';' && depth === 0) {
			if (first !== undefined) keywords.push(first);
			first = undefined;
			continue;
		}
		if (depth === 0 && first === undefined) {
			first = token.kind === 'word' ? token.value.toUpperCase() : '';
		}
		if (token.kind === 'punct' && '([{'.includes(token.value)) depth++;
		if (token.kind === 'punct' && ')]}'.includes(token.value)) {
			depth = Math.max(0, depth - 1);
		}
	}
	if (first !== undefined) keywords.push(first);
	return keywords;
};

/**
 * What the SDK's `transaction` refuses before sending (surrealdb 2.1.1,
 * `assertTransactionSafe`): its own transaction control, and a `RETURN`
 * followed by another statement, which would end the transaction early.
 */
const transactionRefusal = (sql: string): string | undefined => {
	let returned = false;
	for (const keyword of topLevelKeywords(sql)) {
		if (
			keyword === 'BEGIN' ||
			keyword === 'COMMIT' ||
			keyword === 'CANCEL'
		) {
			return `queries[0] contains a ${keyword} statement. transaction() wraps the queries in BEGIN and COMMIT itself, so they must not contain any`;
		}
		if (returned) {
			return 'queries[0] contains a RETURN statement which is followed by another statement (in queries[0])';
		}
		if (keyword === 'RETURN') returned = true;
	}
	return undefined;
};

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
			const refusal = transactionRefusal(first?.query ?? '');
			if (refusal !== undefined) {
				return Promise.reject(new ExpressionError(refusal));
			}
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
		// `guarded` ends in a top-level RETURN, which the SDK accepts only
		// as the last statement of a transaction.
		const { calls, client } = fake(async () => [
			4,
			undefined,
			undefined,
			'owner',
		]);
		const results = await Effect.runPromise(
			client.transaction(
				[
					bind(double, { n: 2 }),
					bind(guarded, { boundOwner: 'owner' }),
				],
				{ retry: true },
			),
		);
		expect(results).toEqual([4, 'owner']);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.atomic).toBe(true);
		expect(calls[0]?.sql).toBe(`${double.sql}\n${guarded.sql}`);
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

	it('refuses a later input shadowed by an earlier LET', async () => {
		const { client, calls } = fake(async () => []);
		const readOwner = make({
			sql: 'RETURN $owner;',
			params: { owner: Schema.String },
			result: Schema.String,
		});
		const error = await failure(
			client.transaction([
				bind(guarded, { boundOwner: 'first' }),
				bind(readOwner, { owner: 'second' }),
			]),
		);
		expect(error).toMatchObject({ binding: 'owner' });
		expect(calls).toHaveLength(0);
	});

	it('distinguishes native binding types with the same rendered text', async () => {
		const read = make({
			sql: 'RETURN $x;',
			params: { x: Schema.Unknown },
			result: Schema.Unknown,
		});
		for (const [a, b] of [
			[[1], new Set([1])],
			[1, new Uint8Array([1])],
		]) {
			const { client, calls } = fake(async () => []);
			expect(
				await failure(
					client.transaction([
						bind(read, { x: a }),
						bind(read, { x: b }),
					]),
				),
			).toMatchObject({ binding: 'x' });
			expect(calls).toHaveLength(0);
		}
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
		// Finding 9: the error names the statement that declares the
		// message, not the transaction.
		expect(error).toMatchObject({ statement: 'guarded' });

		const alsoGuarded = make({
			name: 'also guarded',
			sql: "IF $who = NONE { THROW 'Not the owner' };",
			params: { who: Schema.UndefinedOr(SurrealSchema.String) },
			result: Schema.Undefined,
			throws: ['Not the owner'],
		});
		const either = await failure(
			fake(() => Promise.reject(thrown)).client.transaction([
				bind(alsoGuarded, {}),
				bind(guarded, { boundOwner: 'x' }),
			]),
		);
		expect(either).toBeInstanceOf(StatementThrown);
		expect(either).toMatchObject({
			statement: 'one of `also guarded`, `guarded`',
			reason: 'Not the owner',
		});
	});

	it('refuses a binding that another statement defines, before sending', async () => {
		// Finding 1: `letter` LETs `$n`, and `reader` would read that
		// instead of its own binding.
		const letter = sequence(
			part({ sql: 'LET $n = 1;', defines: ['n'] }),
			part({ sql: '$n;', free: ['n'] }),
		).define({ name: 'letter', params: {}, result: SurrealSchema.Int });
		const reader = make({
			name: 'reader',
			sql: 'RETURN $n;',
			params: { n: SurrealSchema.Int },
			result: SurrealSchema.Int,
		});
		expect(letter.defines).toEqual(['n']);
		expect(reader.defines).toEqual([]);
		for (const order of [
			[bind(letter), bind(reader, { n: 2 })],
			[bind(reader, { n: 2 }), bind(letter)],
		] as const) {
			const { calls, client } = fake(async () => [undefined, 1, 2]);
			const error = await failure(client.transaction(order));
			expect(error).toBeInstanceOf(StatementEncodeError);
			expect(error).toMatchObject({ statement: 'reader', binding: 'n' });
			expect(error.message).toMatch(
				/`\$n` is bound by `reader` and defined with LET by `letter`.*rename/,
			);
			expect(calls).toHaveLength(0);
		}
	});

	it('refuses a statement that opens its own transaction, before sending', async () => {
		const own = sequence(
			part({ sql: 'BEGIN; CREATE slot:one; COMMIT;', slots: 3 }),
		).define({ name: 'own', params: {}, result: Schema.Unknown });
		expect(own.transactional).toBe(true);
		const { calls, client } = fake(async () => []);
		const error = await failure(client.transaction([bind(own)]));
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error).toMatchObject({ statement: 'own' });
		expect(error.message).toMatch(/BEGIN, COMMIT or CANCEL/);
		expect(calls).toHaveLength(0);
	});

	describe('binds one name twice only to one value', () => {
		const statementOf = (
			name: string,
			codec: Schema.Codec<unknown, unknown>,
		) =>
			make({
				name,
				sql: '$v;',
				params: { v: codec },
				result: Schema.Unknown,
			});
		const pair = (
			first: readonly [Schema.Codec<unknown, unknown>, unknown],
			second: readonly [Schema.Codec<unknown, unknown>, unknown],
		) => {
			const { calls, client } = fake(async () => [first[1], second[1]]);
			return {
				calls,
				outcome: Effect.runPromise(
					Effect.result(
						client.transaction([
							bind(statementOf('a', first[0]), { v: first[1] }),
							bind(statementOf('b', second[0]), { v: second[1] }),
						]),
					),
				),
			};
		};
		const refused = async (
			first: readonly [Schema.Codec<unknown, unknown>, unknown],
			second: readonly [Schema.Codec<unknown, unknown>, unknown],
		) => {
			const { calls, outcome } = pair(first, second);
			const result = await outcome;
			expect(Result.isFailure(result)).toBe(true);
			const error = Result.isFailure(result) ? result.failure : undefined;
			expect(error).toBeInstanceOf(StatementEncodeError);
			expect(error).toMatchObject({ statement: 'b', binding: 'v' });
			expect(error?.message).toMatch(
				/`\$v` is bound by `a` and by `b`.*rename/,
			);
			expect(calls).toHaveLength(0);
		};
		const accepted = async (
			first: readonly [Schema.Codec<unknown, unknown>, unknown],
			second: readonly [Schema.Codec<unknown, unknown>, unknown],
		) => {
			const { calls, outcome } = pair(first, second);
			expect(Result.isSuccess(await outcome)).toBe(true);
			expect(calls).toHaveLength(1);
		};
		const unknown = Schema.Unknown;

		it('refuses values of different kinds that print alike', async () => {
			// Finding 2: each of these printed as the same SurrealQL text.
			await refused(
				[Schema.Array(SurrealSchema.String), ['a']],
				[SurrealSchema.SetOf(SurrealSchema.String), new Set(['a'])],
			);
			await refused(
				[SurrealSchema.Int, 5],
				[SurrealSchema.Bytes, new Uint8Array([5])],
			);
			await refused(
				[unknown, new Uint8Array([1, 2]).buffer],
				[unknown, new Uint8Array([3, 4]).buffer],
			);
			await refused([unknown, 1], [unknown, 1n]);
			await refused([unknown, 0], [unknown, -0]);
			await refused([unknown, '1'], [unknown, 1]);
			await refused([unknown, null], [unknown, undefined]);
			await refused(
				[unknown, new Uint8Array([1])],
				[unknown, new Uint8Array([1]).buffer],
			);
			await refused(
				[unknown, new RecordId('t', 1)],
				[unknown, new RecordId('t', 1n)],
			);
			await refused(
				[unknown, { a: 1 }],
				[unknown, { a: 1, b: undefined }],
			);
			await refused(
				[unknown, new Map([['a', 1]])],
				[unknown, new Map([['a', 1]])],
			);
		});

		it('accepts structurally equal values, whatever their key order', async () => {
			await accepted(
				[unknown, { a: 1, b: [2, { c: 3 }] }],
				[unknown, { b: [2, { c: 3 }], a: 1 }],
			);
			await accepted(
				[unknown, new Set([1, 2])],
				[unknown, new Set([2, 1])],
			);
			await accepted(
				[unknown, new Uint8Array([1, 2]).buffer],
				[unknown, new Uint8Array([1, 2]).buffer],
			);
			await accepted(
				[unknown, new Uint8Array([1, 2])],
				[unknown, new Uint8Array([1, 2])],
			);
			await accepted(
				[unknown, new RecordId('t', ['a', 1])],
				[unknown, new RecordId('t', ['a', 1])],
			);
			await accepted(
				[unknown, new DateTime('1969-12-31T23:59:59.5Z')],
				[unknown, new DateTime('1969-12-31T23:59:59.5Z')],
			);
			await accepted([unknown, Number.NaN], [unknown, Number.NaN]);
		});

		it('never fails with a defect, whatever the value', async () => {
			const bare = Object.assign(Object.create(null), { a: 1 });
			await accepted([unknown, bare], [unknown, { a: 1 }]);
			const other = Object.assign(Object.create(null), { a: 2 });
			await refused([unknown, bare], [unknown, other]);
			const cyclic: Record<string, unknown> = { a: 1 };
			cyclic.self = cyclic;
			const twin: Record<string, unknown> = { a: 1 };
			twin.self = twin;
			await refused([unknown, cyclic], [unknown, twin]);
			const throwing = {
				get a(): never {
					throw new Error('boom');
				},
			};
			await refused([unknown, throwing], [unknown, { a: 1 }]);
			await refused([unknown, Symbol('a')], [unknown, Symbol('a')]);
		});
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

	it('is refused by the SDK stand-in as the SDK refuses it', async () => {
		// Finding 10: the real SDK refuses a RETURN another statement follows
		// (test/conformance.test.ts); the stand-in must too.
		const { calls, client } = fake(async () => [
			undefined,
			undefined,
			'x',
			4,
		]);
		const error = await failure(
			client.transaction([
				bind(guarded, { boundOwner: 'owner' }),
				bind(double, { n: 2 }),
			]),
		);
		expect(error).toBeInstanceOf(StatementRequestError);
		expect(error.message).toMatch(/RETURN statement which is followed/);
		expect(calls).toHaveLength(0);
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
		// `add` ends in RETURN, which a transaction takes only last.
		const sum = make({
			name: 'sum',
			sql: '$a + $b;',
			params: { a: SurrealSchema.Int, b: SurrealSchema.Int },
			result: SurrealSchema.Int,
		});
		const program = Effect.gen(function* () {
			const one = yield* run(add, { a: 1, b: 2 });
			const [first, second] = yield* transaction([
				bind(sum, { a: 1, b: 2 }),
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
