/**
 * The client against a stand-in for the SDK: what is sent, what is refused
 * before sending, and how answers and failures are mapped.
 */
import {
	Effect,
	Fiber,
	Metric,
	Result,
	Schedule,
	Schema,
	Tracer,
} from 'effect';
import { TestClock } from 'effect/testing';
import { DateTime, ExpressionError, RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { DEFAULT_HOLE_DELIMITERS, lex } from '../src/analyze/lexer.js';

import {
	bind,
	layer,
	make as makeClient,
	metrics,
	type Queryable,
	run,
	StatementConflict,
	StatementDecodeError,
	StatementEmptyResult,
	StatementEncodeError,
	StatementRequestError,
	StatementScopeError,
	StatementThrown,
	StatementUnexpectedThrow,
	StatementWriteOutcomeError,
	transaction,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import { make, type Part, part, sequence } from '../src/statement/index.js';

interface Call {
	readonly sql: string;
	readonly bindings: Record<string, unknown>;
	readonly atomic: boolean;
	readonly retry: unknown;
	readonly signal: AbortSignal | undefined;
	/** What `transaction` was handed: the request itself. */
	readonly request?: unknown;
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
	for (const token of lex(sql, DEFAULT_HOLE_DELIMITERS).tokens) {
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
				request: first,
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
	writes: false,
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
	writes: false,
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
			writes: false,
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

	it('maps an undeclared THROW to its own error, with the thrown text', async () => {
		const other = serverError('An error occurred: Something else', {
			kind: 'Thrown',
		});
		const error = await failure(
			fake(() => Promise.reject(other)).client.run(guarded, {
				boundOwner: 'x',
			}),
		);
		expect(error).toBeInstanceOf(StatementUnexpectedThrow);
		expect(error).toMatchObject({
			statement: 'guarded',
			reason: 'Something else',
			cause: other,
		});
		// Wherever the SDK nests it.
		const wrapped = new Error('wrapped', { cause: other });
		expect(
			await failure(
				fake(() => Promise.reject(wrapped)).client.run(guarded, {
					boundOwner: 'x',
				}),
			),
		).toMatchObject({
			_tag: 'StatementUnexpectedThrow',
			reason: 'Something else',
			cause: wrapped,
		});
		expect(error.message).toBe(
			"`guarded` threw 'Something else', which it does not declare",
		);
		// Not a request failure: the request ran, and the text threw.
		expect(error).not.toBeInstanceOf(StatementRequestError);
	});

	it('says where a declared THROW comes from when its part records it', async () => {
		const sited = sequence(
			part({
				source: 'accounts/requireOwner.surql',
				sql: "IF $owner != $auth.id {\n\tTHROW 'Not the owner'\n};",
				free: ['owner'],
				throws: ['Not the owner'],
				throwsAt: { 'Not the owner': [2] },
			}),
		).define({
			name: 'sited',
			params: { owner: SurrealSchema.String },
			result: Schema.Undefined,
		});
		const thrown = serverError('An error occurred: Not the owner', {
			kind: 'Thrown',
		});
		const error = await failure(
			fake(() => Promise.reject(thrown)).client.run(sited, {
				owner: 'x',
			}),
		);
		expect(error).toBeInstanceOf(StatementThrown);
		expect(error).toMatchObject({
			statement: 'sited',
			reason: 'Not the owner',
			source: 'accounts/requireOwner.surql',
			lines: [2],
		});
		// Without a source, neither is set.
		const bare = await failure(
			fake(() => Promise.reject(thrown)).client.run(guarded, {
				boundOwner: 'x',
			}),
		);
		expect(bare).toBeInstanceOf(StatementThrown);
		expect('source' in bare).toBe(false);
		expect('lines' in bare).toBe(false);
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

	it('never hands a retry option to the SDK', async () => {
		const { calls, client } = fake(async () => [3]);
		await Effect.runPromise(
			client.run(add, { a: 1, b: 2 }, { retry: Schedule.recurs(3) }),
		);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.retry).toBeUndefined();
	});

	describe('retry', () => {
		const conflict = () =>
			serverError('Cannot COMMIT: Transaction conflict', {
				kind: 'Query',
				isTransactionConflict: true,
			});
		/** Lets the fake's promises settle, outside the test clock. */
		const settle = Effect.promise(
			() => new Promise((resolve) => setTimeout(resolve, 5)),
		);
		const named = (name: string) =>
			make({ ...add, name, sql: add.sql, params: add.params });
		const count = (
			metric: typeof metrics.runs,
			attributes: Record<string, string>,
		) =>
			Metric.value(Metric.withAttributes(metric, attributes)).pipe(
				Effect.map((state) => Number(state.count)),
			);

		it('sends the same encoded request again after each conflict, as the schedule allows', async () => {
			const statement = named('add retried');
			let failures = 2;
			const { calls, client } = fake(async () => {
				if (failures-- > 0) throw conflict();
				return [3];
			});
			const result = await Effect.runPromise(
				Effect.gen(function* () {
					const fiber = yield* Effect.forkChild(
						client.run(
							statement,
							{ a: 1, b: 2 },
							{ retry: Schedule.spaced('1 second') },
						),
					);
					yield* settle;
					expect(calls).toHaveLength(1);
					yield* TestClock.adjust('999 millis');
					yield* settle;
					expect(calls).toHaveLength(1);
					yield* TestClock.adjust('1 millis');
					yield* settle;
					expect(calls).toHaveLength(2);
					yield* TestClock.adjust('1 second');
					yield* settle;
					const value = yield* Fiber.join(fiber);
					const retried = yield* count(metrics.retries, {
						statement: 'add retried',
					});
					const succeeded = yield* count(metrics.runs, {
						statement: 'add retried',
						outcome: 'success',
					});
					return { value, retried, succeeded };
				}).pipe(Effect.provide(TestClock.layer())),
			);
			expect(result).toEqual({ value: 3, retried: 2, succeeded: 1 });
			expect(calls).toHaveLength(3);
			// The same request: one encoding, sent three times.
			expect(calls[1]?.bindings).toBe(calls[0]?.bindings);
			expect(calls[2]?.bindings).toBe(calls[0]?.bindings);
			expect(calls.map((call) => call.sql)).toEqual([
				add.sql,
				add.sql,
				add.sql,
			]);
		});

		it('fails with the last conflict when the schedule ends', async () => {
			const { calls, client } = fake(async () => {
				throw conflict();
			});
			const error = await Effect.runPromise(
				Effect.gen(function* () {
					const fiber = yield* Effect.forkChild(
						Effect.flip(
							client.run(
								named('add exhausted'),
								{ a: 1, b: 2 },
								{
									retry: Schedule.max([
										Schedule.spaced('10 millis'),
										Schedule.recurs(2),
									]),
								},
							),
						),
					);
					for (let i = 0; i < 4; i++) {
						yield* settle;
						yield* TestClock.adjust('10 millis');
					}
					return yield* Fiber.join(fiber);
				}).pipe(Effect.provide(TestClock.layer())),
			);
			expect(error).toBeInstanceOf(StatementConflict);
			expect(calls).toHaveLength(3);
		});

		it('retries nothing but a conflict', async () => {
			for (const answer of [
				() => Promise.reject(new Error('connection lost')),
				() =>
					Promise.reject(
						serverError('An error occurred: Not the owner', {
							kind: 'Thrown',
						}),
					),
				async () => ['not an int'],
			]) {
				const { calls, client } = fake(answer);
				const error = await failure(
					client.run(
						named('add once'),
						{ a: 1, b: 2 },
						{ retry: Schedule.recurs(5) },
					),
				);
				expect(error).not.toBeInstanceOf(StatementConflict);
				expect(calls).toHaveLength(1);
			}
		});

		it('retries a transaction as one request', async () => {
			const sum = make({
				name: 'sum',
				sql: '$a + $b;',
				params: add.params,
				result: SurrealSchema.Int,
			});
			let failures = 1;
			const { calls, client } = fake(async () => {
				if (failures-- > 0) throw conflict();
				return [3, 3];
			});
			const results = await Effect.runPromise(
				Effect.gen(function* () {
					const fiber = yield* Effect.forkChild(
						client.transaction(
							[
								bind(sum, { a: 1, b: 2 }),
								bind(sum, { a: 1, b: 2 }),
							],
							{ retry: Schedule.spaced('1 second') },
						),
					);
					yield* settle;
					yield* TestClock.adjust('1 second');
					yield* settle;
					return yield* Fiber.join(fiber);
				}).pipe(Effect.provide(TestClock.layer())),
			);
			expect(results).toEqual([3, 3]);
			expect(calls).toHaveLength(2);
			expect(calls.every((call) => call.atomic)).toBe(true);
			expect(calls[1]?.request).toBe(calls[0]?.request);
			expect(calls[1]?.bindings).toEqual({ a: 1, b: 2 });
		});
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
		writes: false,
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
				{ retry: Schedule.recurs(1) },
			),
		);
		expect(results).toEqual([4, 'owner']);
		expect(calls).toHaveLength(1);
		expect(calls[0]?.atomic).toBe(true);
		expect(calls[0]?.sql).toBe(`${double.sql}\n${guarded.sql}`);
		expect(calls[0]?.bindings).toEqual({ boundOwner: 'owner', n: 2 });
		// Retries are the client's own; the SDK is never handed one.
		expect(calls[0]?.retry).toBeUndefined();
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
		expect(error).toBeInstanceOf(StatementScopeError);
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
		expect(error).toBeInstanceOf(StatementScopeError);
		expect(error).toMatchObject({ binding: 'owner' });
		expect(calls).toHaveLength(0);
	});

	it('refuses a binding shadowed by a LET of a directly declared statement', async () => {
		const text = "LET $owner = 'a'; RETURN $owner;";
		expect(() =>
			make({ sql: text, params: {}, result: Schema.String, slots: 2 }),
		).toThrow(/declares no `defines`/);
		const letter = make({
			sql: text,
			params: {},
			result: Schema.String,
			slots: 2,
			defines: ['owner'],
		});
		const readOwner = make({
			sql: 'RETURN $owner;',
			params: { owner: Schema.String },
			result: Schema.String,
		});
		const { client, calls } = fake(async () => []);
		const error = await failure(
			client.transaction([
				bind(letter, {}),
				bind(readOwner, { owner: 'b' }),
			]),
		);
		expect(error).toBeInstanceOf(StatementScopeError);
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
			expect(error).toBeInstanceOf(StatementScopeError);
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
		expect(error).toBeInstanceOf(StatementScopeError);
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
			expect(error).toBeInstanceOf(StatementScopeError);
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

	it('reports an acknowledged mixed batch when only its read result fails', async () => {
		const write = make({
			name: 'change',
			sql: 'UPDATE tally SET n += 1 RETURN NONE;',
			params: {},
			result: Schema.Undefined,
		});
		const { client, calls } = fake(async () => [undefined, 'invalid']);
		const error = await failure(
			client.transaction([bind(write), bind(double, { n: 2 })]),
		);
		expect(error).toBeInstanceOf(StatementWriteOutcomeError);
		expect(error).toMatchObject({
			request: 'transaction(change, double)',
			cause: { statement: 'double', slot: 1 },
		});
		expect(calls).toHaveLength(1);
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

describe('empty results', () => {
	const made = make({
		name: 'make one',
		sql: 'CREATE ONLY t RETURN AFTER;',
		params: {},
		result: Schema.Struct({ n: SurrealSchema.Int }),
		writes: true,
		nonEmpty: true,
	});

	it('reports NONE or an empty list from a statement that always returns a row as an empty result', async () => {
		for (const answer of [undefined, null, []]) {
			const error = await failure(
				fake(async () => [answer]).client.run(made),
			);
			expect(error).toBeInstanceOf(StatementEmptyResult);
			expect(error).toMatchObject({ statement: 'make one', slot: 1 });
			expect(error.message).toMatch(/may lack permission/);
		}
		expect(
			await Effect.runPromise(
				fake(async () => [{ n: 1 }]).client.run(made),
			),
		).toEqual({ n: 1 });
	});

	it('leaves an empty result alone when the statement does not promise a row', async () => {
		const some = make({
			name: 'update some',
			sql: 'UPDATE t SET n = 1 WHERE n = 0;',
			params: {},
			result: Schema.Array(Schema.Struct({ n: SurrealSchema.Int })),
			writes: true,
		});
		expect(
			await Effect.runPromise(fake(async () => [[]]).client.run(some)),
		).toEqual([]);
	});

	it('reports an empty result inside a transaction by its statement', async () => {
		const sum = make({
			name: 'sum',
			sql: '$a + $b;',
			params: add.params,
			result: SurrealSchema.Int,
		});
		const error = await failure(
			fake(async () => [3, undefined]).client.transaction([
				bind(sum, { a: 1, b: 2 }),
				bind(made),
			]),
		);
		expect(error).toBeInstanceOf(StatementEmptyResult);
		expect(error).toMatchObject({ statement: 'make one', slot: 1 });
	});
});

describe('a result that does not decode', () => {
	it('says that the writes of a writing statement are committed', async () => {
		const writing = make({
			name: 'write',
			sql: 'UPDATE t SET n = 1 RETURN AFTER;',
			params: {},
			result: Schema.Array(Schema.Struct({ n: SurrealSchema.Int })),
			writes: true,
		});
		const error = await failure(
			fake(async () => [[{ n: 'x' }]]).client.run(writing),
		);
		// A request that may write reports the decode error inside a
		// write outcome, so it is never mistaken for nothing having run.
		expect(error).toBeInstanceOf(StatementWriteOutcomeError);
		const cause = (error as StatementWriteOutcomeError).cause;
		expect(cause).toBeInstanceOf(StatementDecodeError);
		expect(cause).toMatchObject({ committed: true, slot: 1 });
		expect(cause.message).toMatch(
			/request succeeded and its writes are committed/,
		);
		expect(error.message).toBe(cause.message);
		const count = await failure(fake(async () => []).client.run(writing));
		expect(count).toMatchObject({
			request: 'write',
			cause: { committed: true, slot: 0 },
		});
	});

	it('says any writes are committed when it cannot know whether the statement wrote', async () => {
		const unknown = make({
			name: 'unknown',
			sql: 'RETURN fn::maybe_writes();',
			params: {},
			result: SurrealSchema.Int,
		});
		const error = await failure(
			fake(async () => ['x']).client.run(unknown),
		);
		expect(error).toMatchObject({
			_tag: 'StatementWriteOutcomeError',
			cause: { committed: true },
		});
		expect(error.message).toMatch(/any writes it made are committed/);
		expect(error.message).toMatch(/may have written/);
		expect(error.message).not.toMatch(/its writes are committed/);
	});

	it('refuses a statement declared read-only that writes', () => {
		// One flag: `make` cannot be told both that a text writes and that
		// it does not, so the contradiction cannot be declared there.
		const declared = make({
			name: 'contradiction',
			sql: 'UPDATE t SET n = 1;',
			params: {},
			result: Schema.Unknown,
			writes: true,
		});
		expect(declared.writes).toBe(true);
		expect('readOnly' in declared).toBe(false);
		expect(() =>
			sequence(part({ sql: 'UPDATE t SET n = 1;' })).define({
				writes: false,
				params: {},
				result: Schema.Unknown,
			}),
		).toThrow(/declared not to write, but it writes/);
		expect(() =>
			part({ sql: 'UPDATE t SET n = 1;', writes: false }),
		).toThrow(/writes: declared false, the text writes/);
	});

	it('does not claim a commit for a statement that does not write', async () => {
		const error = await failure(
			fake(async () => ['x']).client.run(add, { a: 1, b: 2 }),
		);
		expect(error).toBeInstanceOf(StatementDecodeError);
		expect(error).toMatchObject({ committed: false });
		expect(error.message).not.toMatch(/committed/);
	});

	it('takes the writes of authored text from the analyzer', async () => {
		const authored = sequence(
			part({ sql: 'LET $x = (UPSERT t:one SET n = 1);', defines: ['x'] }),
			part({ sql: 'RETURN $x;', free: ['x'] }),
		).define({ name: 'authored', params: {}, result: SurrealSchema.Int });
		expect(authored.writes).toBe(true);
		const error = await failure(
			fake(async () => [undefined, 'x']).client.run(authored),
		);
		expect(error).toMatchObject({
			_tag: 'StatementWriteOutcomeError',
			cause: { committed: true },
		});
		expect(error.message).toMatch(/its writes are committed/);
		// No write found is not proof of none, so this one is unknown.
		const reading = sequence(part({ sql: "RETURN 'update';" })).define({
			params: {},
			result: SurrealSchema.Int,
		});
		expect(reading.writes).toBeUndefined();
		expect(
			await failure(fake(async () => ['x']).client.run(reading)),
		).toMatchObject({ _tag: 'StatementWriteOutcomeError' });
	});

	describe('in each of the three states of `writes`', () => {
		// Bare expressions: the SDK refuses a top-level RETURN that is not
		// the last statement of a transaction.
		const readOnly = part({ sql: '1;', writes: false });
		const unknown = part({ sql: 'fn::maybe_writes();' });
		const writing = part({ sql: 'UPDATE t:one SET n = 1 RETURN NONE;' });
		const define = (...parts: ReadonlyArray<Part>) =>
			sequence(...parts).define({
				name: 'parts',
				params: {},
				result: SurrealSchema.Int,
			});
		const answers = (statement: { readonly slots: number }) =>
			fake(async () => [
				...Array.from({ length: statement.slots - 1 }, () => undefined),
				'x',
			]).client;

		it('a sequence writes if any part does, is read-only only if every part is, and is unknown otherwise', () => {
			expect(readOnly.writes).toBe(false);
			expect(unknown.writes).toBeUndefined();
			expect(writing.writes).toBe(true);
			expect(define(readOnly, readOnly).writes).toBe(false);
			expect(define(readOnly, unknown).writes).toBeUndefined();
			expect(define(unknown, unknown).writes).toBeUndefined();
			expect(define(readOnly, writing).writes).toBe(true);
			expect(define(unknown, writing).writes).toBe(true);
		});

		it('a declaration settles what the parts leave unknown, and may not contradict them', () => {
			const declare = (writes: boolean, ...parts: ReadonlyArray<Part>) =>
				sequence(...parts).define({
					name: 'declared',
					writes,
					params: {},
					result: SurrealSchema.Int,
				});
			expect(declare(false, unknown).writes).toBe(false);
			expect(declare(true, unknown).writes).toBe(true);
			expect(declare(false, readOnly).writes).toBe(false);
			expect(declare(true, writing).writes).toBe(true);
			expect(() => declare(true, readOnly, readOnly)).toThrow(
				/`declared` is declared to write, but every part is declared not to/,
			);
			expect(() => declare(false, unknown, writing)).toThrow(
				/`declared` is declared not to write, but it writes/,
			);
		});

		it('a result that does not decode says what is known about the writes', async () => {
			const certain = define(readOnly, writing);
			const committed = await failure(answers(certain).run(certain));
			expect(committed).toBeInstanceOf(StatementWriteOutcomeError);
			expect(committed.message).toMatch(/its writes are committed/);

			const maybe = define(readOnly, unknown);
			const possible = await failure(answers(maybe).run(maybe));
			expect(possible).toBeInstanceOf(StatementWriteOutcomeError);
			expect(possible.message).toMatch(/may have written/);
			expect(possible.message).not.toMatch(/its writes are committed/);

			const read = define(readOnly, readOnly);
			const plain = await failure(answers(read).run(read));
			expect(plain).toBeInstanceOf(StatementDecodeError);
			expect(plain).toMatchObject({ committed: false });
			expect(plain.message).not.toMatch(/committed|written/);
		});

		it('a transaction is read-only only if every statement is', async () => {
			const read = define(readOnly);
			const maybe = define(unknown);
			const plain = await failure(
				fake(async () => [1, 'x']).client.transaction([
					bind(read),
					bind(read),
				]),
			);
			expect(plain).toBeInstanceOf(StatementDecodeError);
			const possible = await failure(
				fake(async () => [1, 'x']).client.transaction([
					bind(read),
					bind(maybe),
				]),
			);
			expect(possible).toMatchObject({
				_tag: 'StatementWriteOutcomeError',
				request: 'transaction(parts, parts)',
				cause: { committed: true },
			});
			expect(possible.message).toMatch(/may have written/);
		});
	});

	it('in a transaction, says the writes are committed when any statement writes', async () => {
		const writing = make({
			name: 'write',
			sql: 'UPDATE t SET n = 1 RETURN NONE;',
			params: {},
			result: Schema.Tuple([]),
			writes: true,
		});
		const sum = make({
			name: 'sum',
			sql: '$a + $b;',
			params: add.params,
			result: SurrealSchema.Int,
		});
		const error = await failure(
			fake(async () => [[], 'x']).client.transaction([
				bind(writing),
				bind(sum, { a: 1, b: 2 }),
			]),
		);
		expect(error).toMatchObject({
			_tag: 'StatementWriteOutcomeError',
			request: 'transaction(write, sum)',
			cause: { statement: 'sum', committed: true },
		});
	});
});

describe('outgoing values', () => {
	const open = make({
		name: 'open',
		sql: 'RETURN $v;',
		params: { v: Schema.Unknown },
		result: Schema.Unknown,
	});

	class Point {
		constructor(readonly x: number) {}
	}

	it('refuses a value the SDK would not send as it is, naming the binding and the path', async () => {
		for (const [value, path, problem] of [
			[{ a: ['ok', 'bad\uD800'] }, ['a', 1], /lone UTF-16 surrogate/],
			[{ m: new Map([['k', 1]]) }, ['m'], /a Map/],
			[[new Point(1)], [0], /a Point/],
			[{ 'k\uDC00': 1 }, ['k\uDC00'], /a key with a lone/],
			[{ n: 2 ** 60 }, ['n'], /beyond ±2\^53/],
			[{ n: 2n ** 70n }, ['n'], /beyond 64 bits/],
			[{ f: () => 1 }, ['f'], /a function/],
			[{ s: Symbol('s') }, ['s'], /a symbol/],
			[{ d: new Date(Number.NaN) }, ['d'], /an invalid Date/],
			[{ [Symbol('k')]: 1 }, [], /symbol keys/],
			[
				{ id: new RecordId('t', { bad: 'x\uD800' }) },
				['id', 'id', 'bad'],
				/surrogate/,
			],
			[new Float64Array([1]), [], /raw bytes/],
		] as const) {
			const { calls, client } = fake(async () => [value]);
			const error = await failure(client.run(open, { v: value }));
			expect(error).toBeInstanceOf(StatementEncodeError);
			expect(error).toMatchObject({
				statement: 'open',
				binding: 'v',
				path,
			});
			expect(error.message).toMatch(problem);
			expect(error.message).toMatch(/^\$v/);
			expect(calls).toHaveLength(0);
		}
	});

	it('names the path in its message', async () => {
		const error = await failure(
			fake(async () => []).client.run(open, {
				v: { 'a b': [{ c: 'x\uD800' }] },
			}),
		);
		expect(error.message).toMatch(/^\$v\["a b"\]\[0\]\.c is a string/);
	});

	it('accepts every SurrealValue, NONE and bytes', async () => {
		const values = [
			undefined,
			null,
			true,
			1.5,
			-0,
			Number.NaN,
			2 ** 53,
			2n ** 63n,
			'text',
			new Date(0),
			new DateTime('2026-01-01T00:00:00Z'),
			new RecordId('t', ['a', 1, { b: 2n }]),
			new Uint8Array([1]),
			new Uint8Array([1]).buffer,
			new Set(['a']),
			[1, [2, { c: null }]],
			Object.assign(Object.create(null), { a: 1 }),
		];
		for (const value of values) {
			const { calls, client } = fake(async () => [null]);
			await Effect.runPromise(client.run(open, { v: value }));
			expect(calls).toHaveLength(1);
		}
	});

	it('names the statement that binds the value in a transaction', async () => {
		const sum = make({
			name: 'sum',
			sql: '$a + $b;',
			params: add.params,
			result: SurrealSchema.Int,
		});
		const openly = make({
			name: 'openly',
			sql: '$v;',
			params: { v: Schema.Unknown },
			result: Schema.Unknown,
		});
		const { calls, client } = fake(async () => [3, 1]);
		const error = await failure(
			client.transaction([
				bind(sum, { a: 1, b: 2 }),
				bind(openly, { v: [new Point(1)] }),
			]),
		);
		expect(error).toBeInstanceOf(StatementEncodeError);
		expect(error).toMatchObject({
			statement: 'openly',
			binding: 'v',
			path: [0],
		});
		expect(calls).toHaveLength(0);
	});

	it('never throws, whatever the value', async () => {
		const throwing = {
			get a(): never {
				throw new Error('boom');
			},
		};
		const cyclic: Record<string, unknown> = {};
		cyclic.self = cyclic;
		let deep: unknown = 1;
		for (let i = 0; i < 100_000; i++) deep = [deep];
		const proxy = new Proxy(
			{},
			{
				ownKeys: () => {
					throw new Error('trap');
				},
			},
		);
		for (const value of [throwing, cyclic, deep, proxy]) {
			const { calls, client } = fake(async () => [null]);
			const error = await failure(client.run(open, { v: value }));
			expect(error).toBeInstanceOf(StatementEncodeError);
			expect(calls).toHaveLength(0);
		}
	});

	it('can be skipped, explicitly', async () => {
		const value = { m: new Map([['k', 1]]) };
		const { calls, client } = fake(async () => [null]);
		await Effect.runPromise(
			client.run(open, { v: value }, { unsafeSkipValueCheck: true }),
		);
		expect(calls[0]?.bindings).toEqual({ v: value });
	});
});

describe('observability', () => {
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
	const attributesOf = (span: Tracer.Span | undefined) =>
		Object.fromEntries(span?.attributes ?? []);

	it('records the rows of a list result, the source of its parts and no values', async () => {
		const listed = sequence(
			part({
				source: 'people/list.surql',
				sql: 'SELECT VALUE name FROM person WHERE name != $secret;',
				free: ['secret'],
			}),
		).define({
			name: 'list people',
			params: { secret: SurrealSchema.String },
			result: Schema.Array(SurrealSchema.String),
		});
		const { spans, tracer } = tracing();
		await Effect.runPromise(
			fake(async () => [['a', 'b', 'c']])
				.client.run(listed, { secret: 'hunter2' })
				.pipe(Effect.withTracer(tracer)),
		);
		const attributes = attributesOf(spans[0]);
		expect(spans[0]?.name).toBe('list people');
		expect(attributes).toMatchObject({
			'db.response.returned_rows': 3,
			'code.file.path': 'people/list.surql',
		});
		expect(JSON.stringify(attributes)).not.toMatch(/hunter2/);
	});

	it('records the error tag on failure', async () => {
		const { spans, tracer } = tracing();
		await Effect.runPromise(
			Effect.result(
				fake(async () => ['x'])
					.client.run(add, { a: 1, b: 2 })
					.pipe(Effect.withTracer(tracer)),
			),
		);
		expect(attributesOf(spans[0])).toMatchObject({
			'error.type': 'StatementDecodeError',
		});
		expect(attributesOf(spans[0])).not.toHaveProperty(
			'db.response.returned_rows',
		);
	});

	it('counts runs by statement and outcome, and times them', async () => {
		const counted = make({ ...add, name: 'add counted' });
		const runs = (outcome: string) =>
			Metric.value(
				Metric.withAttributes(metrics.runs, {
					statement: 'add counted',
					outcome,
				}),
			).pipe(Effect.map((state) => Number(state.count)));
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const { client } = fake(async (call) =>
					call.bindings.a === 0 ? ['x'] : [3],
				);
				yield* client.run(counted, { a: 1, b: 2 });
				yield* client.run(counted, { a: 1, b: 2 });
				yield* Effect.result(client.run(counted, { a: 0, b: 2 }));
				yield* Effect.result(client.run(counted, { a: 1, b: 2 ** 60 }));
				const timed = yield* Metric.value(
					Metric.withAttributes(metrics.duration, {
						statement: 'add counted',
					}),
				);
				return {
					success: yield* runs('success'),
					decode: yield* runs('StatementDecodeError'),
					encode: yield* runs('StatementEncodeError'),
					timed: timed.count,
				};
			}),
		);
		expect(result).toEqual({ success: 2, decode: 1, encode: 1, timed: 4 });
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
