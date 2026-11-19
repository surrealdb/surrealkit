/**
 * Runs statements against SurrealDB through the JavaScript SDK.
 *
 * One request per statement: bindings are encoded through their codecs
 * first, and each encoded value is checked to be one the SDK sends without
 * loss, so a refused value is never sent; the answer must have the
 * declared number of results, NONE in all but the last, and a last result
 * the statement's codec accepts. Interrupting the effect aborts the request.
 */
import {
	Clock,
	Context,
	Duration,
	Effect,
	Layer,
	Metric,
	type Schedule,
	Schema,
} from 'effect';
import { BoundQuery, type SurrealSession } from 'surrealdb';

import type {
	AnyStatement,
	CheckedInput,
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import { sameValue } from './equal.js';
import {
	StatementConflict,
	StatementDecodeError,
	StatementEmptyResult,
	StatementEncodeError,
	type StatementError,
	StatementRequestError,
	StatementScopeError,
	StatementThrown,
	StatementUnexpectedThrow,
	StatementWriteOutcomeError,
} from './errors.js';
import { pathText, valueProblem } from './values.js';

/**
 * What a client needs from the SDK: a connection or a session. Statements
 * run with that session's authentication, namespace and database.
 */
export type Queryable = Pick<SurrealSession, 'query' | 'transaction'>;

export interface RunOptions {
	/**
	 * Sends the same, already encoded request again while the engine
	 * reports a transaction conflict ({@link StatementConflict}), as the
	 * schedule allows; any other failure is never retried. Safe for one
	 * statement and for `transaction`, which are single atomic requests.
	 * SurrealDB 4 already retries a single conflicting statement on the
	 * server. Off by default.
	 *
	 * @example
	 * client.run(bump, {}, { retry: Schedule.recurs(3) })
	 */
	readonly retry?: Schedule.Schedule<unknown, StatementConflict>;
	/**
	 * Skips the check that every encoded binding is a value the SDK sends
	 * without loss (a `SurrealValue`, or `undefined` for NONE, with
	 * well-formed strings). Codecs still run. Default false.
	 */
	readonly unsafeSkipValueCheck?: boolean;
}

/**
 * The input argument: optional when no binding needs a value. `I` is the
 * input as the caller wrote it, checked by {@link CheckedInput}.
 */
export type InputOf<P extends Params, I = Input<P>> =
	Record<never, never> extends Input<P>
		? readonly [input?: CheckedInput<P, I>]
		: readonly [input: CheckedInput<P, I>];

/** The arguments after the statement: its input, then the options. */
export type RunArgs<P extends Params, I = Input<P>> =
	Record<never, never> extends Input<P>
		? readonly [input?: CheckedInput<P, I>, options?: RunOptions]
		: readonly [input: CheckedInput<P, I>, options?: RunOptions];

/** Only {@link bind} makes a {@link Bound}: no other code can name this. */
declare const BoundTypeId: unique symbol;

/**
 * A statement together with the values of its bindings, made by
 * {@link bind}, which checks the input against the statement's bindings.
 */
export interface Bound<
	R extends Codec = Codec,
	Throws extends string = string,
> {
	readonly [BoundTypeId]: 'made by bind';
	readonly statement: Statement<Params, R, Throws>;
	readonly input: Readonly<Record<string, unknown>>;
}

/** Pairs a statement with its input, to run it inside {@link Client.transaction}. */
export const bind = <
	P extends Params,
	R extends Codec,
	Throws extends string,
	I = Input<P>,
>(
	statement: Statement<P, R, Throws>,
	...input: InputOf<P, I>
): Bound<R, Throws> =>
	({
		statement,
		input: (input[0] ?? {}) as Readonly<Record<string, unknown>>,
	}) as unknown as Bound<R, Throws>;

/**
 * The statements of a transaction as the caller wrote them, `S`: each one
 * not made by {@link bind} is typed as a message saying so.
 */
export type BoundStatements<S extends ReadonlyArray<unknown>> = {
	// Only the mark is tested, so a bound statement of a generic result passes.
	readonly [I in keyof S]: S[I] extends {
		readonly [BoundTypeId]: 'made by bind';
	}
		? S[I]
		: 'a transaction takes statements paired with their input by `bind`';
};

/** The decoded results of the statements of a transaction, in order. */
export type Results<S extends ReadonlyArray<unknown>> = {
	readonly [I in keyof S]: S[I] extends Bound<infer R, string>
		? R['Type']
		: never;
};

/** The messages any statement of a transaction can `THROW`. */
export type ThrownByAny<S extends ReadonlyArray<unknown>> =
	S[number] extends Bound<Codec, infer T> ? T : never;

export interface Client {
	/**
	 * Runs one statement and decodes its result.
	 *
	 * The statement is one request. Unless its own text says otherwise
	 * (`BEGIN … COMMIT`), each of its SurrealQL statements commits on its
	 * own; use {@link Client.transaction} for all-or-nothing.
	 */
	readonly run: <
		P extends Params,
		R extends Codec,
		Throws extends string,
		I = Input<P>,
	>(
		statement: Statement<P, R, Throws>,
		...args: RunArgs<P, I>
	) => Effect.Effect<R['Type'], StatementError<Throws>>;

	/**
	 * Runs several statements as one atomic request and decodes each result:
	 * all of them take effect, or none does.
	 *
	 * The statements share one variable scope, as SurrealQL has it, so
	 * before anything is sent a binding name is refused when two
	 * statements bind it to values that are not certainly equal, or when
	 * one statement binds a name another `LET`s (a `StatementScopeError`
	 * naming both). A statement whose own text writes `BEGIN`, `COMMIT` or
	 * `CANCEL` cannot be nested in this one and is refused as a
	 * `StatementScopeError` too. The SDK refuses a top-level `RETURN` that
	 * is followed by another statement, because some engines end the
	 * transaction there. A declared `THROW` names the statement that
	 * declares it.
	 */
	readonly transaction: <const S extends ReadonlyArray<unknown>>(
		statements: BoundStatements<S>,
		options?: RunOptions,
	) => Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>>;
}

/** The client statements run through, as a service. */
export class SurrealClient extends Context.Service<SurrealClient, Client>()(
	'effect-surrealkit/SurrealClient',
) {}

const thrownPrefix = 'An error occurred: ';

const messageOf = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

/** The statement that declares a `THROW` message, and where. */
interface Declarer {
	readonly statement: string;
	readonly source?: string;
	readonly lines?: ReadonlyArray<number>;
}

/**
 * The SDK chains server errors through `cause`. They are matched by shape
 * rather than by class, so a second installed copy of the SDK still maps.
 */
const classify = <Throws extends string>(
	name: string,
	/** What declares `reason`, or `undefined` when nothing does. */
	declaredBy: (reason: string) => Declarer | undefined,
	cause: unknown,
):
	| StatementRequestError
	| StatementConflict
	| StatementThrown<Throws>
	| StatementUnexpectedThrow => {
	let unexpected: string | undefined;
	for (
		let error: unknown = cause;
		error instanceof Error;
		error = error.cause
	) {
		const { isTransactionConflict, kind } = error as {
			readonly isTransactionConflict?: unknown;
			readonly kind?: unknown;
		};
		if (kind === 'Thrown') {
			const reason = error.message.startsWith(thrownPrefix)
				? error.message.slice(thrownPrefix.length)
				: error.message;
			const declarer = declaredBy(reason);
			if (declarer !== undefined) {
				return new StatementThrown<Throws>({ ...declarer, reason });
			}
			unexpected ??= reason;
		}
		if (isTransactionConflict === true) {
			return new StatementConflict({
				statement: name,
				message: error.message,
				cause,
			});
		}
	}
	if (unexpected !== undefined) {
		return new StatementUnexpectedThrow({
			statement: name,
			reason: unexpected,
			cause,
		});
	}
	return new StatementRequestError({
		statement: name,
		message: messageOf(cause),
		cause,
	});
};

const encode = (
	statement: AnyStatement,
	input: Readonly<Record<string, unknown>>,
): Effect.Effect<Record<string, unknown>, StatementEncodeError> =>
	Effect.forEach(Object.entries(statement.params), ([name, codec]) =>
		Schema.encodeUnknownEffect(codec)(input[name]).pipe(
			Effect.map((value) => [name, value] as const),
			Effect.mapError(
				(error) =>
					new StatementEncodeError({
						statement: statement.name,
						binding: name,
						message: error.message,
					}),
			),
		),
	).pipe(Effect.map((entries) => Object.fromEntries(entries)));

/**
 * Refuses an encoded binding the SDK would not send as it is. `owner`
 * names the statement that binds each name.
 */
const checkValues = (
	bindings: Readonly<Record<string, unknown>>,
	owner: (binding: string) => string,
	options: RunOptions | undefined,
): Effect.Effect<void, StatementEncodeError> => {
	if (options?.unsafeSkipValueCheck === true) return Effect.void;
	for (const [binding, value] of Object.entries(bindings)) {
		const found = valueProblem(value);
		if (found !== undefined) {
			return Effect.fail(
				new StatementEncodeError({
					statement: owner(binding),
					binding,
					path: found.path,
					message: `${pathText(binding, found.path)} is ${found.problem}; nothing was sent`,
				}),
			);
		}
	}
	return Effect.void;
};

/**
 * What a request changed: it certainly wrote (a statement has `writes:
 * true`), it may have (one leaves `writes` unknown: authored text can call
 * a function that writes), or it did not (every one has `writes: false`).
 */
type Changes = 'writes' | 'maybe' | 'none';

const changesOf = (statements: ReadonlyArray<AnyStatement>): Changes =>
	statements.some((statement) => statement.writes === true)
		? 'writes'
		: statements.every((statement) => statement.writes === false)
			? 'none'
			: 'maybe';

const committedNote = (changes: Changes): string =>
	changes === 'writes'
		? '. The request succeeded and its writes are committed, so do not send it again to retry'
		: changes === 'maybe'
			? '. The request succeeded and may have written: it is not known whether it writes, and any writes it made are committed; read the data back before sending it again'
			: '';

/**
 * A decode failure of a request that may have written is reported as a
 * {@link StatementWriteOutcomeError} around it; a read-only one as itself.
 */
const decodeOutcome =
	(request: string, changes: Changes) =>
	<E>(error: E): E | StatementWriteOutcomeError =>
		changes !== 'none' && error instanceof StatementDecodeError
			? new StatementWriteOutcomeError({ request, cause: error })
			: error;

/** Decodes the results of one statement out of `results`, from `offset`. */
const decode = (
	statement: AnyStatement,
	results: ReadonlyArray<unknown>,
	offset: number,
	writes: Changes,
): Effect.Effect<unknown, StatementDecodeError | StatementEmptyResult> => {
	const at = offset + statement.valueSlot - 1;
	for (let slot = offset; slot < offset + statement.slots; slot++) {
		if (slot !== at && results[slot] !== undefined) {
			return Effect.fail(
				new StatementDecodeError({
					statement: statement.name,
					slot: slot - offset + 1,
					committed: writes !== 'none',
					message: `result ${slot - offset + 1} of ${statement.slots} must be NONE; only result ${statement.valueSlot} of a statement is decoded${committedNote(writes)}`,
				}),
			);
		}
	}
	const value = results[at];
	if (
		statement.nonEmpty === true &&
		(value === undefined ||
			value === null ||
			(Array.isArray(value) && value.length === 0))
	) {
		return Effect.fail(
			new StatementEmptyResult({
				statement: statement.name,
				slot: statement.valueSlot,
				message: `\`${statement.name}\` returned nothing, although it returns a row whenever it applies: the session may lack permission for it (SurrealDB reports a denied write as an empty result, without an error), and it wrote nothing`,
			}),
		);
	}
	return Schema.decodeUnknownEffect(statement.result)(value, {
		onExcessProperty: 'error',
	}).pipe(
		Effect.mapError(
			(error) =>
				new StatementDecodeError({
					statement: statement.name,
					slot: statement.valueSlot,
					committed: writes !== 'none',
					message: `${error.message}${committedNote(writes)}`,
				}),
		),
	);
};

const countOf = (
	name: string,
	results: unknown,
	expected: number,
	writes: Changes,
): Effect.Effect<ReadonlyArray<unknown>, StatementDecodeError> =>
	Array.isArray(results) && results.length === expected
		? Effect.succeed(results)
		: Effect.fail(
				new StatementDecodeError({
					statement: name,
					slot: 0,
					committed: writes !== 'none',
					message: `expected ${expected} results, one per SurrealQL statement, and received ${
						Array.isArray(results) ? results.length : 'no list'
					}${committedNote(writes)}`,
				}),
			);

/**
 * Sends a request, again while it conflicts and `retry` allows. Each send
 * is the same request; `sent` counts them.
 */
const sending = <A, E extends { readonly _tag: string }>(
	send: Effect.Effect<A, E>,
	options: RunOptions | undefined,
): Effect.Effect<A, E> =>
	options?.retry === undefined
		? send
		: send.pipe(
				Effect.retry({
					schedule: options.retry as unknown as Schedule.Schedule<
						unknown,
						E
					>,
					while: (error: E) => error._tag === 'StatementConflict',
				}),
			);

/**
 * The metrics every run updates, through Effect's `Metric`:
 *
 * - `runs`: a counter of runs, by `statement` (its name) and `outcome`
 *   (`success`, the error's tag, or `interrupted`);
 * - `duration`: a timer of each run, from encoding to decoding, by
 *   `statement`;
 * - `retries`: a counter of requests sent again after a transaction
 *   conflict, by `statement`.
 */
export const metrics = {
	runs: Metric.counter('effect_surrealkit_statement_runs', {
		description: 'Statements run, by statement name and outcome',
		incremental: true,
	}),
	duration: Metric.timer('effect_surrealkit_statement_duration', {
		description: 'Time to run a statement, by statement name',
	}),
	retries: Metric.counter('effect_surrealkit_statement_retries', {
		description: 'Requests sent again after a transaction conflict',
		incremental: true,
	}),
} as const;

const { runs, duration: durations, retries } = metrics;

/**
 * The span and metrics of one run. The span carries the static text, the
 * sources, the number of rows when the result is a list and the error tag
 * on failure; never a binding value.
 */
const traced = <A, E extends { readonly _tag: string }>(
	name: string,
	sql: string,
	sources: ReadonlyArray<string> | undefined,
	rows: boolean,
	sent: { count: number },
	effect: Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
	Effect.gen(function* () {
		const started = yield* Clock.currentTimeNanos;
		const record = (outcome: string) =>
			Effect.gen(function* () {
				const ended = yield* Clock.currentTimeNanos;
				const statement = { statement: name };
				yield* Metric.update(
					Metric.withAttributes(runs, { ...statement, outcome }),
					1,
				);
				yield* Metric.update(
					Metric.withAttributes(durations, statement),
					Duration.nanos(ended - started),
				);
				if (sent.count > 1) {
					yield* Metric.update(
						Metric.withAttributes(retries, statement),
						sent.count - 1,
					);
				}
			});
		return yield* effect.pipe(
			Effect.tap((result) =>
				rows && Array.isArray(result)
					? Effect.annotateCurrentSpan(
							'db.response.returned_rows',
							result.length,
						)
					: Effect.void,
			),
			Effect.tapError((error) =>
				Effect.annotateCurrentSpan('error.type', error._tag),
			),
			Effect.withSpan(name, {
				kind: 'client',
				attributes: {
					'db.system.name': 'surrealdb',
					'db.operation.name': name,
					// Static text: values travel as bindings and are not recorded.
					'db.query.text': sql,
					...(sources !== undefined &&
						sources.length > 0 && {
							'code.file.path': sources.join(', '),
						}),
				},
			}),
			Effect.tap(() => record('success')),
			Effect.tapError((error) => record(error._tag)),
			Effect.onInterrupt(() => record('interrupted')),
		);
	});

/**
 * What a transaction refuses before encoding anything: a statement that
 * writes its own transaction control, and a name one statement binds and
 * another `LET`s, which would make one of them read the other's value.
 */
const refuseInTransaction = (
	statements: ReadonlyArray<Bound>,
): StatementScopeError | undefined => {
	for (const { statement } of statements) {
		if (statement.transactional) {
			return new StatementScopeError({
				statement: statement.name,
				message: `\`${statement.name}\` writes its own BEGIN, COMMIT or CANCEL, so it cannot run inside a transaction; run it on its own`,
			});
		}
	}
	for (const [index, { statement }] of statements.entries()) {
		for (const binding of Object.keys(statement.params)) {
			const definer = statements.find(
				(other, at) =>
					at !== index && other.statement.defines.includes(binding),
			);
			if (definer !== undefined) {
				return new StatementScopeError({
					statement: statement.name,
					binding,
					message: `\`$${binding}\` is bound by \`${statement.name}\` and defined with LET by \`${definer.statement.name}\`, and statements in one transaction share one variable scope, so one would read the other's value; rename one of them`,
				});
			}
		}
	}
	return undefined;
};

/**
 * What declares the `THROW` message `reason`: the statement's name (which
 * ones when several do) and, when one statement does and records where,
 * the source and lines; `undefined` when none does.
 */
const declarer = (
	statements: ReadonlyArray<AnyStatement>,
	reason: string,
): Declarer | undefined => {
	const declaring = statements.filter((statement) =>
		(statement.throws as ReadonlyArray<string>).includes(reason),
	);
	const names = [...new Set(declaring.map((statement) => statement.name))];
	if (names.length > 1) {
		return {
			statement: `one of ${names.map((name) => `\`${name}\``).join(', ')}`,
		};
	}
	const [first] = declaring;
	if (first === undefined) return undefined;
	const site = first.throwSites?.[reason];
	return site === undefined
		? { statement: first.name }
		: { statement: first.name, source: site.source, lines: site.lines };
};

/** The abort signal a request takes, as the SDK names it. */
type Signal = NonNullable<
	Parameters<ReturnType<Queryable['query']>['signal']>[0]
>;

/**
 * What a client sends its requests through: one statement's text with its
 * encoded bindings, or a transaction's joined text with the bindings of all
 * its statements. Each resolves to the list of results, one per SurrealQL
 * statement, or rejects as the SDK does (`classify` reads the rejection).
 *
 * @internal The SDK is the only transport the package offers; the scripted
 * client of `effect-surrealkit/testing` is the other.
 */
export interface Transport {
	readonly query: (
		statement: AnyStatement,
		bindings: Readonly<Record<string, unknown>>,
		signal: Signal,
	) => PromiseLike<unknown>;
	readonly transaction: (
		statements: ReadonlyArray<AnyStatement>,
		request: {
			readonly sql: string;
			readonly bindings: Readonly<Record<string, unknown>>;
		},
		signal: Signal,
	) => PromiseLike<unknown>;
}

const sdkTransport = (db: Queryable): Transport => {
	// One request per transaction, encoded once and sent again as it is on a
	// retry: the same bindings object makes the same `BoundQuery`.
	const requests = new WeakMap<object, BoundQuery>();
	return {
		query: (statement, bindings, signal) =>
			db
				.query(statement.sql, bindings as Record<string, unknown>)
				.signal(signal),
		transaction: (_, { sql, bindings }, signal) => {
			let request = requests.get(bindings);
			if (request === undefined) {
				request = new BoundQuery(
					sql,
					bindings as Record<string, unknown>,
				);
				requests.set(bindings, request);
			}
			return db.transaction([request], { signal });
		},
	};
};

/**
 * A client over an SDK connection or session.
 *
 * @example
 * const db = new Surreal();
 * await db.connect(url, { namespace, database, authentication });
 * const client = make(db);
 */
export const make = (db: Queryable): Client => makeWith(sdkTransport(db));

/** A client over a {@link Transport}. @internal */
export const makeWith = (transport: Transport): Client => ({
	run: <P extends Params, R extends Codec, Throws extends string, I>(
		statement: Statement<P, R, Throws>,
		...args: RunArgs<P, I>
	) => {
		const input = (args[0] ?? {}) as Readonly<Record<string, unknown>>;
		const options = args[1];
		const writes = changesOf([statement]);
		const sent = { count: 0 };
		return traced(
			statement.name,
			statement.sql,
			statement.sources,
			true,
			sent,
			Effect.gen(function* () {
				const bindings = yield* encode(statement, input);
				yield* checkValues(bindings, () => statement.name, options);
				const results = yield* sending(
					Effect.tryPromise({
						try: (signal) => {
							sent.count++;
							return transport.query(statement, bindings, signal);
						},
						catch: (cause) =>
							classify(
								statement.name,
								(reason) => declarer([statement], reason),
								cause,
							),
					}),
					options,
				);
				const slots = yield* countOf(
					statement.name,
					results,
					statement.slots,
					writes,
				).pipe(Effect.mapError(decodeOutcome(statement.name, writes)));
				return yield* decode(statement, slots, 0, writes).pipe(
					Effect.mapError(decodeOutcome(statement.name, writes)),
				);
			}),
		) as Effect.Effect<R['Type'], StatementError<Throws>>;
	},
	transaction: <const S extends ReadonlyArray<unknown>>(
		given: BoundStatements<S>,
		options?: RunOptions,
	) => {
		const statements = given as unknown as ReadonlyArray<Bound>;
		const name = `transaction(${statements
			.map(({ statement }) => statement.name)
			.join(', ')})`;
		const sql = statements.map(({ statement }) => statement.sql).join('\n');
		const sources = [
			...new Set(
				statements.flatMap(({ statement }) => statement.sources ?? []),
			),
		];
		const writes = changesOf(statements.map(({ statement }) => statement));
		const sent = { count: 0 };
		return traced(
			name,
			sql,
			sources,
			false,
			sent,
			Effect.gen(function* () {
				if (statements.length === 0) return [];
				const refused = refuseInTransaction(statements);
				if (refused !== undefined) return yield* Effect.fail(refused);
				const bindings: Record<string, unknown> = {};
				const boundBy = new Map<string, string>();
				for (const { input, statement } of statements) {
					const encoded = yield* encode(statement, input);
					for (const [key, value] of Object.entries(encoded)) {
						const first = boundBy.get(key);
						if (first === undefined) {
							boundBy.set(key, statement.name);
						} else if (!sameValue(bindings[key], value)) {
							return yield* Effect.fail(
								new StatementScopeError({
									statement: statement.name,
									binding: key,
									message: `\`$${key}\` is bound by \`${first}\` and by \`${statement.name}\` to values that are not certainly equal, and statements in one transaction share one variable scope; rename the binding in one of them`,
								}),
							);
						}
						bindings[key] = value;
					}
				}
				yield* checkValues(
					bindings,
					(binding) => boundBy.get(binding) ?? name,
					options,
				);
				const request = { sql, bindings };
				const results = yield* sending(
					Effect.tryPromise({
						try: (signal) => {
							sent.count++;
							return transport.transaction(
								statements.map((item) => item.statement),
								request,
								signal,
							);
						},
						catch: (cause) =>
							classify(
								name,
								(reason) =>
									declarer(
										statements.map(
											(item) => item.statement,
										),
										reason,
									),
								cause,
							),
					}),
					options,
				);
				const slots = yield* countOf(
					name,
					results,
					statements.reduce(
						(total, { statement }) => total + statement.slots,
						0,
					),
					writes,
				).pipe(Effect.mapError(decodeOutcome(name, writes)));
				const decoded: Array<unknown> = [];
				let offset = 0;
				for (const { statement } of statements) {
					decoded.push(
						yield* decode(statement, slots, offset, writes).pipe(
							Effect.mapError(decodeOutcome(name, writes)),
						),
					);
					offset += statement.slots;
				}
				return decoded;
			}),
		) as Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>>;
	},
});

/** Provides {@link SurrealClient} from an SDK connection or session. */
export const layer = (db: Queryable): Layer.Layer<SurrealClient> =>
	Layer.succeed(SurrealClient, make(db));

/** Runs a statement through the {@link SurrealClient} in context. */
export const run = <
	P extends Params,
	R extends Codec,
	Throws extends string,
	I = Input<P>,
>(
	statement: Statement<P, R, Throws>,
	...args: RunArgs<P, I>
): Effect.Effect<R['Type'], StatementError<Throws>, SurrealClient> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.run<P, R, Throws, I>(statement, ...args),
	);

/** Runs statements atomically through the {@link SurrealClient} in context. */
export const transaction = <const S extends ReadonlyArray<unknown>>(
	statements: BoundStatements<S>,
	options?: RunOptions,
): Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>, SurrealClient> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.transaction<S>(statements, options),
	);
