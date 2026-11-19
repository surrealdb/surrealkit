/**
 * Runs statements against SurrealDB through the JavaScript SDK.
 *
 * One request per statement: bindings are encoded through their codecs
 * first, so a value a codec refuses is never sent; the answer must have the
 * declared number of results, NONE in all but the last, and a last result
 * the statement's codec accepts. Interrupting the effect aborts the request.
 */
import { Context, Effect, Layer, Schema } from 'effect';
import { BoundQuery, type RetryValue, type SurrealSession } from 'surrealdb';

import type {
	AnyStatement,
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import { sameValue } from './equal.js';
import {
	StatementConflict,
	StatementDecodeError,
	StatementEncodeError,
	type StatementError,
	StatementRequestError,
	StatementThrown,
} from './errors.js';

/**
 * What a client needs from the SDK: a connection or a session. Statements
 * run with that session's authentication, namespace and database.
 */
export type Queryable = Pick<SurrealSession, 'query' | 'transaction'>;

export interface RunOptions {
	/**
	 * Send the request again when the engine reports a transaction conflict
	 * (the SDK's `retry`). Safe for one statement and for `transaction`,
	 * which are single atomic requests. Off unless the connection sets it.
	 */
	readonly retry?: RetryValue;
}

/** The input argument: optional when no binding needs a value. */
export type InputOf<P extends Params> =
	Record<never, never> extends Input<P>
		? readonly [input?: Input<P>]
		: readonly [input: Input<P>];

/** The arguments after the statement: its input, then the options. */
export type RunArgs<P extends Params> =
	Record<never, never> extends Input<P>
		? readonly [input?: Input<P>, options?: RunOptions]
		: readonly [input: Input<P>, options?: RunOptions];

/** A statement together with the values of its bindings. */
export interface Bound<
	R extends Codec = Codec,
	Throws extends string = string,
> {
	readonly statement: Statement<Params, R, Throws>;
	readonly input: Readonly<Record<string, unknown>>;
}

/** Pairs a statement with its input, to run it inside {@link Client.transaction}. */
export const bind = <P extends Params, R extends Codec, Throws extends string>(
	statement: Statement<P, R, Throws>,
	...input: InputOf<P>
): Bound<R, Throws> => ({
	statement,
	input: (input[0] ?? {}) as Readonly<Record<string, unknown>>,
});

/** The decoded results of the statements of a transaction, in order. */
export type Results<S extends ReadonlyArray<Bound>> = {
	readonly [I in keyof S]: S[I] extends Bound<infer R, string>
		? R['Type']
		: never;
};

/** The messages any statement of a transaction can `THROW`. */
export type ThrownByAny<S extends ReadonlyArray<Bound>> =
	S[number] extends Bound<Codec, infer T> ? T : never;

export interface Client {
	/**
	 * Runs one statement and decodes its result.
	 *
	 * The statement is one request. Unless its own text says otherwise
	 * (`BEGIN … COMMIT`), each of its SurrealQL statements commits on its
	 * own; use {@link Client.transaction} for all-or-nothing.
	 */
	readonly run: <P extends Params, R extends Codec, Throws extends string>(
		statement: Statement<P, R, Throws>,
		...args: RunArgs<P>
	) => Effect.Effect<R['Type'], StatementError<Throws>>;

	/**
	 * Runs several statements as one atomic request and decodes each result:
	 * all of them take effect, or none does.
	 *
	 * The statements share one variable scope, as SurrealQL has it, so
	 * before anything is sent a binding name is refused when two
	 * statements bind it to values that are not certainly equal, or when
	 * one statement binds a name another `LET`s (a `StatementEncodeError`
	 * naming both). A statement whose own text writes `BEGIN`, `COMMIT` or
	 * `CANCEL` cannot be nested in this one and is refused as a
	 * `StatementRequestError`. The SDK refuses a top-level `RETURN` that
	 * is followed by another statement, because some engines end the
	 * transaction there. A declared `THROW` names the statement that
	 * declares it.
	 */
	readonly transaction: <const S extends ReadonlyArray<Bound>>(
		statements: S,
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

/**
 * The SDK chains server errors through `cause`. They are matched by shape
 * rather than by class, so a second installed copy of the SDK still maps.
 */
const classify = <Throws extends string>(
	name: string,
	/** The statement that declares `reason`, or `undefined` when none does. */
	declaredBy: (reason: string) => string | undefined,
	cause: unknown,
): StatementRequestError | StatementConflict | StatementThrown<Throws> => {
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
			const statement = declaredBy(reason);
			if (statement !== undefined) {
				return new StatementThrown<Throws>({ statement, reason });
			}
		}
		if (isTransactionConflict === true) {
			return new StatementConflict({
				statement: name,
				message: error.message,
				cause,
			});
		}
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

/** Decodes the results of one statement out of `results`, from `offset`. */
const decode = (
	statement: AnyStatement,
	results: ReadonlyArray<unknown>,
	offset: number,
): Effect.Effect<unknown, StatementDecodeError> => {
	const last = offset + statement.slots - 1;
	for (let slot = offset; slot < last; slot++) {
		if (results[slot] !== undefined) {
			return Effect.fail(
				new StatementDecodeError({
					statement: statement.name,
					slot: slot - offset + 1,
					message: `result ${slot - offset + 1} of ${statement.slots} must be NONE; only the last result of a statement is decoded`,
				}),
			);
		}
	}
	return Schema.decodeUnknownEffect(statement.result)(results[last], {
		onExcessProperty: 'error',
	}).pipe(
		Effect.mapError(
			(error) =>
				new StatementDecodeError({
					statement: statement.name,
					slot: statement.slots,
					message: error.message,
				}),
		),
	);
};

const countOf = (
	name: string,
	results: unknown,
	expected: number,
): Effect.Effect<ReadonlyArray<unknown>, StatementDecodeError> =>
	Array.isArray(results) && results.length === expected
		? Effect.succeed(results)
		: Effect.fail(
				new StatementDecodeError({
					statement: name,
					slot: 0,
					message: `expected ${expected} results, one per SurrealQL statement, and received ${
						Array.isArray(results) ? results.length : 'no list'
					}`,
				}),
			);

const traced = <A, E>(
	name: string,
	sql: string,
	effect: Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
	effect.pipe(
		Effect.withSpan(name, {
			kind: 'client',
			attributes: {
				'db.system.name': 'surrealdb',
				'db.operation.name': name,
				// Static text: values travel as bindings and are not recorded.
				'db.query.text': sql,
			},
		}),
	);

/**
 * What a transaction refuses before encoding anything: a statement that
 * writes its own transaction control, and a name one statement binds and
 * another `LET`s, which would make one of them read the other's value.
 */
const refuseInTransaction = (
	statements: ReadonlyArray<Bound>,
): StatementEncodeError | StatementRequestError | undefined => {
	for (const { statement } of statements) {
		if (statement.transactional) {
			return new StatementRequestError({
				statement: statement.name,
				message: `\`${statement.name}\` writes its own BEGIN, COMMIT or CANCEL, so it cannot run inside a transaction; run it on its own`,
				cause: undefined,
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
				return new StatementEncodeError({
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
 * The statement that declares the `THROW` message `reason`: its name, which
 * ones when several do, or `undefined` when none does.
 */
const declarer = (
	statements: ReadonlyArray<AnyStatement>,
	reason: string,
): string | undefined => {
	const names = [
		...new Set(
			statements
				.filter((statement) =>
					(statement.throws as ReadonlyArray<string>).includes(
						reason,
					),
				)
				.map((statement) => statement.name),
		),
	];
	if (names.length <= 1) return names[0];
	return `one of ${names.map((name) => `\`${name}\``).join(', ')}`;
};

/**
 * A client over an SDK connection or session.
 *
 * @example
 * const db = new Surreal();
 * await db.connect(url, { namespace, database, authentication });
 * const client = make(db);
 */
export const make = (db: Queryable): Client => ({
	run: <P extends Params, R extends Codec, Throws extends string>(
		statement: Statement<P, R, Throws>,
		...args: RunArgs<P>
	) => {
		const input: Readonly<Record<string, unknown>> = args[0] ?? {};
		const options = args[1];
		return traced(
			statement.name,
			statement.sql,
			Effect.gen(function* () {
				const bindings = yield* encode(statement, input);
				const results = yield* Effect.tryPromise({
					try: (signal) => {
						const query = db
							.query(statement.sql, bindings)
							.signal(signal);
						return options?.retry === undefined
							? query
							: query.retry(options.retry);
					},
					catch: (cause) =>
						classify(
							statement.name,
							(reason) => declarer([statement], reason),
							cause,
						),
				});
				const slots = yield* countOf(
					statement.name,
					results,
					statement.slots,
				);
				return yield* decode(statement, slots, 0);
			}),
		) as Effect.Effect<R['Type'], StatementError<Throws>>;
	},
	transaction: <const S extends ReadonlyArray<Bound>>(
		statements: S,
		options?: RunOptions,
	) => {
		const name = `transaction(${statements
			.map(({ statement }) => statement.name)
			.join(', ')})`;
		const sql = statements.map(({ statement }) => statement.sql).join('\n');
		return traced(
			name,
			sql,
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
								new StatementEncodeError({
									statement: statement.name,
									binding: key,
									message: `\`$${key}\` is bound by \`${first}\` and by \`${statement.name}\` to values that are not certainly equal, and statements in one transaction share one variable scope; rename the binding in one of them`,
								}),
							);
						}
						bindings[key] = value;
					}
				}
				const results = yield* Effect.tryPromise({
					try: (signal) =>
						db.transaction([new BoundQuery(sql, bindings)], {
							signal,
							...(options?.retry === undefined
								? {}
								: { retry: options.retry }),
						}),
					catch: (cause) =>
						classify(
							name,
							(reason) =>
								declarer(
									statements.map((item) => item.statement),
									reason,
								),
							cause,
						),
				});
				const slots = yield* countOf(
					name,
					results,
					statements.reduce(
						(total, { statement }) => total + statement.slots,
						0,
					),
				);
				const decoded: Array<unknown> = [];
				let offset = 0;
				for (const { statement } of statements) {
					decoded.push(yield* decode(statement, slots, offset));
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
export const run = <P extends Params, R extends Codec, Throws extends string>(
	statement: Statement<P, R, Throws>,
	...args: RunArgs<P>
): Effect.Effect<R['Type'], StatementError<Throws>, SurrealClient> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.run(statement, ...args),
	);

/** Runs statements atomically through the {@link SurrealClient} in context. */
export const transaction = <const S extends ReadonlyArray<Bound>>(
	statements: S,
	options?: RunOptions,
): Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>, SurrealClient> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.transaction(statements, options),
	);
