/**
 * Runs statements against SurrealDB through the JavaScript SDK.
 *
 * One request per statement: bindings are encoded through their codecs
 * first, and each encoded value is checked to be one the SDK sends without
 * loss, so a refused value is never sent; the answer must have the
 * declared number of results, NONE in all but the last, and a last result
 * the statement's codec accepts. Interrupting the effect aborts the request.
 */
import { Context, Effect, Layer, type Schedule } from 'effect';
import type { SurrealSession } from 'surrealdb';

import type {
	CheckedInput,
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import { sameValue } from './equal.js';
import {
	type StatementConflict,
	type StatementError,
	StatementScopeError,
} from './errors.js';
import { classify, declarer } from './internal/classify.js';
import {
	changesOf,
	countOf,
	decode,
	decodeOutcome,
} from './internal/decode.js';
import { checkValues, encode } from './internal/encode.js';
import { refuseInTransaction } from './internal/join.js';
import { traced } from './internal/trace.js';
import { sdkTransport, sending, type Transport } from './internal/transport.js';

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
			statement.variant,
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
			undefined,
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
