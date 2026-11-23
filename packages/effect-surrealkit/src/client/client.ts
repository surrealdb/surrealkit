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
	Context,
	Effect,
	Layer,
	type Schedule,
	type Schema,
	type Scope,
	Stream,
} from 'effect';
import type { SurrealSession } from 'surrealdb';

import type { ElementOf } from '../statement/expr.js';
import type { LiveStatement } from '../statement/live.js';
import type {
	CheckedInput,
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import {
	type BulkInput,
	type BulkOptions,
	type BulkSummary,
	type ListBindingsOf,
	makeBulk,
} from './bulk.js';
import type {
	BulkWriteError,
	StatementConflict,
	StatementEncodeError,
	StatementError,
	StatementRequestError,
	StatementScopeError,
	StatementUnsupported,
	TransactionOutcomeUnknown,
} from './errors.js';
import {
	makeWithTransaction,
	type TransactionClient,
	type TransactionError,
	type TransactionOptions,
	withTransactionContext,
} from './interactive.js';
import { classify } from './internal/classify.js';
import {
	changesOf,
	countOf,
	decode,
	decodeOutcome,
} from './internal/decode.js';
import { checkValues, encode } from './internal/encode.js';
import { joinWith, requestPart } from './internal/join.js';
import { recording, traced } from './internal/trace.js';
import {
	type Request,
	sdkTransport,
	sending,
	type Transport,
} from './internal/transport.js';
import { type LiveError, type LiveEvent, makeLive } from './live.js';
import {
	makePipeline,
	type PipelineOptions,
	type PipelineResults,
} from './pipeline.js';
import { makeStream } from './stream.js';

/**
 * What a client needs from a connection or session. Interactive transactions
 * and live queries also require the corresponding optional SDK methods.
 */
export type Queryable = Pick<SurrealSession, 'query' | 'transaction'> &
	Partial<Pick<SurrealSession, 'beginTransaction' | 'liveOf'>>;

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
export type RunArgs<P extends Params, I = Input<P>> = RunArgsWithOptions<
	P,
	I,
	RunOptions
>;

export type RunArgsWithOptions<P extends Params, I, Options> =
	Record<never, never> extends Input<P>
		? readonly [input?: CheckedInput<P, I>, options?: Options]
		: readonly [input: CheckedInput<P, I>, options?: Options];

/** Only {@link bind} makes a {@link Bound}: no other code can name this. */
declare const BoundTypeId: unique symbol;

/**
 * A statement together with the values of its bindings, made by
 * {@link bind}, which checks the input against the statement's bindings.
 */
export interface Bound<
	R extends Codec = Codec,
	Throws extends string = string,
	Unique extends string = string,
> {
	readonly [BoundTypeId]: 'made by bind';
	readonly statement: Statement<Params, R, Throws, Unique>;
	readonly input: Readonly<Record<string, unknown>>;
}

/** Pairs a statement with its input, to run it inside {@link Client.transaction}. */
export const bind = <
	P extends Params,
	R extends Codec,
	Throws extends string,
	Unique extends string = never,
	I = Input<P>,
>(
	statement: Statement<P, R, Throws, Unique>,
	...input: InputOf<P, I>
): Bound<R, Throws, Unique> =>
	({
		statement,
		input: (input[0] ?? {}) as Readonly<Record<string, unknown>>,
	}) as unknown as Bound<R, Throws, Unique>;

/**
 * The statements sent together as the caller wrote them, `S`: each one
 * not made by {@link bind} is typed as a message saying so.
 */
export type BoundStatements<S extends ReadonlyArray<unknown>> = {
	// Only the mark is tested, so a bound statement of a generic result passes.
	readonly [I in keyof S]: S[I] extends {
		readonly [BoundTypeId]: 'made by bind';
	}
		? S[I]
		: 'statements that run together are paired with their input by `bind`';
};

/** The decoded results of the statements of a transaction, in order. */
export type Results<S extends ReadonlyArray<unknown>> = {
	readonly [I in keyof S]: S[I] extends Bound<infer R, string, string>
		? R['Type']
		: never;
};

/** The messages any statement of a transaction can `THROW`. */
export type ThrownByAny<S extends ReadonlyArray<unknown>> =
	S[number] extends Bound<Codec, infer T, string> ? T : never;

/** The unique indexes any statement of a transaction can duplicate a value of. */
export type UniqueIndexesOfAny<S extends ReadonlyArray<unknown>> =
	S[number] extends Bound<Codec, string, infer U> ? U : never;

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
		Unique extends string = never,
		I = Input<P>,
	>(
		statement: Statement<P, R, Throws, Unique>,
		...args: RunArgs<P, I>
	) => Effect.Effect<R['Type'], StatementError<Throws, Unique>>;

	/**
	 * Runs several statements as one atomic request and decodes each result:
	 * all of them take effect, or none does.
	 *
	 * Shared binding names and top-level locals are renamed per statement.
	 * A statement with its own transaction control cannot be nested and
	 * fails with a `StatementScopeError` before sending. The SDK refuses a top-level
	 * `RETURN` followed by another statement, because some engines end the
	 * transaction there. A declared `THROW` names the statement that
	 * declares it.
	 */
	readonly transaction: <const S extends ReadonlyArray<unknown>>(
		statements: BoundStatements<S>,
		options?: RunOptions,
	) => Effect.Effect<
		Results<S>,
		StatementError<ThrownByAny<S>, UniqueIndexesOfAny<S>>
	>;

	/** Sends a list together and returns one outcome per statement. */
	readonly pipeline: <const S extends ReadonlyArray<unknown>>(
		statements: BoundStatements<S>,
		options?: PipelineOptions,
	) => Effect.Effect<
		PipelineResults<S>,
		StatementEncodeError | StatementScopeError | StatementRequestError
	>;

	/** Opens a transaction for the body and commits when the body succeeds. */
	readonly withTransaction: <A, E, R>(
		body: (tx: TransactionClient) => Effect.Effect<A, E, R>,
		options?: TransactionOptions,
	) => Effect.Effect<A, E | TransactionError, R>;

	/** Runs a statement over chunks of one list binding. */
	readonly bulk: <
		P extends Params,
		R extends Codec,
		T extends string,
		K extends ListBindingsOf<P>,
		E = never,
		C = never,
		U extends string = never,
	>(
		statement: Statement<P, R, T, U>,
		input: BulkInput<P, K, E, C>,
		options: BulkOptions<K, ElementOf<P[K]['Type']>, E, C>,
	) => Effect.Effect<
		BulkSummary<R['Type']>,
		| StatementError<T, U>
		| BulkWriteError<T, U>
		| StatementUnsupported
		| TransactionOutcomeUnknown
		| E,
		C
	>;

	/** Decodes an array result one row at a time. */
	readonly stream: <
		P extends Params,
		Row extends Codec,
		T extends string,
		U extends string = never,
		I = Input<P>,
	>(
		statement: Statement<P, Schema.$Array<Row>, T, U>,
		...input: InputOf<P, I>
	) => Stream.Stream<
		Row['Type'],
		StatementError<T, U> | StatementUnsupported
	>;

	/** Acquires a live query in the current scope. */
	readonly liveScoped: <
		P extends Params,
		Row extends Codec,
		Id extends Codec,
		I = Input<P>,
	>(
		statement: LiveStatement<P, Row, Id>,
		...input: InputOf<P, I>
	) => Effect.Effect<
		Stream.Stream<LiveEvent<Row['Type'], Id['Type']>, LiveError>,
		LiveError,
		Scope.Scope
	>;

	/** Streams changes for the lifetime of the subscription. */
	readonly live: <
		P extends Params,
		Row extends Codec,
		Id extends Codec,
		I = Input<P>,
	>(
		statement: LiveStatement<P, Row, Id>,
		...input: InputOf<P, I>
	) => Stream.Stream<LiveEvent<Row['Type'], Id['Type']>, LiveError>;
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

const makeRun =
	(transport: Transport): Client['run'] =>
	<
		P extends Params,
		R extends Codec,
		Throws extends string,
		Unique extends string,
		I,
	>(
		statement: Statement<P, R, Throws, Unique>,
		...args: RunArgs<P, I>
	) => {
		const input = (args[0] ?? {}) as Readonly<Record<string, unknown>>;
		const options = args[1];
		const writes = changesOf([statement]);
		return traced(
			statement.name,
			statement.sql,
			statement.sources,
			statement.variant,
			true,
			recording('run', statement.name, (recorder) =>
				Effect.gen(function* () {
					const request = yield* recorder.encode(
						Effect.gen(function* () {
							const bindings = yield* encode(statement, input);
							yield* checkValues(
								bindings,
								() => statement.name,
								options,
							);
							return {
								mode: 'query',
								sql: statement.sql,
								bindings,
								parts: [
									requestPart(
										statement,
										bindings,
										0,
										statement.slots,
									),
								],
							} satisfies Request;
						}),
					);
					const results = yield* sending(
						recorder.wait(
							Effect.tryPromise({
								try: (signal) =>
									transport.collect(request, signal),
								catch: (cause) =>
									classify(
										statement.name,
										[statement],
										cause,
									),
							}),
						),
						options,
					);
					yield* recorder.server(statement.name, undefined);
					const decoded = yield* recorder.decode(
						Effect.gen(function* () {
							const slots = yield* countOf(
								statement.name,
								results,
								statement.slots,
								writes,
							);
							return yield* decode(statement, slots, 0, writes);
						}).pipe(
							Effect.mapError(
								decodeOutcome(statement.name, writes),
							),
						),
					);
					if (Array.isArray(decoded))
						yield* recorder.rows(statement.name, decoded.length);
					return decoded;
				}),
			),
		) as Effect.Effect<R['Type'], StatementError<Throws, Unique>>;
	};

const makeTransaction =
	(transport: Transport): Client['transaction'] =>
	<const S extends ReadonlyArray<unknown>>(
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
		const descriptors = statements.map(({ statement }) => statement);
		const writes = changesOf(descriptors);
		return traced(
			name,
			sql,
			sources,
			undefined,
			false,
			recording('transaction', name, (recorder) =>
				Effect.gen(function* () {
					if (statements.length === 0) return [];
					const request = yield* recorder.encode(
						Effect.gen(function* () {
							const joined = yield* joinWith(
								statements.map(({ statement, input }) => ({
									statement,
									bindings: Effect.suspend(() =>
										encode(statement, input),
									),
								})),
								{ mode: 'atomic', refuseTransactional: true },
							);
							// Join first so scope and codec failures keep precedence.
							for (const {
								statement,
								bindings,
							} of joined.parts) {
								yield* checkValues(
									bindings,
									() => statement.name,
									options,
								);
							}
							return joined;
						}),
					);
					const results = yield* sending(
						recorder.wait(
							Effect.tryPromise({
								try: (signal) =>
									transport.collect(request, signal),
								catch: (cause) =>
									classify(name, descriptors, cause),
							}),
						),
						options,
					);
					return yield* recorder.decode(
						Effect.gen(function* () {
							const slots = yield* countOf(
								name,
								results,
								request.parts.reduce(
									(total, part) => total + part.slots,
									0,
								),
								writes,
							);
							const decoded: Array<unknown> = [];
							for (const { statement, offset } of request.parts) {
								// Register each part before its optional row count, even when names repeat.
								yield* recorder.server(
									statement.name,
									undefined,
								);
								const value = yield* decode(
									statement,
									slots,
									offset,
									writes,
								);
								if (Array.isArray(value))
									yield* recorder.rows(
										statement.name,
										value.length,
									);
								decoded.push(value);
							}
							return decoded;
						}).pipe(Effect.mapError(decodeOutcome(name, writes))),
					);
				}),
			),
		) as Effect.Effect<
			Results<S>,
			StatementError<ThrownByAny<S>, UniqueIndexesOfAny<S>>
		>;
	};

/** A client over a {@link Transport}. @internal */
export const makeWith = (transport: Transport): Client => ({
	run: makeRun(transport),
	transaction: makeTransaction(transport),
	pipeline: makePipeline(transport),
	withTransaction: makeWithTransaction(transport),
	bulk: makeBulk({ transport }),
	stream: makeStream(transport),
	...makeLive(transport),
});

/** Provides {@link SurrealClient} from an SDK connection or session. */
export const layer = (db: Queryable): Layer.Layer<SurrealClient> =>
	Layer.succeed(SurrealClient, make(db));

/** Runs a statement through the {@link SurrealClient} in context. */
export const run = <
	P extends Params,
	R extends Codec,
	Throws extends string,
	Unique extends string = never,
	I = Input<P>,
>(
	statement: Statement<P, R, Throws, Unique>,
	...args: RunArgs<P, I>
): Effect.Effect<R['Type'], StatementError<Throws, Unique>, SurrealClient> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.run<P, R, Throws, Unique, I>(statement, ...args),
	);

/** Runs statements atomically through the {@link SurrealClient} in context. */
export const transaction = <const S extends ReadonlyArray<unknown>>(
	statements: BoundStatements<S>,
	options?: RunOptions,
): Effect.Effect<
	Results<S>,
	StatementError<ThrownByAny<S>, UniqueIndexesOfAny<S>>,
	SurrealClient
> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.transaction<S>(statements, options),
	);

/** Sends a pipeline through the client in context. */
export const pipeline = <const S extends ReadonlyArray<unknown>>(
	statements: BoundStatements<S>,
	options?: PipelineOptions,
): Effect.Effect<
	PipelineResults<S>,
	StatementEncodeError | StatementScopeError | StatementRequestError,
	SurrealClient
> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		client.pipeline<S>(statements, options),
	);

/** Runs an effect with a transaction-bound client in context. */
export const withTransaction = <A, E, R>(
	effect: Effect.Effect<A, E, R>,
	options?: TransactionOptions,
): Effect.Effect<A, E | TransactionError, R | SurrealClient> =>
	Effect.flatMap(Effect.service(SurrealClient), (client) =>
		withTransactionContext(client, effect, options),
	);

/** Streams rows through the client in context. */
export const stream = <
	P extends Params,
	Row extends Codec,
	T extends string,
	U extends string = never,
	I = Input<P>,
>(
	statement: Statement<P, Schema.$Array<Row>, T, U>,
	...input: InputOf<P, I>
): Stream.Stream<
	Row['Type'],
	StatementError<T, U> | StatementUnsupported,
	SurrealClient
> =>
	Stream.unwrap(
		Effect.map(Effect.service(SurrealClient), (client) =>
			client.stream<P, Row, T, U, I>(statement, ...input),
		),
	);
