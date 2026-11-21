import { Context, Duration, Effect, Exit, type Schedule, Stream } from 'effect';
import {
	ConnectionUnavailableError,
	InvalidSessionError,
	MissingNamespaceDatabaseError,
	ServerError,
	UnavailableFeatureError,
	UnsupportedFeatureError,
} from 'surrealdb';

import type { ElementOf } from '../statement/expr.js';
import type {
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
import {
	type Bound,
	type BoundStatements,
	type Client,
	type Results,
	type RunArgsWithOptions,
	type RunOptions,
	SurrealClient,
	type ThrownByAny,
} from './client.js';
import {
	type BulkWriteError,
	StatementConflict,
	type StatementError,
	StatementRequestError,
	StatementScopeError,
	StatementUnsupported,
	TransactionOutcomeUnknown,
} from './errors.js';
import {
	classify,
	declarer,
	firstFailure,
	messageOf,
} from './internal/classify.js';
import { countOf, decode } from './internal/decode.js';
import { checkValues, encode } from './internal/encode.js';
import { joinWith } from './internal/join.js';
import { withClientSpan } from './internal/span.js';
import { recording } from './internal/trace.js';
import type {
	RequestPart,
	SlotAnswer,
	TransactionHandle,
	Transport,
} from './internal/transport.js';

export interface TransactionOptions {
	/** Retry the whole transaction body on a statement or commit conflict. */
	readonly retry?: Schedule.Schedule<unknown, StatementConflict>;
}

type TransactionRunOptions = Omit<RunOptions, 'retry'>;

export interface TransactionClient {
	readonly run: <
		P extends Params,
		R extends Codec,
		T extends string,
		I = Input<P>,
	>(
		statement: Statement<P, R, T>,
		...args: RunArgsWithOptions<P, I, TransactionRunOptions>
	) => Effect.Effect<R['Type'], StatementError<T>>;

	readonly transaction: <const S extends ReadonlyArray<unknown>>(
		statements: BoundStatements<S>,
		options?: TransactionRunOptions,
	) => Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>>;

	readonly bulk: <
		P extends Params,
		R extends Codec,
		T extends string,
		K extends ListBindingsOf<P>,
		E = never,
		C = never,
	>(
		statement: Statement<P, R, T>,
		input: BulkInput<P, K, E, C>,
		options: Omit<
			BulkOptions<K, ElementOf<P[K]['Type']>, E, C>,
			'retry' | 'atomic'
		>,
	) => Effect.Effect<
		BulkSummary<R['Type']>,
		StatementError<T> | BulkWriteError<T> | StatementUnsupported | E,
		C
	>;
}

export type TransactionError =
	| StatementConflict
	| StatementRequestError
	| StatementScopeError
	| StatementUnsupported
	| TransactionOutcomeUnknown;

const name = 'withTransaction';
const commitDeadline = '30 seconds';

const activeTransaction = Context.Reference<boolean>(
	'effect-surrealkit/activeTransaction',
	{ defaultValue: () => false },
);

const scopeError = (operation: string): StatementScopeError =>
	new StatementScopeError({
		statement: operation,
		message: `${operation} cannot run inside an interactive transaction`,
	});

const unsupported = (cause?: unknown): StatementUnsupported =>
	new StatementUnsupported({
		feature: 'interactive transaction',
		message: 'the connection does not support interactive transactions',
		...(cause !== undefined && { cause }),
	});

const requestError = (cause: unknown): StatementRequestError =>
	new StatementRequestError({
		statement: name,
		message: messageOf(cause),
		cause,
	});

const commitUnknown = (cause: unknown): TransactionOutcomeUnknown =>
	new TransactionOutcomeUnknown({
		message:
			'The transaction commit was not acknowledged. Read back the data before retrying.',
		cause,
	});

const commitError = (cause: unknown): TransactionError => {
	const classified = classify(name, () => undefined, cause);
	if (classified instanceof StatementConflict) return classified;
	// The server answered, or the SDK refused to send the request.
	if (
		cause instanceof ServerError ||
		cause instanceof ConnectionUnavailableError ||
		cause instanceof InvalidSessionError ||
		cause instanceof MissingNamespaceDatabaseError ||
		cause instanceof UnsupportedFeatureError ||
		cause instanceof UnavailableFeatureError
	)
		return requestError(cause);
	return commitUnknown(cause);
};

const recordControl = <A, E>(
	operation: 'begin' | 'commit' | 'cancel',
	request: Effect.Effect<A, E>,
): Effect.Effect<A, E> => {
	const span = `${name}.${operation}`;
	return recording('interactive', span, (recorder) =>
		recorder.wait(request),
	).pipe(
		withClientSpan(span, {
			kind: 'client',
			attributes: {
				'db.system.name': 'surrealdb',
				'db.operation.name': operation,
			},
		}),
	);
};

const commit = (
	handle: TransactionHandle,
): Effect.Effect<void, TransactionError> =>
	recordControl(
		'commit',
		Effect.interruptible(
			Effect.tryPromise({
				try: () => handle.commit(),
				catch: commitError,
			}),
		).pipe(
			Effect.timeoutOrElse({
				duration: commitDeadline,
				orElse: () =>
					Effect.fail(
						commitUnknown(
							new Error(
								`No acknowledgement within ${commitDeadline}`,
							),
						),
					),
			}),
		),
	);

const cancel = (handle: TransactionHandle): Effect.Effect<void> =>
	recordControl(
		'cancel',
		Effect.tryPromise({
			try: () => handle.cancel(),
			catch: (cause) => cause,
		}),
	).pipe(
		Effect.catch(() =>
			Effect.annotateCurrentSpan('effect_surrealkit.cancel_failed', true),
		),
	);

const serverTime = (
	answers: ReadonlyArray<SlotAnswer>,
): Duration.Duration | undefined => {
	let total: Duration.Duration | undefined;
	for (const { server } of answers) {
		if (server !== undefined)
			total = total === undefined ? server : Duration.sum(total, server);
	}
	return total;
};

const execute = (
	send: TransactionHandle['send'],
	requestName: string,
	statements: ReadonlyArray<Bound>,
	options?: TransactionRunOptions,
): Effect.Effect<ReadonlyArray<unknown>, StatementError<string>> =>
	recording('interactive', requestName, (recorder) =>
		Effect.gen(function* () {
			if (statements.length === 0) return [];
			const request = yield* recorder.encode(
				Effect.gen(function* () {
					const request = yield* joinWith(
						statements.map(({ statement, input }) => ({
							statement,
							bindings: Effect.suspend(() =>
								encode(statement, input),
							),
						})),
						{ mode: 'query', refuseTransactional: true },
					);
					for (const part of request.parts) {
						yield* checkValues(
							part.bindings,
							() => part.statement.name,
							options,
						);
					}
					return request;
				}),
			);
			const failureOf = (cause: unknown) =>
				cause instanceof StatementScopeError
					? cause
					: classify(
							requestName,
							(reason) =>
								declarer(
									statements.map(
										({ statement }) => statement,
									),
									reason,
								),
							cause,
						);
			const answers = yield* recorder.wait(
				Effect.tryPromise({
					try: (signal) => send(request, signal),
					catch: failureOf,
				}),
			);
			const recordServer = ({ offset, slots, statement }: RequestPart) =>
				recorder.server(
					statement.name,
					serverTime(answers.slice(offset, offset + slots)),
				);
			const failure = firstFailure(answers);
			if (failure !== undefined) {
				yield* Effect.forEach(request.parts, recordServer, {
					discard: true,
				});
				return yield* Effect.fail(failureOf(failure.error));
			}
			return yield* recorder.decode(
				Effect.gen(function* () {
					// The writes are still provisional. The body can propagate the error and cancel them.
					const values = yield* countOf(
						requestName,
						answers.map((slot) =>
							slot.ok ? slot.value : undefined,
						),
						request.parts.reduce(
							(total, part) => total + part.slots,
							0,
						),
						'none',
					);
					const decoded: Array<unknown> = [];
					for (const part of request.parts) {
						const { statement, offset } = part;
						yield* recordServer(part);
						const value = yield* decode(
							statement,
							values,
							offset,
							'none',
						);
						if (Array.isArray(value))
							yield* recorder.rows(statement.name, value.length);
						decoded.push(value);
					}
					return decoded;
				}),
			);
		}),
	).pipe(withClientSpan(requestName, { kind: 'client' }));

const transactionClient = (
	send: TransactionHandle['send'],
): TransactionClient => ({
	run: <P extends Params, R extends Codec, T extends string, I>(
		statement: Statement<P, R, T>,
		...args: RunArgsWithOptions<P, I, TransactionRunOptions>
	) => {
		const input = (args[0] ?? {}) as Readonly<Record<string, unknown>>;
		const bound = { statement, input } as unknown as Bound;
		return execute(send, statement.name, [bound], args[1]).pipe(
			Effect.map(([value]) => value),
		) as Effect.Effect<R['Type'], StatementError<T>>;
	},
	transaction: <const S extends ReadonlyArray<unknown>>(
		given: BoundStatements<S>,
		options?: TransactionRunOptions,
	) => {
		const statements = given as unknown as ReadonlyArray<Bound>;
		const labels = statements
			.map(({ statement }) => statement.name)
			.join(', ');
		return execute(
			send,
			`transaction(${labels})`,
			statements,
			options,
		) as Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>>;
	},
	bulk: makeBulk({ transaction: { send } }),
});

const beginTransaction = (begin: NonNullable<Transport['begin']>) =>
	recordControl(
		'begin',
		Effect.tryPromise({
			try: (signal) => begin(signal),
			catch: (cause) =>
				cause instanceof UnsupportedFeatureError ||
				cause instanceof UnavailableFeatureError
					? unsupported(cause)
					: requestError(cause),
		}),
	);

const attempt = <A, E, R>(
	begin: NonNullable<Transport['begin']>,
	body: (tx: TransactionClient) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | TransactionError, R> =>
	Effect.uninterruptibleMask((restore) =>
		Effect.gen(function* () {
			// Wait for a handle before honoring interruption so a late begin can be cancelled.
			const handle = yield* beginTransaction(begin);
			let open = true;
			const send: TransactionHandle['send'] = (request, signal) =>
				open
					? handle.send(request, signal)
					: Promise.reject(
							new StatementScopeError({
								statement:
									request.parts[0]?.statement.name ?? name,
								message:
									'the interactive transaction has already ended',
							}),
						);
			const tx = transactionClient(send);
			const result = yield* Effect.exit(
				restore(
					Effect.suspend(() => body(tx)).pipe(
						Effect.provideService(activeTransaction, true),
					),
				),
			);
			open = false;
			if (Exit.isFailure(result)) {
				yield* cancel(handle);
				return yield* Effect.failCause(result.cause);
			}
			yield* commit(handle).pipe(
				Effect.tapError((error) =>
					error instanceof TransactionOutcomeUnknown
						? Effect.void
						: cancel(handle),
				),
			);
			return result.value;
		}),
	);

export const makeWithTransaction =
	(transport: Pick<Transport, 'begin'>): Client['withTransaction'] =>
	<A, E, R>(
		body: (tx: TransactionClient) => Effect.Effect<A, E, R>,
		options?: TransactionOptions,
	): Effect.Effect<A, E | TransactionError, R> =>
		Effect.gen(function* () {
			if (yield* activeTransaction)
				return yield* Effect.fail(scopeError(name));
			if (transport.begin === undefined)
				return yield* Effect.fail(unsupported());
			const run = attempt(transport.begin, body);
			const retry = options?.retry;
			if (retry === undefined) return yield* run;
			return yield* Effect.retry(run, {
				schedule: retry as Schedule.Schedule<
					unknown,
					E | TransactionError
				>,
				while: (error) => error instanceof StatementConflict,
			});
		}).pipe(
			withClientSpan(name, {
				kind: 'client',
				attributes: { 'db.system.name': 'surrealdb' },
			}),
		);

const contextClient = (tx: TransactionClient): Client => ({
	run: tx.run,
	transaction: tx.transaction,
	bulk: (statement, input, { atomic: _atomic, retry: _retry, ...options }) =>
		tx.bulk(statement, input, options),
	pipeline: () => Effect.fail(scopeError('pipeline')),
	withTransaction: () => Effect.fail(scopeError(name)),
	stream: () => Stream.fail(scopeError('stream')),
	live: () => Stream.fail(scopeError('live')),
	liveScoped: () => Effect.fail(scopeError('liveScoped')),
});

export const withTransactionContext = <A, E, R>(
	client: Client,
	effect: Effect.Effect<A, E, R>,
	options: TransactionOptions | undefined,
): Effect.Effect<A, E | TransactionError, R> =>
	client.withTransaction(
		(tx) => Effect.provideService(effect, SurrealClient, contextClient(tx)),
		options,
	);
