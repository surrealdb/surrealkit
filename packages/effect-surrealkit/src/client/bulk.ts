import {
	Cause,
	Duration,
	Effect,
	Pull,
	type Schedule,
	type Scope,
	Stream,
} from 'effect';
import {
	CallTerminatedError,
	HttpConnectionError,
	UnexpectedConnectionError,
	UnexpectedServerResponseError,
} from 'surrealdb';

import type { ElementOf } from '../statement/expr.js';
import type {
	AnyStatement,
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import type { Client } from './client.js';
import {
	BulkWriteError,
	StatementConflict,
	type StatementError,
	StatementScopeError,
	StatementUnsupported,
	TransactionOutcomeUnknown,
} from './errors.js';
import { makeWithTransaction, type TransactionClient } from './interactive.js';
import { classify, declarer, firstFailure } from './internal/classify.js';
import {
	changesOf,
	countOf,
	decode,
	decodeOutcome,
} from './internal/decode.js';
import { checkValues, encode } from './internal/encode.js';
import { joinWith } from './internal/join.js';
import { recording } from './internal/trace.js';
import {
	type SlotAnswer,
	sending,
	type TransactionHandle,
	type Transport,
} from './internal/transport.js';

/** The names of the bindings whose decoded values are lists. */
export type ListBindingsOf<P extends Params> = {
	[K in keyof P]-?: [P[K]['Type']] extends [never]
		? never
		: [P[K]['Type']] extends [ReadonlyArray<unknown>]
			? K
			: never;
}[keyof P] &
	string;

export type BulkInput<
	P extends Params,
	K extends ListBindingsOf<P>,
	E,
	R,
> = Omit<Input<P>, K> & {
	readonly [Key in K]:
		| Iterable<ElementOf<P[Key]['Type']>>
		| Stream.Stream<ElementOf<P[Key]['Type']>, E, R>;
};

export interface BulkProgress {
	/** Chunks finished so far. */
	readonly chunks: number;
	/** Rows finished so far. */
	readonly rows: number;
	/** Total row count when the collection exposes its size. */
	readonly total: number | undefined;
}

export interface BulkOptions<K extends string, Row, E = never, R = never> {
	/** The list binding that holds the rows. */
	readonly over: K;
	/** Rows per request. A positive integer; defaults to 500. */
	readonly chunk?: number;
	/** Commit all chunks together (default) or each chunk separately. */
	readonly atomic?: boolean;
	/** Called with the original rows before sending each chunk. Its failure stops the write. */
	readonly beforeChunk?: (chunk: {
		readonly index: number;
		readonly rows: ReadonlyArray<Row>;
	}) => Effect.Effect<void, E, R>;
	/** Called once each chunk is acknowledged. */
	readonly onProgress?: (
		progress: BulkProgress,
	) => Effect.Effect<void, never, R>;
	/** Retry the whole atomic write or the conflicting non-atomic chunk. */
	readonly retry?: Schedule.Schedule<unknown, StatementConflict>;
	readonly unsafeSkipValueCheck?: boolean;
}

export interface BulkSummary<A> {
	readonly rows: number;
	readonly chunks: number;
	/** The decoded result of each chunk, in order. */
	readonly results: ReadonlyArray<A>;
}

type RootSender = {
	readonly transport: Pick<Transport, 'begin' | 'collect' | 'send'>;
};
type ScopedSender = { readonly transaction: Pick<TransactionHandle, 'send'> };
type BulkSender = RootSender | ScopedSender;
type BulkFailure<E> =
	| StatementError<string>
	| BulkWriteError<string>
	| StatementUnsupported
	| TransactionOutcomeUnknown
	| E;

type Rows<Row> = readonly [Row, ...ReadonlyArray<Row>];
type Next<Row, E> = Effect.Effect<Rows<Row> | undefined, E>;

interface Job<Row, E, R> {
	readonly statement: AnyStatement;
	readonly input: Readonly<Record<string, unknown>>;
	readonly options: BulkOptions<string, Row, E, R>;
	readonly total: number | undefined;
}

/** User and producer failures must not borrow the database's conflict retry policy. */
class SourceFailure<E> {
	constructor(readonly error: E) {}
}

const streamChunks = <Row, E, R>(
	source: Stream.Stream<Row, E, R>,
	size: number,
): Effect.Effect<Next<Row, E>, never, R | Scope.Scope> =>
	Effect.map(Stream.toPull(Stream.grouped(source, size)), (pull) =>
		pull.pipe(
			Effect.map(([chunk]) => chunk),
			Pull.catchDone(() => Effect.succeed(undefined)),
		),
	);

const iterableChunks = <Row>(
	source: Iterable<Row>,
	size: number,
): Effect.Effect<Next<Row, never>, never, Scope.Scope> =>
	Effect.gen(function* () {
		let done = false;
		const iterator = yield* Effect.acquireRelease(
			Effect.sync(() => source[Symbol.iterator]()),
			(cursor) =>
				Effect.sync(() => {
					if (!done) cursor.return?.();
				}),
		);
		return Effect.sync(() => {
			if (done) return undefined;
			const head = iterator.next();
			if (head.done) {
				done = true;
				return undefined;
			}
			const chunk: [Row, ...Array<Row>] = [head.value];
			while (chunk.length < size) {
				const row = iterator.next();
				if (row.done) {
					done = true;
					break;
				}
				chunk.push(row.value);
			}
			return chunk;
		});
	});

const totalOf = (
	rows: Iterable<unknown> | Stream.Stream<unknown, unknown, unknown>,
): number | undefined =>
	Array.isArray(rows)
		? rows.length
		: rows instanceof Set || rows instanceof Map
			? rows.size
			: undefined;

const inputWith = (
	input: Readonly<Record<string, unknown>>,
	key: string,
	rows: unknown,
): Readonly<Record<string, unknown>> => ({ ...input, [key]: rows });

const uncertain = (cause: unknown): boolean =>
	cause instanceof CallTerminatedError ||
	cause instanceof UnexpectedConnectionError ||
	cause instanceof UnexpectedServerResponseError ||
	(cause instanceof HttpConnectionError && cause.status >= 500) ||
	(cause instanceof Error &&
		(cause.name === 'TimeoutError' || cause.name === 'AbortError')) ||
	// Fetch and response-body failures escape the SDK as host TypeErrors.
	cause instanceof TypeError;

const failureOf = (
	statement: AnyStatement,
	atomic: boolean,
	cause: unknown,
) => {
	if (
		cause instanceof StatementScopeError ||
		cause instanceof StatementConflict
	)
		return cause;
	if (atomic && uncertain(cause)) {
		return new TransactionOutcomeUnknown({
			message:
				'The bulk transaction was not acknowledged. Read back the data before retrying.',
			cause,
		});
	}
	return classify(
		statement.name,
		(reason) => declarer([statement], reason),
		cause,
	);
};

const serverTime = (
	answers: ReadonlyArray<SlotAnswer>,
): Duration.Duration | undefined => {
	let server: Duration.Duration | undefined;
	for (const slot of answers) {
		if (slot.server !== undefined)
			server =
				server === undefined
					? slot.server
					: Duration.sum(server, slot.server);
	}
	return server;
};

const submit = <Row, E, R>(
	sender: BulkSender,
	job: Job<Row, E, R>,
	chunks: ReadonlyArray<Rows<Row>>,
	atomic: boolean,
): Effect.Effect<
	ReadonlyArray<unknown>,
	StatementError<string> | TransactionOutcomeUnknown
> => {
	const { statement, input, options } = job;
	const scoped = 'transaction' in sender;
	const label = `bulk(${statement.name})`;
	const writes = scoped ? 'none' : changesOf([statement]);
	return recording('bulk', label, (recorder) =>
		Effect.gen(function* () {
			const request = yield* recorder.encode(
				Effect.gen(function* () {
					const request = yield* joinWith(
						chunks.map((rows) => ({
							statement,
							bindings: encode(
								statement,
								inputWith(input, options.over, rows),
							),
						})),
						{
							mode: atomic ? 'atomic' : 'query',
							refuseTransactional: true,
						},
					);
					for (const part of request.parts) {
						yield* checkValues(
							part.bindings,
							() => statement.name,
							options,
						);
					}
					return { ...request, sdkRetry: false } as const;
				}),
			);
			const sendFailure = (cause: unknown) =>
				failureOf(statement, atomic, cause);
			let raw: unknown;
			let answers: ReadonlyArray<SlotAnswer> | undefined;
			if (scoped) {
				answers = yield* recorder.wait(
					Effect.tryPromise({
						try: (signal) =>
							sender.transaction.send(request, signal),
						catch: sendFailure,
					}),
				);
				const failure = firstFailure(answers);
				if (failure !== undefined) {
					yield* recorder.server(statement.name, serverTime(answers));
					return yield* Effect.fail(sendFailure(failure.error));
				}
				raw = answers.map((slot) => (slot.ok ? slot.value : undefined));
			} else {
				raw = yield* sending(
					recorder.wait(
						Effect.tryPromise({
							try: (signal) =>
								sender.transport.collect(request, signal),
							catch: sendFailure,
						}),
					),
					options,
				);
			}
			return yield* recorder.decode(
				Effect.gen(function* () {
					const slots = yield* countOf(
						label,
						raw,
						request.parts.reduce(
							(total, part) => total + part.slots,
							0,
						),
						writes,
					);
					const decoded: Array<unknown> = [];
					for (const { offset, slots: size } of request.parts) {
						yield* recorder.server(
							statement.name,
							answers === undefined
								? undefined
								: serverTime(
										answers.slice(offset, offset + size),
									),
						);
						const value = yield* decode(
							statement,
							slots,
							offset,
							writes,
						);
						if (Array.isArray(value))
							yield* recorder.rows(statement.name, value.length);
						decoded.push(value);
					}
					return decoded;
				}).pipe(Effect.mapError(decodeOutcome(label, writes))),
			);
		}),
	).pipe(
		Effect.withSpan(label, {
			kind: 'client',
			attributes: { 'db.system.name': 'surrealdb' },
		}),
	);
};

const sequential = <Row, E, R>(
	sender: BulkSender,
	job: Job<Row, E, R>,
	first: Rows<Row>,
	next: Next<Row, E>,
) =>
	Effect.gen(function* () {
		const { statement, options, total } = job;
		// A later slot can fail after an earlier slot has written. Commit the
		// entire chunk together before allowing a root request to be replayed.
		const atomic = !('transaction' in sender) && statement.slots > 1;
		const results: Array<unknown> = [];
		let rows = 0;
		for (
			let chunk: Rows<Row> | undefined = first;
			chunk !== undefined;
			chunk = yield* next
		) {
			const index = results.length;
			yield* options.beforeChunk?.({ index, rows: chunk }) ?? Effect.void;
			const [result] = yield* submit(sender, job, [chunk], atomic).pipe(
				Effect.mapError((cause) =>
					'transaction' in sender ||
					cause instanceof TransactionOutcomeUnknown
						? cause
						: new BulkWriteError({
								statement: statement.name,
								chunk: index,
								chunksCommitted: index,
								rowsCommitted: rows,
								cause,
							}),
				),
			);
			results.push(result);
			rows += chunk.length;
			yield* options.onProgress?.({
				chunks: results.length,
				rows,
				total,
			}) ?? Effect.void;
		}
		return { chunks: results.length, rows, results };
	});

const batched = <Row, E, R>(
	sender: RootSender,
	job: Job<Row, E, R>,
	first: Rows<Row>,
	next: Next<Row, E>,
) =>
	Effect.gen(function* () {
		const { options, total } = job;
		const chunks: Array<Rows<Row>> = [];
		for (
			let chunk: Rows<Row> | undefined = first;
			chunk !== undefined;
			chunk = yield* next
		) {
			yield* options.beforeChunk?.({
				index: chunks.length,
				rows: chunk,
			}) ?? Effect.void;
			chunks.push(chunk);
		}
		const results = yield* submit(sender, job, chunks, true);
		let rows = 0;
		for (const [index, chunk] of chunks.entries()) {
			rows += chunk.length;
			yield* options.onProgress?.({ chunks: index + 1, rows, total }) ??
				Effect.void;
		}
		return { chunks: chunks.length, rows, results };
	});

const fromOneShot = <Row, E>(
	first: Rows<Row>,
	next: Next<Row, E>,
	replay: boolean,
): (() => Stream.Stream<Row, SourceFailure<E>>) => {
	const history = [first];
	let finished = false;
	return () => {
		let index = 0;
		const pull = Effect.suspend(() => {
			const cached = history[index++];
			if (cached !== undefined) return Effect.succeed(cached);
			if (finished) return Cause.done();
			return next.pipe(
				Effect.mapError((error) => new SourceFailure(error)),
				Effect.flatMap((chunk) => {
					if (chunk === undefined) {
						finished = true;
						return Cause.done();
					}
					if (replay) history.push(chunk);
					return Effect.succeed(chunk);
				}),
			);
		});
		return Stream.fromPull(Effect.succeed(pull));
	};
};

export function makeBulk(sender: ScopedSender): TransactionClient['bulk'];
export function makeBulk(sender: RootSender): Client['bulk'];
export function makeBulk(sender: BulkSender): Client['bulk'];
export function makeBulk(sender: BulkSender): Client['bulk'] {
	return <
		P extends Params,
		R extends Codec,
		T extends string,
		K extends ListBindingsOf<P>,
		E = never,
		C = never,
	>(
		statement: Statement<P, R, T>,
		input: BulkInput<P, K, E, C>,
		options: BulkOptions<K, ElementOf<P[K]['Type']>, E, C>,
	) => {
		type Row = ElementOf<P[K]['Type']>;
		const size = options.chunk ?? 500;
		if (!Number.isInteger(size) || size <= 0) {
			return Effect.die(
				new RangeError('bulk chunk size must be a positive integer'),
			);
		}
		if (statement.transactional) {
			return Effect.fail(
				new StatementScopeError({
					statement: statement.name,
					message:
						'bulk cannot run a statement with its own BEGIN, COMMIT or CANCEL',
				}),
			);
		}
		return Effect.scoped(
			Effect.gen(function* () {
				const source = input[options.over];
				const next: Next<Row, E> = Stream.isStream(source)
					? yield* streamChunks(
							source as Stream.Stream<Row, E, C>,
							size,
						)
					: yield* iterableChunks(source, size);
				const first = yield* next;
				if (first === undefined)
					return { chunks: 0, rows: 0, results: [] };
				const total = totalOf(source);
				const job = { statement, input, options, total };
				if ('transaction' in sender || options.atomic === false) {
					return yield* sequential(sender, job, first, next);
				}
				if (sender.transport.begin === undefined) {
					return yield* batched(sender, job, first, next);
				}
				const replay = fromOneShot(
					first,
					next,
					options.retry !== undefined,
				);
				let bodyStarted = false;
				const before = options.beforeChunk;
				const progress = options.onProgress;
				const scoped = makeWithTransaction(sender.transport)(
					(tx) => {
						bodyStarted = true;
						return tx.bulk<P, R, T, K, SourceFailure<E>, C>(
							statement,
							inputWith(
								input,
								options.over,
								replay(),
							) as BulkInput<P, K, SourceFailure<E>, C>,
							{
								over: options.over,
								chunk: size,
								...(options.unsafeSkipValueCheck !==
									undefined && {
									unsafeSkipValueCheck:
										options.unsafeSkipValueCheck,
								}),
								...(before !== undefined && {
									beforeChunk: (chunk) =>
										before(chunk).pipe(
											Effect.mapError(
												(error) =>
													new SourceFailure(error),
											),
										),
								}),
								...(progress !== undefined && {
									onProgress: (value) =>
										progress({ ...value, total }),
								}),
							},
						);
					},
					options.retry === undefined
						? undefined
						: { retry: options.retry },
				);
				return yield* scoped.pipe(
					Effect.catch(
						(
							error,
						): Effect.Effect<
							BulkSummary<unknown>,
							BulkFailure<E>,
							C
						> => {
							if (error instanceof SourceFailure)
								return Effect.fail(error.error);
							if (
								!bodyStarted &&
								error instanceof StatementUnsupported
							)
								return batched(sender, job, first, next);
							return Effect.fail(error);
						},
					),
				);
			}),
		) as Effect.Effect<
			BulkSummary<R['Type']>,
			| StatementError<T>
			| BulkWriteError<T>
			| StatementUnsupported
			| TransactionOutcomeUnknown
			| E,
			C
		>;
	};
}
