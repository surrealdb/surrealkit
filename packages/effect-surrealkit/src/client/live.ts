import { Effect, Queue, Schema, Stream } from 'effect';
import {
	ConfigurationError,
	UnavailableFeatureError,
	UnsupportedFeatureError,
} from 'surrealdb';

import { codecIssueText } from '../internal/codecIssue.js';
import type { LiveStatement } from '../statement/live.js';
import {
	type AnyStatement,
	type Codec,
	make,
	type Params,
} from '../statement/statement.js';
import type { Client, InputOf } from './client.js';
import {
	LiveQueryClosed,
	StatementDecodeError,
	type StatementEncodeError,
	StatementRequestError,
	type StatementScopeError,
	StatementUnsupported,
} from './errors.js';
import { requestErrorText } from './internal/classify.js';
import { checkValues, encode } from './internal/encode.js';
import { requestPart } from './internal/join.js';
import { withClientSpan } from './internal/span.js';
import { recording } from './internal/trace.js';
import type { LiveHandle, Signal, Transport } from './internal/transport.js';

export interface LiveEvent<Row, Id> {
	readonly action: 'CREATE' | 'UPDATE' | 'DELETE';
	readonly id: Id;
	/** The record after the change, or the deleted record for DELETE. */
	readonly value: Row;
}

export type LiveError =
	| StatementEncodeError
	| StatementRequestError
	| StatementScopeError
	| StatementDecodeError
	| StatementUnsupported
	| LiveQueryClosed;

const unsupported = (statement: string, cause?: unknown) =>
	new StatementUnsupported({
		feature: 'live query',
		statement,
		message: 'the connection cannot listen for live queries',
		...(cause !== undefined && { cause }),
	});

const requestError = (statement: string, cause: unknown) =>
	cause instanceof UnsupportedFeatureError ||
	cause instanceof UnavailableFeatureError ||
	(cause instanceof ConfigurationError && cause.isLiveQueryNotSupported)
		? unsupported(statement, cause)
		: new StatementRequestError({
				statement,
				message: requestErrorText(cause),
				cause,
			});

const closed = (statement: string, reason: LiveQueryClosed['reason']) =>
	new LiveQueryClosed({
		statement,
		reason,
		message:
			reason === 'killed'
				? `the engine killed live query \`${statement}\``
				: `the connection for live query \`${statement}\` closed`,
	});

type Message =
	LiveHandle['messages'] extends AsyncIterable<infer M> ? M : never;
type Change = Message & {
	readonly action: LiveEvent<unknown, unknown>['action'];
};

const liveIterator = <E>(handle: LiveHandle, onError: (cause: unknown) => E) =>
	Effect.try({
		try: () => handle.messages[Symbol.asyncIterator](),
		catch: onError,
	});

const consumeFrames = <E>(
	statement: string,
	iterator: AsyncIterator<Message>,
	consume: (frame: Change) => Effect.Effect<unknown>,
	onError: (cause: unknown) => E,
) =>
	Effect.forever(
		Effect.gen(function* () {
			const frame = yield* Effect.tryPromise({
				try: () => iterator.next(),
				catch: onError,
			});
			if (frame.done)
				return yield* Effect.fail(closed(statement, 'disconnected'));
			const { action, id, value } = frame.value;
			if (action === 'KILLED')
				return yield* Effect.fail(closed(statement, 'killed'));
			yield* consume({ action, id, value });
		}),
	);

/** Start pulling the attached SDK channel immediately, independently of consumers. */
export const consumeLive = <E>(
	statement: string,
	handle: LiveHandle,
	consume: (frame: Change) => Effect.Effect<unknown>,
	onError: (cause: unknown) => E,
) =>
	liveIterator(handle, onError).pipe(
		Effect.flatMap((iterator) =>
			consumeFrames(statement, iterator, consume, onError),
		),
	);

const decodeField = <C extends Codec>(
	statement: string,
	field: 'id' | 'value',
	codec: C,
	value: unknown,
): Effect.Effect<C['Type'], StatementDecodeError> =>
	Schema.decodeUnknownEffect(codec)(value, {
		onExcessProperty: 'error',
	}).pipe(
		Effect.mapError(
			(error) =>
				new StatementDecodeError({
					statement,
					slot: 1,
					committed: false,
					message: `live event ${field}: ${codecIssueText(error.issue, codec.ast)}`,
				}),
		),
	);

/** Drain the attached SDK channel immediately. Its own queue discards pending
 * messages when the engine sends KILLED or disconnects. */
const changes = (statement: string, handle: LiveHandle) =>
	Effect.gen(function* () {
		const disconnected = () => closed(statement, 'disconnected');
		const iterator = yield* liveIterator(handle, disconnected);
		const queue = yield* Queue.unbounded<Change, LiveQueryClosed>();
		yield* Effect.addFinalizer(() => Queue.shutdown(queue));
		yield* Effect.forkScoped(
			consumeFrames(
				statement,
				iterator,
				(frame) => Queue.offer(queue, frame),
				disconnected,
			).pipe(Effect.catch((error) => Queue.fail(queue, error))),
			{ startImmediately: true },
		);
		return Stream.fromQueue(queue);
	});

const register = (
	open: NonNullable<Transport['live']>,
	descriptor: AnyStatement,
	input: Readonly<Record<string, unknown>>,
	connectionSignal?: Signal,
) => {
	const { name, sql } = descriptor;
	const operation = `live(${name})`;
	return recording('run', operation, (recorder) =>
		Effect.gen(function* () {
			// Encoding can be interrupted. Once LIVE SELECT may have run, keep
			// awaiting its ID so the scope can remove a late registration.
			const bindings = yield* Effect.interruptible(
				recorder.encode(
					encode(descriptor, input).pipe(
						Effect.tap((encoded) =>
							checkValues(encoded, () => name, undefined),
						),
					),
				),
			);
			yield* recorder.server(name, undefined);
			return yield* recorder.wait(
				Effect.tryPromise({
					try: (signal) =>
						open(
							{
								mode: 'query',
								sql,
								bindings,
								parts: [
									requestPart(descriptor, bindings, 0, 1),
								],
							},
							signal,
							connectionSignal,
						),
					catch: (cause) => requestError(name, cause),
				}),
			);
		}),
	).pipe(withClientSpan(operation, { kind: 'client' }));
};

export const acquireLive = (
	transport: Transport,
	statement: LiveStatement,
	input: Readonly<Record<string, unknown>>,
	connectionSignal?: Signal,
) => {
	const open = transport.live;
	const { name, sql, params } = statement;
	if (open === undefined) return Effect.fail(unsupported(name));
	const descriptor = make({
		name,
		sql,
		params,
		result: Schema.Unknown,
		writes: false,
	});
	return Effect.acquireRelease(
		register(open, descriptor, input, connectionSignal),
		(subscribed) => kill(name, subscribed),
	);
};

const kill = (name: string, handle: LiveHandle) => {
	const operation = `kill(${name})`;
	return recording('run', operation, (recorder) =>
		recorder
			.wait(Effect.tryPromise(() => handle.kill()))
			.pipe(
				Effect.catch(() =>
					Effect.logWarning(
						'live query cleanup could not be confirmed',
					).pipe(Effect.annotateLogs({ statement: name })),
				),
			),
	).pipe(withClientSpan(operation, { kind: 'client' }));
};

const scoped =
	(transport: Transport): Client['liveScoped'] =>
	<P extends Params, Row extends Codec, Id extends Codec, I>(
		statement: LiveStatement<P, Row, Id>,
		...args: InputOf<P, I>
	) =>
		Effect.gen(function* () {
			const { name } = statement;
			const input = (args[0] ?? {}) as Readonly<Record<string, unknown>>;
			const handle = yield* acquireLive(transport, statement, input);
			const messages = yield* changes(name, handle);
			return messages.pipe(
				Stream.mapEffect(({ action, id, value }) =>
					Effect.gen(function* () {
						const recordId = yield* decodeField(
							name,
							'id',
							statement.id,
							id,
						);
						const row = yield* decodeField(
							name,
							'value',
							statement.row,
							value,
						);
						return { action, id: recordId, value: row };
					}),
				),
			);
		});

export const makeLive = (
	transport: Transport,
): Pick<Client, 'live' | 'liveScoped'> => {
	const liveScoped = scoped(transport);
	return {
		liveScoped,
		live: <P extends Params, Row extends Codec, Id extends Codec, I>(
			statement: LiveStatement<P, Row, Id>,
			...args: InputOf<P, I>
		) => Stream.unwrap(liveScoped<P, Row, Id, I>(statement, ...args)),
	};
};
