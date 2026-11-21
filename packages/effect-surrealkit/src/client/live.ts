import { Effect, Queue, Schema, Stream } from 'effect';
import {
	ConfigurationError,
	UnavailableFeatureError,
	UnsupportedFeatureError,
} from 'surrealdb';

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
import { messageOf } from './internal/classify.js';
import { checkValues, encode } from './internal/encode.js';
import { requestPart } from './internal/join.js';
import { recording } from './internal/trace.js';
import type { LiveHandle, Transport } from './internal/transport.js';

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
		message:
			cause === undefined
				? 'the connection cannot listen for live queries'
				: messageOf(cause),
		...(cause !== undefined && { cause }),
	});

const requestError = (statement: string, cause: unknown) =>
	cause instanceof UnsupportedFeatureError ||
	cause instanceof UnavailableFeatureError ||
	(cause instanceof ConfigurationError && cause.isLiveQueryNotSupported)
		? unsupported(statement, cause)
		: new StatementRequestError({
				statement,
				message: messageOf(cause),
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
					message: `live event ${field}: ${error.message}`,
				}),
		),
	);

/** Drain the attached SDK channel immediately. Its own queue discards pending
 * messages when the engine sends KILLED or disconnects. */
const changes = (statement: string, handle: LiveHandle) =>
	Effect.gen(function* () {
		const disconnected = () => closed(statement, 'disconnected');
		const iterator = yield* Effect.try({
			try: () => handle.messages[Symbol.asyncIterator](),
			catch: disconnected,
		});
		const queue = yield* Queue.unbounded<Change, LiveQueryClosed>();
		yield* Effect.addFinalizer(() => Queue.shutdown(queue));
		const poll = Effect.gen(function* () {
			const frame = yield* Effect.tryPromise({
				try: () => iterator.next(),
				catch: disconnected,
			});
			if (frame.done) return yield* Effect.fail(disconnected());
			const { action, id, value } = frame.value;
			if (action === 'KILLED')
				return yield* Effect.fail(closed(statement, 'killed'));
			yield* Queue.offer(queue, { action, id, value });
		});
		yield* Effect.forkScoped(
			Effect.forever(poll).pipe(
				Effect.catch((error) => Queue.fail(queue, error)),
			),
			{ startImmediately: true },
		);
		return Stream.fromQueue(queue);
	});

const register = (
	open: NonNullable<Transport['live']>,
	descriptor: AnyStatement,
	input: Readonly<Record<string, unknown>>,
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
						),
					catch: (cause) => requestError(name, cause),
				}),
			);
		}),
	).pipe(Effect.withSpan(operation, { kind: 'client' }));
};

const kill = (name: string, handle: LiveHandle) => {
	const operation = `kill(${name})`;
	return recording('run', operation, (recorder) =>
		recorder.wait(Effect.tryPromise(() => handle.kill())),
	).pipe(Effect.withSpan(operation, { kind: 'client' }), Effect.ignore);
};

const scoped =
	(transport: Transport): Client['liveScoped'] =>
	<P extends Params, Row extends Codec, Id extends Codec, I>(
		statement: LiveStatement<P, Row, Id>,
		...args: InputOf<P, I>
	) =>
		Effect.gen(function* () {
			const open = transport.live;
			if (open === undefined)
				return yield* Effect.fail(unsupported(statement.name));
			const { name, sql, params } = statement;
			const descriptor = make({
				name,
				sql,
				params,
				result: Schema.Unknown,
				writes: false,
			});
			const input = (args[0] ?? {}) as Readonly<Record<string, unknown>>;
			const handle = yield* Effect.acquireRelease(
				register(open, descriptor, input),
				(subscribed) => kill(name, subscribed),
			);
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
