import { Cause, Deferred, Duration, Effect, Queue, Stream } from 'effect';

import type { LiveStatement } from '../statement/live.js';
import type {
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import type { Client, InputOf } from './client.js';
import {
	type StatementError,
	StatementRequestError,
	StatementScopeError,
	StatementUnsupported,
	WatchQueryClosed,
} from './errors.js';
import {
	causedBy,
	isAuthenticationFailure,
	isClosedSession,
	requestErrorText,
} from './internal/classify.js';
import type {
	Signal,
	Transport,
	WatchConnectionEvent,
} from './internal/transport.js';
import { acquireLive, consumeLive, type LiveError } from './live.js';

declare const BoundLiveTypeId: unique symbol;

/** A typed live statement and its bindings, paired by {@link bindLive}. */
export interface BoundLive {
	readonly [BoundLiveTypeId]: 'made by bindLive';
	readonly statement: LiveStatement;
	readonly input: Readonly<Record<string, unknown>>;
}

/** Pair a live statement with the values of its bindings. */
export const bindLive = <
	P extends Params,
	Row extends Codec,
	Id extends Codec,
	I = Input<P>,
>(
	statement: LiveStatement<P, Row, Id>,
	...input: InputOf<P, I>
): BoundLive => ({ statement, input: input[0] ?? {} }) as unknown as BoundLive;

export interface WatchQueryOptions {
	/** Explicit notification sources covering changes to the query's result. */
	readonly sources: readonly [BoundLive, ...BoundLive[]];
	/** Re-read even without a notification. Positive and finite, default 30 seconds. */
	readonly reconcileEvery?: Duration.Input;
}

export type WatchQueryState<A> =
	| { readonly _tag: 'Loading' }
	| { readonly _tag: 'Stale' }
	| { readonly _tag: 'Current'; readonly value: A };

export type WatchQueryError<
	T extends string = never,
	U extends string = never,
> = StatementError<T, U> | LiveError | WatchQueryClosed;

export type WatchQuery = <
	P extends Params,
	R extends Codec,
	T extends string,
	U extends string = never,
	I = Input<P>,
>(
	statement: Statement<P, R, T, U>,
	options: WatchQueryOptions,
	...input: InputOf<P, I>
) => Stream.Stream<WatchQueryState<R['Type']>, WatchQueryError<T, U>>;

const closed = (statement: string, reason: WatchQueryClosed['reason']) =>
	new WatchQueryClosed({
		statement,
		reason,
		message: {
			disconnected: 'the observer connection closed',
			'session-closed': 'the observer session closed',
			'identity-changed':
				'the observer session credentials or database selection changed',
			authentication:
				'the observer session authentication failed or was invalidated',
			'source-overflow': 'a live source reported notification overflow',
		}[reason],
	});

const intervalOf = (input: Duration.Input) => {
	const interval = Duration.fromInput(input);
	if (interval._tag === 'None') return;
	const millis = Duration.toMillis(interval.value);
	if (Number.isFinite(millis) && millis > 0) return interval.value;
};

const watchFailure = <E>(name: string, error: E): E | WatchQueryClosed => {
	if (isAuthenticationFailure(error)) return closed(name, 'authentication');
	if (isClosedSession(error)) return closed(name, 'session-closed');
	if (causedBy(error, (cause) => cause.name === 'LiveQueryOverflowError'))
		return closed(name, 'source-overflow');
	return error;
};

const sessionIsValid = (transport: Transport) =>
	transport.isWatchSessionValid?.() !== false;

const notificationFailure = (name: string, cause: unknown) => {
	const error = watchFailure(name, cause);
	return error instanceof WatchQueryClosed
		? error
		: closed(name, 'disconnected');
};

interface ConnectionController {
	readonly signal: Signal;
	abort(): void;
}

// Use the runtime's controller without requiring DOM declarations from users.
const newConnectionController = () =>
	new (
		globalThis as typeof globalThis & {
			readonly AbortController: new () => ConnectionController;
		}
	).AbortController();

interface Generation {
	/** Stop refreshes and retire registrations, even if the next connection is already ready. */
	readonly ended: Deferred.Deferred<void>;
	/** Only abort a pending registration when its owning connection has gone. */
	readonly connection: ConnectionController;
}

type QueuedState<A> =
	| { readonly _tag: 'Stale' }
	| {
			readonly _tag: 'Current';
			readonly value: A;
			readonly generation: Generation;
	  };

const maintain = <A, E>(
	name: string,
	transport: Transport,
	sources: ReadonlyArray<BoundLive>,
	interval: Duration.Duration,
	read: Effect.Effect<A, E>,
	generation: Generation,
	publish: (value: A) => void,
	onFailure: (error: E | LiveError | WatchQueryClosed) => void,
) =>
	Effect.scoped(
		Effect.gen(function* () {
			// One shared wakeup for every source and the timer, including while a
			// request is in flight. Payloads never enter an observer-side queue.
			const dirty = yield* Queue.dropping<void>(1);
			const failed = yield* Deferred.make<
				never,
				LiveError | WatchQueryClosed
			>();
			const markDirty = () => Queue.offerUnsafe(dirty, undefined);
			const invalidate = Effect.sync(markDirty);
			yield* Effect.forkScoped(
				Effect.forever(
					Effect.sleep(interval).pipe(
						Effect.andThen(
							Effect.sync(() => {
								// A child can close without emitting an SDK event, even while a read is blocked.
								if (sessionIsValid(transport)) markDirty();
								else onFailure(closed(name, 'session-closed'));
							}),
						),
					),
				),
				{ startImmediately: true },
			);
			return yield* Effect.raceFirst(
				Deferred.await(failed),
				Effect.gen(function* () {
					const verify = transport.verifyWatchOwner;
					if (verify !== undefined) {
						const verified = yield* Effect.tryPromise({
							try: verify,
							catch: (cause) =>
								new StatementRequestError({
									statement: name,
									message: requestErrorText(cause),
									cause,
								}),
						});
						if (!verified)
							return yield* Effect.fail(
								new StatementScopeError({
									statement: name,
									message:
										'watchQuery requires the connection that owns the session',
								}),
							);
					}
					for (const { statement, input } of sources) {
						const handle = yield* acquireLive(
							transport,
							statement,
							input,
							generation.connection.signal,
						);
						yield* Effect.forkScoped(
							consumeLive(
								statement.name,
								handle,
								() => invalidate,
								(cause) =>
									notificationFailure(statement.name, cause),
							).pipe(
								Effect.catch((error) =>
									Deferred.fail(failed, error),
								),
							),
							{ startImmediately: true },
						);
					}
					// Notifications from acquisition are covered by the first read.
					yield* Queue.poll(dirty);
					yield* Effect.forever(
						Effect.suspend<A, E | WatchQueryClosed, never>(() =>
							sessionIsValid(transport)
								? read
								: Effect.fail(closed(name, 'session-closed')),
						).pipe(
							Effect.tap((value) =>
								Effect.sync(() => publish(value)),
							),
							Effect.andThen(Queue.take(dirty)),
						),
					);
				}),
			).pipe(
				Effect.mapError((error) => watchFailure(name, error)),
				Effect.tapError((error) => Effect.sync(() => onFailure(error))),
			);
		}),
	);

const observer = <A, E>(
	name: string,
	transport: Transport,
	watch: NonNullable<Transport['watch']>,
	sources: ReadonlyArray<BoundLive>,
	interval: Duration.Duration,
	read: Effect.Effect<A, E>,
) =>
	Effect.gen(function* () {
		const output = yield* Queue.sliding<
			QueuedState<A>,
			E | LiveError | WatchQueryClosed
		>(1);
		const lifecycle = yield* Queue.dropping<void>(1);
		let generation: Generation | undefined;
		let supervised: Generation | undefined;
		let status: WatchConnectionEvent | undefined;
		let currentPublished = false;
		let stale = false;
		let finished = false;

		const publishStale = () => {
			if (stale) return;
			stale = true;
			Queue.offerUnsafe(output, { _tag: 'Stale' });
		};
		const fail = (
			cause: Cause.Cause<E | LiveError | WatchQueryClosed>,
			wasConnected = false,
		) => {
			if (finished) return;
			finished = true;
			// Replace any queued snapshot with Stale before surfacing the error.
			if (currentPublished || wasConnected) publishStale();
			Queue.failCauseUnsafe(output, cause);
		};
		const notify = (event: WatchConnectionEvent) => {
			if (
				event === 'reconnecting' ||
				event === 'disconnected' ||
				event === 'session-closed'
			) {
				generation?.connection.abort();
				supervised?.connection.abort();
			}
			if (finished) return;
			if (event === status) return;
			status = event;
			if (event === 'connected') {
				generation = {
					ended: Deferred.makeUnsafe(),
					connection: newConnectionController(),
				};
			} else {
				const previous = generation;
				generation = undefined;
				if (previous !== undefined)
					Deferred.doneUnsafe(previous.ended, Effect.void);
				if (event === 'reconnecting') publishStale();
				else
					fail(
						Cause.fail(closed(name, event)),
						previous !== undefined,
					);
			}
			Queue.offerUnsafe(lifecycle, undefined);
		};

		yield* Effect.acquireRelease(
			Effect.try({
				try: () => watch(notify),
				catch: () => closed(name, 'disconnected'),
			}),
			(stop) => Effect.sync(stop),
		);
		yield* Effect.forkScoped(
			Effect.forever(
				Effect.gen(function* () {
					const active = generation;
					if (active === undefined)
						return yield* Queue.take(lifecycle);
					supervised = active;
					yield* Effect.raceFirst(
						Deferred.await(active.ended),
						maintain(
							name,
							transport,
							sources,
							interval,
							read,
							active,
							(value) => {
								if (generation !== active || finished) return;
								if (!sessionIsValid(transport)) {
									notify('session-closed');
									return;
								}
								currentPublished = true;
								stale = false;
								Queue.offerUnsafe(output, {
									_tag: 'Current',
									value,
									generation: active,
								});
							},
							(error) => {
								if (generation !== active) return;
								if (
									error instanceof WatchQueryClosed &&
									error.reason === 'session-closed'
								)
									notify('session-closed');
								else fail(Cause.fail(error));
							},
						),
					).pipe(
						Effect.ensuring(
							Effect.sync(() => {
								supervised = undefined;
							}),
						),
						Effect.catchCause((cause) =>
							generation === active
								? Effect.failCause(cause)
								: Effect.void,
						),
					);
				}),
			).pipe(
				Effect.catchCause((cause) => Effect.sync(() => fail(cause))),
			),
			{ startImmediately: true },
		);
		const emit = (
			state: QueuedState<A>,
		): Effect.Effect<
			WatchQueryState<A>,
			E | LiveError | WatchQueryClosed
		> =>
			Effect.suspend(() => {
				if (state._tag === 'Stale') return Effect.succeed(state);
				if (state.generation === generation && !finished) {
					if (sessionIsValid(transport))
						return Effect.succeed({
							_tag: 'Current',
							value: state.value,
						});
					notify('session-closed');
				}
				return Queue.take(output).pipe(Effect.flatMap(emit));
			});
		const states = Stream.fromEffectRepeat(
			Queue.take(output).pipe(Effect.flatMap(emit)),
		);
		return Stream.concat(
			Stream.succeed<WatchQueryState<A>>({ _tag: 'Loading' }),
			states,
		);
	});

export const makeWatchQuery =
	(transport: Transport, run: Client['run']): WatchQuery =>
	<P extends Params, R extends Codec, T extends string, U extends string, I>(
		statement: Statement<P, R, T, U>,
		options: WatchQueryOptions,
		...input: InputOf<P, I>
	) =>
		Stream.unwrap(
			Effect.gen(function* () {
				const { name, writes } = statement;
				const scopeError = (message: string) =>
					new StatementScopeError({ statement: name, message });
				if (writes !== false)
					return yield* Effect.fail(
						scopeError(
							'watchQuery requires a statement declared with writes: false',
						),
					);
				if (options.sources.length === 0)
					return yield* Effect.fail(
						scopeError(
							'watchQuery requires at least one live source',
						),
					);
				const interval = intervalOf(
					options.reconcileEvery ?? '30 seconds',
				);
				if (interval === undefined)
					return yield* Effect.fail(
						scopeError(
							'watchQuery reconcileEvery must be positive and finite',
						),
					);
				const watch = transport.watch;
				if (watch === undefined || transport.live === undefined)
					return yield* Effect.fail(
						new StatementUnsupported({
							feature: 'live query',
							statement: name,
							message:
								'watchQuery requires live queries and connection lifecycle events',
						}),
					);
				return yield* observer(
					name,
					transport,
					watch,
					options.sources,
					interval,
					run<P, R, T, U, I>(statement, ...input),
				);
			}),
		);
