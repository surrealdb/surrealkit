import { Duration, Effect, type Schedule } from 'effect';
import {
	BoundQuery,
	type QueryResponse,
	type Duration as SurrealDuration,
	Uuid,
} from 'surrealdb';

import type { AnyStatement } from '../../statement/statement.js';
import type { Queryable, RunOptions } from '../client.js';

/** The abort signal a request takes, as the SDK names it. */
export type Signal = NonNullable<
	Parameters<ReturnType<Queryable['query']>['signal']>[0]
>;

/** One statement of a request: its original bindings and the names they were sent under. */
export interface RequestPart {
	readonly statement: AnyStatement;
	readonly bindings: Readonly<Record<string, unknown>>;
	readonly sent: Readonly<Record<string, string>>;
	/** Index of the first slot in the answer, and the number of slots this part occupies. */
	readonly offset: number;
	readonly slots: number;
}

export interface Request {
	readonly mode: 'query' | 'atomic';
	/** Bulk owns replay and disables SDK-level retries on its collected writes. */
	readonly sdkRetry?: false;
	readonly sql: string;
	readonly bindings: Readonly<Record<string, unknown>>;
	readonly parts: ReadonlyArray<RequestPart>;
}

export type SlotAnswer =
	| {
			readonly ok: true;
			readonly value: unknown;
			readonly server?: Duration.Duration;
	  }
	| {
			readonly ok: false;
			readonly error: unknown;
			readonly server?: Duration.Duration;
	  };

export interface TransactionHandle {
	readonly send: (
		request: Request,
		signal: Signal,
	) => PromiseLike<ReadonlyArray<SlotAnswer>>;
	readonly commit: () => PromiseLike<void>;
	readonly cancel: () => PromiseLike<void>;
}

export interface LiveHandle {
	readonly messages: AsyncIterable<{
		readonly action: 'CREATE' | 'UPDATE' | 'DELETE' | 'KILLED';
		readonly id?: unknown;
		readonly value?: unknown;
	}>;
	readonly kill: () => PromiseLike<void>;
}

export type StreamFrame =
	| {
			readonly kind: 'row' | 'value';
			readonly slot: number;
			readonly value: unknown;
	  }
	| {
			readonly kind: 'done';
			readonly slot: number;
			readonly server?: Duration.Duration;
	  }
	| {
			readonly kind: 'error';
			readonly slot: number;
			readonly error: unknown;
			readonly server?: Duration.Duration;
	  };

export interface Transport {
	/** The existing collector, which preserves SDK query retry and failure selection. */
	readonly collect: (
		request: Request,
		signal: Signal,
	) => PromiseLike<unknown>;
	readonly send: (
		request: Request,
		signal: Signal,
	) => PromiseLike<ReadonlyArray<SlotAnswer>>;
	/** The value slot arrives row by row, with other slots arriving as whole values. */
	readonly stream: (
		request: Request,
		signal: Signal,
	) => AsyncIterable<StreamFrame>;
	/** Absent when the connection cannot do it. */
	readonly begin?: (signal: Signal) => PromiseLike<TransactionHandle>;
	readonly live?: (
		request: Request,
		signal: Signal,
	) => PromiseLike<LiveHandle>;
}

const sdkDuration = (value: SurrealDuration): Duration.Duration => {
	const [seconds = 0n, nanos = 0n] = value.toCompact();
	return Duration.nanos(seconds * 1_000_000_000n + nanos);
};

const responseSlots = (
	responses: ReadonlyArray<QueryResponse>,
): ReadonlyArray<SlotAnswer> =>
	responses.map((response) => {
		const timing =
			response.stats === undefined
				? {}
				: { server: sdkDuration(response.stats.duration) };
		return response.success
			? { ok: true, value: response.result, ...timing }
			: { ok: false, error: response.error, ...timing };
	});

export const sdkTransport = (db: Queryable): Transport => {
	// One request per transaction, encoded once and sent again as it is on a
	// retry. Separate requests may reuse a bindings object with different SQL.
	const requests = new WeakMap<Request, BoundQuery>();
	const collectAtomic = (request: Request, signal: Signal) => {
		let bound = requests.get(request);
		if (bound === undefined) {
			bound = new BoundQuery(request.sql, request.bindings);
			requests.set(request, bound);
		}
		return db.transaction([bound], {
			signal,
			...(request.sdkRetry === false && { retry: false }),
		});
	};
	const query = (request: Request, signal: Signal) => {
		const pending = db.query(request.sql, request.bindings).signal(signal);
		return request.sdkRetry === false ? pending.retry(false) : pending;
	};
	const beginTransaction = db.beginTransaction?.bind(db);
	const liveOf = db.liveOf?.bind(db);
	return {
		collect: (request, signal) =>
			request.mode === 'atomic'
				? collectAtomic(request, signal)
				: query(request, signal),
		send: async (request, signal) => {
			if (request.mode === 'atomic') {
				const values = await collectAtomic(request, signal);
				return values.map((value) => ({ ok: true, value }));
			}
			return responseSlots(await query(request, signal).responses());
		},
		...(beginTransaction === undefined
			? {}
			: {
					begin: async (signal: Signal) => {
						signal.throwIfAborted();
						const tx = await beginTransaction();
						return {
							send: async (request: Request, signal: Signal) =>
								responseSlots(
									await tx
										.query(request.sql, request.bindings)
										.signal(signal)
										.responses(),
								),
							commit: () => tx.commit(),
							cancel: () => tx.cancel(),
						};
					},
				}),
		...(liveOf === undefined
			? {}
			: {
					live: async (
						request: Request,
						signal: Signal,
					): Promise<LiveHandle> => {
						signal.throwIfAborted();
						// Wait for the query ID even if the scope ends during registration.
						// The owner can then kill the exact query before releasing the connection.
						const ids = await db
							.query(request.sql, request.bindings)
							.retry(false)
							.collect();
						const [id] = ids;
						if (ids.length !== 1 || !(id instanceof Uuid))
							throw new Error(
								'LIVE SELECT did not return one query UUID',
							);
						const killById = async () => {
							await db
								.query('KILL $id;', { id })
								.retry(false)
								.collect();
						};
						let attached:
							| Awaited<ReturnType<typeof liveOf>>
							| undefined;
						try {
							attached = await liveOf(id);
							const subscription = attached;
							// Register the iterator before the caller can issue its first write.
							const iterator =
								subscription[Symbol.asyncIterator]();
							signal.throwIfAborted();
							return {
								messages: (async function* () {
									for await (const message of {
										[Symbol.asyncIterator]: () => iterator,
									}) {
										if (message.action === 'KILLED')
											yield { action: message.action };
										else
											yield {
												action: message.action,
												id: message.recordId,
												value: message.value,
											};
									}
								})(),
								// The SDK caches a rejected kill. A fresh request
								// on the same session can still release this UUID.
								kill: () => subscription.kill().catch(killById),
							};
						} catch (error) {
							const cleanup =
								attached === undefined
									? killById()
									: attached.kill().catch(killById);
							await cleanup.catch(() => {
								throw new Error(
									'live query attachment failed and cleanup could not be confirmed',
									{ cause: error },
								);
							});
							throw error;
						}
					},
				}),
		stream: async function* (request, signal) {
			const part = request.parts[0];
			const valueSlot =
				part === undefined
					? -1
					: part.offset + part.statement.valueSlot - 1;
			for await (const frame of query(request, signal).stream()) {
				const slot = frame.query;
				if (frame.isValue()) {
					if (
						frame.isSingle &&
						slot === valueSlot &&
						Array.isArray(frame.value)
					) {
						for (const value of frame.value)
							yield { kind: 'row', slot, value };
					} else {
						yield {
							kind: frame.isSingle ? 'value' : 'row',
							slot,
							value: frame.value,
						};
					}
				} else if (frame.isDone()) {
					const timing =
						frame.stats === undefined
							? {}
							: { server: sdkDuration(frame.stats.duration) };
					yield { kind: 'done', slot, ...timing };
				} else if (frame.isError()) {
					const timing =
						frame.stats === undefined
							? {}
							: { server: sdkDuration(frame.stats.duration) };
					yield {
						kind: 'error',
						slot,
						error: frame.error,
						...timing,
					};
				}
			}
		},
	};
};

/**
 * Sends a request, again while it conflicts and `retry` allows. Each send
 * is the same request; `sent` counts them.
 */
export const sending = <A, E extends { readonly _tag: string }>(
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
