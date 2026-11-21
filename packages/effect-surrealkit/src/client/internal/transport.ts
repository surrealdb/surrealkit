import { Duration, Effect, type Schedule } from 'effect';
import { BoundQuery, type Duration as SurrealDuration } from 'surrealdb';

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
	| { readonly kind: 'row'; readonly slot: number; readonly value: unknown }
	| {
			readonly kind: 'done';
			readonly slot: number;
			readonly server?: Duration.Duration;
	  }
	| {
			readonly kind: 'error';
			readonly slot: number;
			readonly error: unknown;
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
		return db.transaction([bound], { signal });
	};
	const query = (request: Request, signal: Signal) =>
		db.query(request.sql, request.bindings).signal(signal);
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
			const responses = await query(request, signal).responses();
			return responses.map((response) => {
				const timing =
					response.stats === undefined
						? {}
						: { server: sdkDuration(response.stats.duration) };
				return response.success
					? { ok: true, value: response.result, ...timing }
					: { ok: false, error: response.error, ...timing };
			});
		},
		stream: () => {
			throw new Error(
				'effect-surrealkit: stream is not implemented yet (lane L6)',
			);
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
