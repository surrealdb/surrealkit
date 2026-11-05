/**
 * `useQuery` and `useLive`: typed queries as React hooks.
 *
 * ```tsx
 * 'use client';
 * import { useLive } from 'react-surrealkit';
 *
 * export function Users({ lim, start }: { lim: number; start: number }) {
 * 	const users = useLive(
 * 		'SELECT * FROM user ORDER BY name LIMIT $lim START $start',
 * 		{ lim, start },
 * 	);
 * 	if (users.error) return <p>{users.error.message}</p>;
 * 	return <ul>{users.data.map((u) => <li key={String(u.id)}>{u.name}</li>)}</ul>;
 * }
 * ```
 *
 * The query text is a key of the generated `Queries` registry, which types
 * `data` and the parameters. A hook follows its parameters by value: a new
 * object with equal values starts nothing, different values stop the old
 * query (a live one is killed) and start the new one. Mounting subscribes,
 * unmounting kills the live query, and hooks showing the same query and
 * parameters share one. Strict mode's double mount keeps the same live query.
 *
 * `q` may also be a {@link Preloaded} payload from a Server Component, which
 * renders the server's result at once.
 */

import { useCallback, useMemo, useRef, useSyncExternalStore } from 'react';
import {
	cacheFor,
	type DataOf,
	decodeValue,
	isPreloaded,
	type LiveRowOf,
	type LiveText,
	type ParamsOf,
	type ParamsOptional,
	type Preloaded,
	preloadedParams,
	type QueryState,
	type QueryStatus,
	type QueryText,
	queryKey,
	type SurrealLike,
	type SurrealQueryError,
} from 'surrealkit-query';

import { useSurreal } from './context.js';

/** A query's state. */
export interface QueryResult<T> {
	/** The result, once there is one. */
	readonly data: T | undefined;
	readonly error: SurrealQueryError | undefined;
	readonly status: QueryStatus;
	/** `status === 'pending'`. */
	readonly loading: boolean;
	/**
	 * The data is the previous parameters' result, kept while the result for
	 * the new parameters loads. Never set across a cache `reset()`, so one
	 * identity's rows are never shown to the next.
	 */
	readonly stale: boolean;
	/** Runs the query again. */
	refetch(): Promise<void>;
}

/** A live query's state. `data` is always the current rows. */
export interface LiveResult<Row> extends Omit<QueryResult<Row[]>, 'data'> {
	/** The current rows, `[]` until the first snapshot. */
	readonly data: Row[];
}

export interface HookOptions {
	/** The client to use instead of the provider's. */
	client?: SurrealLike;
	/**
	 * While `false`, nothing subscribes: `data` is `[]` (live) or `undefined`,
	 * and `status` is `pending`. Default `true`.
	 */
	enabled?: boolean;
}

/** The arguments after `q`: parameters (required when `Q` needs any), then options. */
export type HookArgs<Q extends string> =
	ParamsOptional<Q> extends true
		? [params?: ParamsOf<Q> | undefined, options?: HookOptions]
		: [params: ParamsOf<Q>, options?: HookOptions];

/** A one-shot query. `data` is the query's result. */
export function useQuery<Q extends QueryText>(
	q: Q | Preloaded<Q>,
	...args: HookArgs<Q>
): QueryResult<DataOf<Q>> {
	return useEntry('query', q, args[0], args[1]) as QueryResult<DataOf<Q>>;
}

/**
 * A live query. `q` is a plain SELECT; the rows follow every change to the
 * records it selects. See the `surrealkit-query` README for which clauses
 * are applied to notifications directly and which re-run the SELECT.
 */
export function useLive<Q extends QueryText>(
	q: LiveText<Q> | Preloaded<Q>,
	...args: HookArgs<Q>
): LiveResult<LiveRowOf<Q>> {
	return useEntry('live', q, args[0], args[1]) as LiveResult<LiveRowOf<Q>>;
}

type Kind = 'query' | 'live';

const IDLE: QueryState<unknown> = {
	status: 'pending',
	data: undefined,
	error: undefined,
};
const idleSubscribe = () => () => {};
const idleSnapshot = () => IDLE;

const browser = typeof document !== 'undefined';

function useEntry(
	kind: Kind,
	q: string | Preloaded,
	params: object | undefined,
	options: HookOptions | undefined,
): QueryResult<unknown> {
	const cache = cacheFor(useSurreal(options?.client));
	const payload = isPreloaded(q) ? q : undefined;
	const text = payload ? payload.text : (q as string);
	const bound = params ?? (payload ? preloadedParams(payload) : undefined);
	const key = queryKey(text, bound);
	// The last result this hook showed, so new parameters can show it until
	// theirs lands.
	const last = useRef<{ key: string; data: unknown } | undefined>(undefined);

	// Keyed by value: `bound` is read only when the key changes.
	// biome-ignore lint/correctness/useExhaustiveDependencies: the key stands for text and bound.
	const view = useMemo(() => {
		let seeded: QueryState<unknown> | undefined;
		if (
			payload &&
			queryKey(payload.text, preloadedParams(payload)) === key
		) {
			const result = decodeValue<unknown[]>(payload.data);
			const data =
				kind === 'live'
					? result[0]
					: result.length === 1
						? result[0]
						: result;
			seeded = { status: 'success', data, error: undefined };
			if (browser) cache.hydrate(payload);
		}
		const observable =
			kind === 'live'
				? cache.observeLive(text, bound)
				: cache.observe(text, bound);
		const previous = last.current?.key === key ? undefined : last.current;
		const stale: QueryState<unknown> | undefined = previous && {
			status: 'success',
			data: previous.data,
			error: undefined,
		};
		// Once this key has a result, a later pending state (a reset) is shown
		// as pending: the previous key's rows never come back.
		let settled = false;
		const snapshot = (): QueryState<unknown> => {
			const state = observable.get();
			if (state.status === 'pending' && seeded) return seeded;
			if (state.status !== 'pending') settled = true;
			return state.status === 'pending' && !settled && stale
				? stale
				: state;
		};
		return { observable, snapshot, stale };
	}, [cache, kind, key, payload?.data]);

	const subscribe = useCallback(
		(listener: () => void) => view.observable.subscribe(listener),
		[view],
	);
	const enabled = options?.enabled ?? true;
	const state = useSyncExternalStore(
		enabled ? subscribe : idleSubscribe,
		enabled ? view.snapshot : idleSnapshot,
		enabled ? view.snapshot : idleSnapshot,
	);
	const isStale = enabled && state === view.stale;
	if (state.status === 'success' && !isStale) {
		last.current = { key, data: state.data };
	}

	return {
		data: kind === 'live' ? (state.data ?? []) : state.data,
		error: state.error,
		status: state.status,
		loading: state.status === 'pending',
		stale: isStale,
		refetch: view.observable.refetch,
	};
}
