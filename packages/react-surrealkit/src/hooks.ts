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
	idleState,
	type LiveRowOf,
	type LiveText,
	type ParamsOf,
	type ParamsOptional,
	type Preloaded,
	type QueryKind,
	type QueryStatus,
	type QueryText,
	resolveRequest,
	type SurrealLike,
	type SurrealQueryError,
	seedState,
	ViewMemory,
	type ViewState,
} from 'surrealkit-query';

import { useSurreal } from './context.js';

/** A query's state. */
export interface QueryResult<T, Q extends string = string> {
	/** The query text, typed as written, so `<Query q={result}>` is typed by it. */
	readonly text: Q;
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
export interface LiveResult<Row, Q extends string = string>
	extends Omit<QueryResult<Row[], Q>, 'data'> {
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
): QueryResult<DataOf<Q>, Q> {
	return useEntry('query', q, args[0], args[1]) as QueryResult<DataOf<Q>, Q>;
}

/**
 * A live query. `q` is a plain SELECT; the rows follow every change to the
 * records it selects. See the `surrealkit-query` README for which clauses
 * are applied to notifications directly and which re-run the SELECT.
 */
export function useLive<Q extends QueryText>(
	q: LiveText<Q> | Preloaded<Q>,
	...args: HookArgs<Q>
): LiveResult<LiveRowOf<Q>, Q> {
	return useEntry('live', q, args[0], args[1]) as LiveResult<LiveRowOf<Q>, Q>;
}

type Kind = QueryKind;

const idleSubscribe = () => () => {};
const IDLE: Record<Kind, ViewState> = {
	query: { ...idleState('query'), stale: false },
	live: { ...idleState('live'), stale: false },
};
const idleSnapshots: Record<Kind, () => ViewState> = {
	query: () => IDLE.query,
	live: () => IDLE.live,
};

/** @internal Shared by the hooks and the components. */
export function useEntry(
	kind: Kind,
	q: string | Preloaded,
	params: object | undefined,
	options: HookOptions | undefined,
): QueryResult<unknown> {
	const cache = cacheFor(useSurreal(options?.client));
	const request = resolveRequest(q, params);
	const { key } = request;
	const memory = useRef<ViewMemory | undefined>(undefined);
	memory.current ??= new ViewMemory();

	// Keyed by value: the request's text and params are read only when the key
	// changes.
	// biome-ignore lint/correctness/useExhaustiveDependencies: the key stands for the request.
	const view = useMemo(() => {
		const seed = seedState(kind, request, cache);
		const observable =
			kind === 'live'
				? cache.observeLive(request.text, request.params)
				: cache.observe(request.text, request.params);
		const shown = memory.current as ViewMemory;
		const snapshot = (): ViewState =>
			shown.show(key, observable.get(), seed);
		return { observable, snapshot };
	}, [cache, kind, key, request.payload?.data]);

	const subscribe = useCallback(
		(listener: () => void) => view.observable.subscribe(listener),
		[view],
	);
	const enabled = options?.enabled ?? true;
	const state = useSyncExternalStore(
		enabled ? subscribe : idleSubscribe,
		enabled ? view.snapshot : idleSnapshots[kind],
		enabled ? view.snapshot : idleSnapshots[kind],
	);
	const { text } = request;

	return {
		text,
		data: kind === 'live' ? (state.data ?? []) : state.data,
		error: state.error,
		status: state.status,
		loading: state.status === 'pending',
		stale: state.stale,
		refetch: view.observable.refetch,
	};
}
