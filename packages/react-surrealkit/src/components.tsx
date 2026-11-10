/**
 * `<Query>` and `<LiveQuery>`: a query rendered from its result, with the
 * same props as svelte-surrealkit's components. Snippets become props:
 * `children` is a function of the result and its state, and `empty`,
 * `loading` and `error` are elements (`error` may also be a function of the
 * failure).
 *
 * ```tsx
 * <LiveQuery
 * 	q='SELECT * FROM user ORDER BY name LIMIT $lim START $start'
 * 	params={{ lim, start }}
 * 	empty={<p>Nobody yet.</p>}
 * 	loading={<p>Loading…</p>}
 * >
 * 	{(users, { stale }) => (
 * 		<ul className={stale ? 'dim' : undefined}>
 * 			{users.map((u) => <li key={String(u.id)}>{u.name}</li>)}
 * 		</ul>
 * 	)}
 * </LiveQuery>
 * ```
 *
 * `q` is query text, a `preload` payload, or the result of `useQuery` /
 * `useLive`, which the component then shows (its query, its parameters, one
 * subscription). React has no bindings, so to use the result outside the
 * component, call the hook and pass its result as `q`.
 */

import { type ReactNode, useMemo } from 'react';
import {
	type DataOf,
	isEmptyResult,
	type LiveGuard,
	type LiveRowOf,
	type ParamsFor,
	type Preloaded,
	type QueryText,
	type QueryViewState,
	type SurrealLike,
	type SurrealQueryError,
	type TextOf,
} from 'surrealkit-query';

import { type LiveResult, type QueryResult, useEntry } from './hooks.js';

/** What `<Query q>` accepts. */
export type QueryProp =
	| QueryText
	| Preloaded<QueryText>
	| QueryResult<unknown, QueryText>;

/** What `<LiveQuery q>` accepts. */
export type LiveProp =
	| QueryText
	| Preloaded<QueryText>
	| LiveResult<unknown, QueryText>;

/** The props both components share, for a result of type `D` selected to `R`. */
export interface ViewProps<D, R> {
	/** The client to use instead of the provider's. */
	client?: SurrealLike;
	/**
	 * While `false`, nothing subscribes and nothing renders. Default `true`.
	 * Ignored when `q` is a hook result, which has its own `enabled`.
	 */
	enabled?: boolean;
	/** Maps the result before `children` and the `empty` check. */
	select?: (data: D) => R;
	/** Renders the result (or what `select` made of it) and its state. */
	children?: (data: R, state: QueryViewState) => ReactNode;
	/** Rendered instead of `children` when the result is empty (after `select`). */
	empty?: ReactNode;
	/** Rendered until the first result. */
	loading?: ReactNode;
	/**
	 * Rendered on failure. Without it, a failure with a result to show
	 * renders `children` with `state.error` set, and a failure with none is
	 * thrown to the nearest error boundary.
	 */
	error?: ReactNode | ((error: SurrealQueryError) => ReactNode);
}

/** A one-shot query, rendered from its result. */
export function Query<S extends QueryProp, R = DataOf<TextOf<S>>>(
	props: { q: S } & ParamsFor<S> & ViewProps<DataOf<TextOf<S>>, R>,
): ReactNode {
	return useView('query', props as unknown as AnyProps);
}

/**
 * A live query, rendered from its current rows. Mounting subscribes,
 * unmounting kills the live query, and new `params` resubscribe; components
 * showing the same query and parameters share one live query.
 */
export function LiveQuery<S extends LiveProp, R = LiveRowOf<TextOf<S>>[]>(
	props: { q: S & LiveGuard<S> } & ParamsFor<S> &
		ViewProps<LiveRowOf<TextOf<S>>[], R>,
): ReactNode {
	return useView('live', props as unknown as AnyProps);
}

interface AnyProps extends ViewProps<unknown, unknown> {
	q: unknown;
	params?: object | undefined;
}

function isResult(value: unknown): value is QueryResult<unknown> {
	return (
		typeof value === 'object' &&
		value !== null &&
		'refetch' in value &&
		'status' in value
	);
}

function useView(kind: 'query' | 'live', props: AnyProps): ReactNode {
	const given = isResult(props.q) ? props.q : undefined;
	// A hook result is shown as it is; the component's own query then never
	// subscribes (hooks run unconditionally, so it is disabled, not skipped).
	const own = useEntry(
		kind,
		given ? given.text : (props.q as string | Preloaded),
		given ? undefined : props.params,
		{
			...(props.client ? { client: props.client } : {}),
			enabled: !given && props.enabled !== false,
		},
	);
	const result = given ?? own;
	const { select } = props;
	const shown = useMemo(
		() =>
			select && result.data !== undefined
				? select(result.data)
				: result.data,
		[select, result.data],
	);

	if (!given && props.enabled === false) return null;
	const failure = result.error;
	const failed =
		failure !== undefined &&
		(props.error !== undefined ||
			result.data === undefined ||
			(Array.isArray(result.data) && result.data.length === 0));
	if (failed && failure) {
		if (props.error === undefined) throw failure;
		return typeof props.error === 'function'
			? props.error(failure)
			: props.error;
	}
	if (result.status === 'pending' && !result.stale)
		return props.loading ?? null;
	if (isEmptyResult(shown) && props.empty !== undefined) return props.empty;
	return (
		props.children?.(shown, {
			loading: result.stale,
			stale: result.stale,
			error: failure,
		}) ?? null
	);
}
