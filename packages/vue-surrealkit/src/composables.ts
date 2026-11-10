/**
 * `useQuery` and `useLive`: typed queries as Vue composables.
 *
 * ```ts
 * const lim = ref(10);
 * const start = ref(0);
 * const { data: users, loading, stale } = useLive(
 * 	'SELECT * FROM user ORDER BY name LIMIT $lim START $start',
 * 	() => ({ lim: lim.value, start: start.value }),
 * );
 * ```
 *
 * The query text is a key of the generated `Queries` registry, which types
 * `data` and the parameters. `q` and the parameters may each be a value, a
 * ref or a getter, and the parameters may be a `reactive()` object. They are
 * compared by value: different values stop the old query (a live one is
 * killed) and start the new one, equal values start nothing.
 *
 * The query starts with the component (or `effectScope`) that calls the
 * composable and stops when it is unmounted. Call it during setup. A server
 * render runs nothing: it shows a `preload` payload's result, or pending.
 */

import {
	cacheFor,
	type DataOf,
	idleState,
	type LiveRowOf,
	type LiveText,
	type Observable,
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
import {
	type ComputedRef,
	computed,
	type MaybeRefOrGetter,
	type Ref,
	shallowRef,
	toValue,
	watch,
} from 'vue';

import { useSurreal } from './context.js';

const HANDLE = Symbol.for('vue-surrealkit.handle');

/** A query's reactive state, as refs. */
export interface QueryRefs<T, Q extends string = string, R = T> {
	/** The query text, typed as written, so `<Query :q="refs">` is typed by it. */
	readonly text: Q;
	/** The result, once there is one. */
	readonly data: Readonly<Ref<T | undefined>>;
	/** What `select` made of the result, or the result without one. */
	readonly selected: Readonly<Ref<R | undefined>>;
	readonly error: Readonly<Ref<SurrealQueryError | undefined>>;
	readonly status: Readonly<Ref<QueryStatus>>;
	/** `status === 'pending'`. */
	readonly loading: Readonly<Ref<boolean>>;
	/**
	 * The data is the previous parameters' result, kept while the result for
	 * the new parameters loads. Never set across a cache `reset()`.
	 */
	readonly stale: Readonly<Ref<boolean>>;
	/** Runs the query again. */
	refetch(): Promise<void>;
}

/** A live query's reactive state. `data` is always the current rows. */
export interface LiveRefs<Row, Q extends string = string, R = Row[]>
	extends Omit<QueryRefs<Row[], Q, R>, 'data' | 'selected'> {
	/** The current rows, `[]` until the first snapshot. */
	readonly data: Readonly<Ref<Row[]>>;
	readonly selected: Readonly<Ref<R>>;
}

/**
 * A query's parameters: a value, a ref, a getter, or a `reactive()` object.
 * A plain value may not be a function: a function has a `name`, so without
 * this a getter returning the wrong parameters would pass as the object.
 */
export type ParamsSource<P> =
	| Ref<P>
	| ComputedRef<P>
	| (() => P)
	| (P & { readonly apply?: never });

export interface ComposableOptions<D, R> {
	/** The client to use instead of the provided one. */
	client?: SurrealLike;
	/**
	 * While this is `false`, nothing subscribes: `data` is `[]` (live) or
	 * `undefined`, and `status` is `pending`. Default `true`.
	 */
	enabled?: MaybeRefOrGetter<boolean>;
	/** Maps the result into `selected`. */
	select?: (data: D) => R;
}

/** The arguments after `q`: parameters (required when `Q` needs any), then options. */
export type ComposableArgs<Q extends string, D, R> =
	ParamsOptional<Q> extends true
		? [
				params?: ParamsSource<ParamsOf<Q>> | undefined,
				options?: ComposableOptions<D, R>,
			]
		: [
				params: ParamsSource<ParamsOf<Q>>,
				options?: ComposableOptions<D, R>,
			];

/** A one-shot query. `data` is the query's result. */
export function useQuery<Q extends QueryText, R = DataOf<Q>>(
	q: MaybeRefOrGetter<Q | Preloaded<Q>>,
	...args: ComposableArgs<Q, DataOf<Q>, R>
): QueryRefs<DataOf<Q>, Q, R> {
	return useView('query', q, args[0], args[1]) as unknown as QueryRefs<
		DataOf<Q>,
		Q,
		R
	>;
}

/**
 * A live query. `q` is a plain SELECT; the rows follow every change to the
 * records it selects. See the `surrealkit-query` README for which clauses
 * are applied to notifications directly and which re-run the SELECT.
 */
export function useLive<Q extends QueryText, R = LiveRowOf<Q>[]>(
	q: MaybeRefOrGetter<LiveText<Q> | Preloaded<Q>>,
	...args: ComposableArgs<Q, LiveRowOf<Q>[], R>
): LiveRefs<LiveRowOf<Q>, Q, R> {
	return useView('live', q, args[0], args[1]) as unknown as LiveRefs<
		LiveRowOf<Q>,
		Q,
		R
	>;
}

/** Whether `value` is the result of `useQuery` or `useLive`. */
export function isQueryRefs(value: unknown): value is QueryRefs<unknown> {
	return typeof value === 'object' && value !== null && HANDLE in value;
}

const onServer = typeof window === 'undefined';

/** @internal Shared by the composables and the components. */
export function useView(
	kind: QueryKind,
	q: MaybeRefOrGetter<string | Preloaded>,
	params: unknown,
	options: ComposableOptions<never, unknown> | undefined,
): QueryRefs<unknown> {
	const cache = cacheFor(options?.client ?? useSurreal());
	// Reading the parameters inside the computed tracks each one, also the
	// properties of a reactive() object, so a change to any of them is seen.
	const request = computed(() =>
		resolveRequest(toValue(q), toValue(params as MaybeRefOrGetter<object>)),
	);
	// A string: a change to equal values leaves it unchanged, and nothing runs.
	const key = computed(() => request.value.key);
	const enabled = computed(() => toValue(options?.enabled) ?? true);

	const memory = new ViewMemory();
	const view = shallowRef<ViewState>({ ...idleState(kind), stale: false });
	let observable: Observable<unknown> | undefined;

	watch(
		[key, enabled],
		([current, on], _, onCleanup) => {
			if (!on) {
				observable = undefined;
				view.value = { ...idleState(kind), stale: false };
				return;
			}
			const req = request.value;
			const seed = seedState(kind, req, cache);
			const watched =
				kind === 'live'
					? cache.observeLive(req.text, req.params)
					: cache.observe(req.text, req.params);
			observable = watched;
			const read = () => {
				view.value = memory.show(current, watched.get(), seed);
			};
			read();
			if (onServer) return;
			onCleanup(watched.subscribe(read));
		},
		{ immediate: true },
	);

	const data = computed(() =>
		kind === 'live' ? (view.value.data ?? []) : view.value.data,
	);
	const select = options?.select;
	return {
		[HANDLE]: true,
		get text() {
			return request.value.text;
		},
		data,
		selected: computed(() =>
			select && data.value !== undefined
				? select(data.value as never)
				: data.value,
		),
		error: computed(() => view.value.error),
		status: computed(() => view.value.status),
		loading: computed(() => view.value.status === 'pending'),
		stale: computed(() => view.value.stale),
		refetch: () => observable?.refetch() ?? Promise.resolve(),
	} as QueryRefs<unknown>;
}
