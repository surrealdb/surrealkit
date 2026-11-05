/**
 * `createQuery` and `createLive`: typed queries as Svelte 5 reactive state.
 *
 * ```svelte
 * <script lang="ts">
 * 	import { createLive } from 'svelte-surrealkit';
 *
 * 	let lim = $state(10);
 * 	let start = $state(0);
 * 	const users = createLive(
 * 		'SELECT * FROM user ORDER BY name LIMIT $lim START $start',
 * 		() => ({ lim, start }),
 * 	);
 * </script>
 *
 * {#each users.data as user (user.id)}<p>{user.name}</p>{/each}
 * ```
 *
 * The query text is a key of the generated `Queries` registry, which types
 * `data` and the parameters. Parameters are a getter, so the query follows
 * the `$state` and `$derived` values it reads: when they change to different
 * values the old query stops (a live one is killed) and the new one starts.
 * A getter that returns equal values starts nothing.
 *
 * The handle reads lazily: the query starts the first time `data`, `status`,
 * `error` or `loading` is read inside an effect (markup counts), and stops
 * once nothing reads it, which includes the component being destroyed. That
 * works in a component and in a shared `.svelte.ts` module alike.
 *
 * `q` may also be a {@link Preloaded} payload from a `load` function, which
 * renders the server's result at once, or a getter returning either.
 */

import {
	cacheFor,
	type DataOf,
	decodeValue,
	isPreloaded,
	type LiveRowOf,
	type LiveText,
	type Observable,
	type ParamsOf,
	type ParamsOptional,
	type Preloaded,
	preloadedParams,
	type QueryCache,
	type QueryState,
	type QueryStatus,
	type QueryText,
	queryKey,
	type SurrealLike,
	type SurrealQueryError,
} from 'surrealkit-query';
import { untrack } from 'svelte';
import { createSubscriber } from 'svelte/reactivity';

import { getSurreal } from './context.js';

const HANDLE = Symbol.for('svelte-surrealkit.handle');

/** A query's reactive view. */
export interface QueryHandle<T, Q extends string = string> {
	/** The query text, typed as written, so a component given this handle is typed by it. */
	readonly text: Q;
	/** The result, once there is one. */
	readonly data: T | undefined;
	readonly error: SurrealQueryError | undefined;
	readonly status: QueryStatus;
	/** `status === 'pending'`. */
	readonly loading: boolean;
	/**
	 * The data is the previous parameters' result, kept while the result for
	 * the new parameters loads. Never set across a {@link QueryCache.reset}, so
	 * one identity's rows are never shown to the next.
	 */
	readonly stale: boolean;
	/** Runs the query again. */
	refetch(): Promise<void>;
}

/** A live query's reactive view. `data` is always the current rows. */
export interface LiveHandle<Row, Q extends string = string>
	extends Omit<QueryHandle<Row[], Q>, 'data'> {
	/** The current rows, `[]` until the first snapshot. */
	readonly data: Row[];
}

export interface CreateOptions {
	/** The client to use instead of the one {@link setSurreal} provided. */
	client?: SurrealLike;
	/**
	 * While this returns `false`, nothing subscribes: `data` is `[]` (live) or
	 * `undefined`, and `status` is `pending`. Default: always enabled.
	 */
	enabled?: () => boolean;
}

/** A value, or a getter for one that is re-read when its dependencies change. */
export type Getter<T> = T | (() => T);

/** A query's text or payload, or a getter for either. */
export type QuerySource<Q extends string> = Getter<Q | Preloaded<Q>>;

/**
 * The arguments after `q`: a getter for the parameters (required when `Q`
 * needs any), then options. The parameters are always a getter: a function
 * has a `name`, so a union of an object and a getter would accept a getter
 * returning the wrong parameters.
 */
export type CreateArgs<Q extends string> =
	ParamsOptional<Q> extends true
		? [params?: () => ParamsOf<Q> | undefined, options?: CreateOptions]
		: [params: () => ParamsOf<Q>, options?: CreateOptions];

/** A one-shot query. `data` is the query's result. */
export function createQuery<Q extends QueryText>(
	q: QuerySource<Q>,
	...args: CreateArgs<Q>
): QueryHandle<DataOf<Q>, Q> {
	return handle('query', q, args[0], args[1]) as unknown as QueryHandle<
		DataOf<Q>,
		Q
	>;
}

/**
 * A live query. `q` is a plain SELECT; the rows follow every change to the
 * records it selects. See the package README for which clauses are applied
 * to notifications directly and which re-run the SELECT.
 */
export function createLive<Q extends QueryText>(
	q: Getter<LiveText<Q> | Preloaded<Q>>,
	...args: CreateArgs<Q>
): LiveHandle<LiveRowOf<Q>, Q> {
	return handle('live', q, args[0], args[1]) as unknown as LiveHandle<
		LiveRowOf<Q>,
		Q
	>;
}

/** Whether `value` is a handle from {@link createQuery} or {@link createLive}. */
export function isHandle(value: unknown): value is QueryHandle<unknown> {
	return typeof value === 'object' && value !== null && HANDLE in value;
}

type Kind = 'query' | 'live';

interface Request {
	readonly text: string;
	readonly params: object | undefined;
	readonly payload: Preloaded | undefined;
}

interface View {
	readonly key: string;
	readonly observable: Observable<unknown>;
	readonly seed: unknown;
	readonly track: () => void;
}

const browser = typeof document !== 'undefined';

/** The decoded result of each payload, so a payload is decoded once. */
const decoded = new WeakMap<Preloaded, unknown[]>();

function resultOf(payload: Preloaded): unknown[] {
	let result = decoded.get(payload);
	if (!result) {
		result = decodeValue<unknown[]>(payload.data);
		decoded.set(payload, result);
	}
	return result;
}

/** Shared by the runes and the components. */
export function handle(
	kind: Kind,
	q: Getter<string | Preloaded>,
	params: (() => object | undefined) | undefined,
	options: CreateOptions | undefined,
): QueryHandle<unknown> & { readonly [HANDLE]: true } {
	const cache = cacheFor(options?.client ?? getSurreal());

	const request = $derived.by((): Request => {
		const source = typeof q === 'function' ? q() : q;
		const explicit = params?.();
		if (isPreloaded(source)) {
			return {
				text: source.text,
				params: explicit ?? preloadedParams(source),
				payload: source,
			};
		}
		return { text: source, params: explicit, payload: undefined };
	});

	// A string, so a getter that returns equal values leaves it unchanged and
	// nothing downstream re-runs.
	const key = $derived(queryKey(request.text, request.params));

	const enabled = $derived(options?.enabled?.() ?? true);

	const view = $derived.by((): View | undefined => {
		void key;
		if (!enabled) return undefined;
		const { text, params, payload } = untrack(() => request);
		let seed: unknown;
		if (
			payload &&
			queryKey(payload.text, preloadedParams(payload)) === key
		) {
			const result = resultOf(payload);
			seed =
				kind === 'live'
					? result[0]
					: result.length === 1
						? result[0]
						: result;
			if (browser) cache.hydrate(payload);
		}
		const observable =
			kind === 'live'
				? cache.observeLive(text, params)
				: cache.observe(text, params);
		return {
			key,
			observable,
			seed,
			track: createSubscriber((update) => observable.subscribe(update)),
		};
	});

	// The last result shown, so new parameters can show it until theirs lands.
	let last: { key: string; data: unknown } | undefined;

	const read = (): QueryState<unknown> & { stale: boolean } => {
		const current = view;
		if (!current) {
			const data = kind === 'live' ? [] : undefined;
			return { status: 'pending', data, error: undefined, stale: false };
		}
		current.track();
		const state = current.observable.get();
		const settled =
			state.status === 'pending' && current.seed !== undefined
				? {
						status: 'success' as const,
						data: current.seed,
						error: undefined,
					}
				: state;
		if (settled.status === 'success') {
			last = { key: current.key, data: settled.data };
			return { ...settled, stale: false };
		}
		if (settled.status === 'pending' && last && last.key !== current.key) {
			return {
				status: 'success',
				data: last.data,
				error: undefined,
				stale: true,
			};
		}
		return { ...settled, stale: false };
	};

	return {
		[HANDLE]: true,
		get text() {
			return request.text;
		},
		get data() {
			const data = read().data;
			return kind === 'live' ? (data ?? []) : data;
		},
		get error() {
			return read().error;
		},
		get status() {
			return read().status;
		},
		get loading() {
			return read().status === 'pending';
		},
		get stale() {
			return read().stale;
		},
		refetch: () => view?.observable.refetch() ?? Promise.resolve(),
	};
}
