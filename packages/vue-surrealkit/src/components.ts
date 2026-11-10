/**
 * `<Query>` and `<LiveQuery>`: a query rendered from its result, with the
 * same props as the Svelte and React components.
 *
 * ```vue
 * <LiveQuery q="SELECT * FROM user ORDER BY name LIMIT $lim START $start" :params="{ lim, start }">
 * 	<template #default="{ data: users, stale }">
 * 		<ul :class="{ dim: stale }"><li v-for="u in users" :key="String(u.id)">{{ u.name }}</li></ul>
 * 	</template>
 * 	<template #empty>Nobody yet.</template>
 * 	<template #loading>Loading…</template>
 * 	<template #error="{ error }">{{ error.message }}</template>
 * </LiveQuery>
 * ```
 *
 * `q` is query text, a `preload` payload, or the result of `useQuery` /
 * `useLive`, which the component then shows (its query, its parameters, one
 * subscription). The slots are all optional:
 *
 * - `default({ data, loading, stale, error })` renders the result, or what
 *   `select` made of it. When `params` change, the previous result stays,
 *   with `stale` set, until the new one lands.
 * - `empty` renders instead when the result is `undefined`, `null` or an
 *   empty array (after `select`).
 * - `loading` renders until the first result. Without it, nothing renders.
 * - `error({ error })` renders on failure. Without it, a failure with a
 *   result to show renders the default slot with `error` set, and a failure
 *   with none is thrown to the nearest `onErrorCaptured`.
 *
 * `v-model:data` and `v-model:selected` expose the result (and what `select`
 * made of it) outside the component, from its own query.
 */

import {
	type DataOf,
	isEmptyResult,
	type LiveGuard,
	type LiveRowOf,
	type LiveTextError,
	type ParamsFor,
	type Preloaded,
	type QueryText,
	type SurrealLike,
	type SurrealQueryError,
	type TextOf,
} from 'surrealkit-query';
import {
	computed,
	defineComponent,
	type SetupContext,
	type SlotsType,
	type VNode,
	watch,
} from 'vue';

import {
	isQueryRefs,
	type LiveRefs,
	type QueryRefs,
	useView,
} from './composables.js';

/** What `<Query q>` accepts. */
export type QueryProp =
	| QueryText
	| Preloaded<QueryText>
	| QueryRefs<unknown, QueryText, unknown>;

/** What `<LiveQuery q>` accepts. */
export type LiveProp =
	| QueryText
	| Preloaded<QueryText>
	| LiveRefs<unknown, QueryText, unknown>;

/** The props both components share, for a result of type `D` selected to `R`. */
export interface ViewProps<D, R> {
	/** The client to use instead of the provided one. */
	client?: SurrealLike;
	/**
	 * While `false`, nothing subscribes and nothing renders. Default `true`.
	 * Ignored when `q` is a composable's result, which has its own `enabled`.
	 */
	enabled?: boolean;
	/** Maps the result before the default slot and the `empty` check. */
	select?: (data: D) => R;
	/** The result, for `v-model:data`. */
	data?: D | undefined;
	/**
	 * What `select` made of the result, for `v-model:selected`. Declare the
	 * bound variable as `select`'s return type (`ReturnType<typeof pick>`):
	 * it types the default slot too.
	 */
	selected?: R | undefined;
	'onUpdate:data'?: (data: D) => void;
	'onUpdate:selected'?: (selected: R) => void;
}

/** The state the default slot gets beside the result. */
export interface ViewSlotScope<R> {
	readonly data: R;
	/** Newer results are on their way: today, exactly when `stale` is set. */
	readonly loading: boolean;
	/** The result is the previous parameters', kept while the new one loads. */
	readonly stale: boolean;
	/** A failure that left a result to show, when no `error` slot claimed it. */
	readonly error: SurrealQueryError | undefined;
}

/** The slots both components take, as `SlotsType`. */
export type ViewSlots<R> = SlotsType<ViewSlotFns<R>>;

type Emits = ('update:data' | 'update:selected')[];

/**
 * What the default slot gets without a `select` prop: a composable's own
 * selection when `q` is one, the result otherwise.
 */
export type SelectedOf<S, D> =
	S extends QueryRefs<unknown, string, infer Selected> ? Selected : D;

/** The props of `<Query>` for a `q` of type `S`, selected to `R`. */
export type QueryProps<S extends QueryProp, R> = { q: S } & ParamsFor<S> &
	ViewProps<DataOf<TextOf<S>>, R>;

/** The props of `<LiveQuery>` for a `q` of type `S`, selected to `R`. */
export type LiveQueryProps<S extends LiveProp, R> = { q: S } & LiveQGuard<S> &
	ParamsFor<S> &
	ViewProps<LiveRowOf<TextOf<S>>[], R>;

/** The slots of either component, for a result (after `select`) of type `R`. */
export interface ViewSlotFns<R> {
	default?: (scope: ViewSlotScope<R>) => VNode[];
	empty?: () => VNode[];
	loading?: () => VNode[];
	error?: (scope: { error: SurrealQueryError }) => VNode[];
}

const PROPS = [
	'q',
	'params',
	'client',
	'enabled',
	'select',
	'data',
	'selected',
] as const;
const EMITS: Emits = ['update:data', 'update:selected'];

/** A one-shot query, rendered from its result. */
export const Query = defineComponent({
	name: 'Query',
	props: [...PROPS],
	emits: EMITS,
	setup: (props, context) =>
		setupView('query', props as unknown as AnyProps, context as never),
}) as unknown as new <
	S extends QueryProp,
	R = SelectedOf<S, DataOf<TextOf<S>>>,
>(
	props: QueryProps<S, R>,
) => {
	$props: QueryProps<S, R>;
	$slots: ViewSlotFns<R>;
	$emit: {
		(event: 'update:data', data: DataOf<TextOf<S>>): void;
		(event: 'update:selected', selected: R): void;
	};
};

/**
 * A live query, rendered from its current rows. Mounting subscribes,
 * unmounting kills the live query, and new `params` resubscribe; components
 * showing the same query and parameters share one live query.
 */
export const LiveQuery = defineComponent({
	name: 'LiveQuery',
	props: [...PROPS],
	emits: EMITS,
	setup: (props, context) =>
		setupView('live', props as unknown as AnyProps, context as never),
}) as unknown as new <
	S extends LiveProp,
	R = SelectedOf<S, LiveRowOf<TextOf<S>>[]>,
>(
	props: LiveQueryProps<S, R>,
) => {
	$props: LiveQueryProps<S, R>;
	$slots: ViewSlotFns<R>;
	$emit: {
		(event: 'update:data', data: LiveRowOf<TextOf<S>>[]): void;
		(event: 'update:selected', selected: R): void;
	};
};

/**
 * Rejects text that cannot be live by narrowing `q` to the error brand. A
 * separate member, so `S` is still inferred from a plain `{ q: S }`.
 */
type LiveQGuard<S> = [LiveGuard<S>] extends [LiveTextError]
	? { q: LiveTextError }
	: unknown;

interface AnyProps extends ViewProps<unknown, unknown> {
	q: unknown;
	params?: object | undefined;
}

function setupView(
	kind: 'query' | 'live',
	props: AnyProps,
	{ slots, emit }: SetupContext<Emits, ViewSlots<unknown>>,
): () => VNode[] | null {
	// A composable's result is shown as it is; otherwise this component makes
	// its own query. Which one is decided once, at setup.
	const given = isQueryRefs(props.q) ? props.q : undefined;
	const refs =
		given ??
		useView(
			kind,
			() => props.q as string | Preloaded,
			() => props.params,
			{
				...(props.client ? { client: props.client } : {}),
				enabled: () => props.enabled !== false,
			} as never,
		);
	// The `select` prop maps the result; without one, a composable's result
	// shows its own selection.
	const shown = computed(() => {
		const data = refs.data.value;
		if (props.select) return data !== undefined ? props.select(data) : data;
		return given ? given.selected.value : data;
	});

	watch(refs.data, (data) => emit('update:data', data), { immediate: true });
	watch(shown, (value) => emit('update:selected', value), {
		immediate: true,
	});

	return () => {
		if (!given && props.enabled === false) return null;
		const data = refs.data.value;
		const failure = refs.error.value;
		const failed =
			failure !== undefined &&
			(slots.error !== undefined ||
				data === undefined ||
				(Array.isArray(data) && data.length === 0));
		if (failed && failure) {
			if (!slots.error) throw failure;
			return slots.error({ error: failure });
		}
		const stale = refs.stale.value;
		if (refs.status.value === 'pending' && !stale) {
			return slots.loading?.() ?? null;
		}
		if (isEmptyResult(shown.value) && slots.empty) return slots.empty();
		return (
			slots.default?.({
				data: shown.value,
				loading: stale,
				stale,
				error: failure,
			}) ?? null
		);
	};
}
