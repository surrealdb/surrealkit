<!--
	@component
	A live query, rendered from its current rows.

	```svelte
	<LiveQuery q="SELECT * FROM user ORDER BY name LIMIT $lim START $start" params={{ lim, start }}>
		{#snippet children(users)}
			{#each users as user (user.id)}<p>{user.name}</p>{/each}
		{/snippet}
	</LiveQuery>
	```

	`q` is a plain SELECT that is a key of the generated `Queries` registry, or
	a `Preloaded` payload; the component makes it live. Mounting subscribes,
	unmounting kills the live query, and new `params` kill it and subscribe
	again. Components showing the same query and parameters share one live
	query.

	Snippets, all optional:

	- `children(rows, state)` renders with the rows once the first snapshot
	  arrives. When `params` change, the previous rows stay, with
	  `state.stale` set, until the new ones land.
	- `empty` renders instead of `children` when there are no rows. Without
	  it, `children([])` renders.
	- `loading` renders until the first snapshot.
	- `error` renders on failure. Without it, a failure with rows to show
	  renders `children` with `state.error` set, and a failure with none is
	  thrown, so the nearest `<svelte:boundary>` handles it.

	With no `loading` or `error` snippet, nothing renders until there are rows,
	which is what a `<LiveQuery>` inside a `<select>` needs.

	`bind:rows` exposes the same rows outside the snippets (a tab count, a
	derived map), from the same live query.
-->
<script lang="ts" generics="S extends LiveProp, R = LiveRowOf<TextOf<S>>[]">
	import type {
		LiveRowOf,
		Preloaded,
		SurrealLike,
		SurrealQueryError,
	} from 'surrealkit-query';
	import type { Snippet } from 'svelte';

	import type { LiveGuard, LiveProp, ParamsFor, TextOf } from './props.js';
	import { handle, isHandle } from './runes.svelte.js';
	import type { QueryViewState } from './state.js';

	type Common = {
		/** The client to use instead of the one `setSurreal` provided. */
		client?: SurrealLike | undefined;
		/**
		 * While `false`, nothing subscribes and nothing renders; the bound
		 * value is `[]` (`<LiveQuery>`) or `undefined` (`<Query>`). Default
		 * `true`. Ignored when `q` is a handle, which has its own `enabled`.
		 */
		enabled?: boolean;
		/** The current rows, `[]` until the first snapshot; for `bind:rows`. */
		rows?: LiveRowOf<Q>[];
		/**
		 * Maps the rows before `children` and the `empty` check: a filter, a
		 * sort, a grouping. `bind:rows` still gets the rows as they came.
		 */
		select?: (rows: LiveRowOf<Q>[]) => R;
		/**
		 * What `select` returned, or the rows without one; for
		 * `bind:selected`. Updated with what `children` renders.
		 */
		selected?: R;
		/** Rendered with the rows (or what `select` made of them) and their state. */
		children?: Snippet<[R, QueryViewState]>;
		/** Rendered instead of `children` when there are no rows (after `select`). */
		empty?: Snippet;
		/** Rendered until the first snapshot. */
		loading?: Snippet;
		/** Rendered on failure. */
		error?: Snippet<[SurrealQueryError]>;
	};

	type Q = TextOf<S>;

	type Props = Common & { q: S & LiveGuard<S> } & ParamsFor<S>;

	let {
		q,
		params,
		client,
		enabled = true,
		rows = $bindable(),
		selected = $bindable(),
		select,
		children,
		empty,
		loading,
		error,
	}: Props = $props();

	// A handle is shown as it is; otherwise this component makes its own.
	// The client is read once: a query's client does not change under it.
	// svelte-ignore state_referenced_locally
	const live = isHandle(q)
		? q
		: handle(
				'live',
				() => q as string | Preloaded,
				() => params,
				client
					? { client, enabled: () => enabled }
					: { enabled: () => enabled },
			);
	const active = $derived(isHandle(q) || enabled);

	const current = $derived(live.data as LiveRowOf<Q>[]);
	const failure = $derived(live.error);
	// A failure with rows still to show renders them, unless an `error` snippet
	// claims it.
	const failed = $derived(
		failure !== undefined && (error !== undefined || current.length === 0),
	);
	const ready = $derived(
		!failed && (live.status !== 'pending' || live.stale),
	);

	const shown = $derived((select ? select(current) : current) as R);
	const isEmpty = $derived(
		shown == null || (Array.isArray(shown) && shown.length === 0),
	);

	$effect.pre(() => {
		rows = current;
		selected = shown;
	});

	$effect(() => {
		if (failed && !error) throw failure;
	});
</script>

{#if !active}
<!-- Disabled: nothing subscribes, nothing renders. -->
{:else if failed && failure}
	{@render error?.(failure)}
{:else if ready}
	{#if isEmpty && empty}
		{@render empty()}
	{:else}
		{@render children?.(shown, {
			loading: live.stale,
			stale: live.stale,
			error: failure,
		})}
	{/if}
{:else}
	{@render loading?.()}
{/if}
