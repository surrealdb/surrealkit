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
<script lang="ts" generics="Q extends QueryText">
	import type {
		LiveRowOf,
		LiveText,
		ParamsProp,
		Preloaded,
		QueryText,
		SurrealLike,
		SurrealQueryError,
	} from 'surrealkit-query';
	import type { Snippet } from 'svelte';

	import { handle } from './runes.svelte.js';
	import type { QueryViewState } from './state.js';

	type Props = {
		/** The SELECT to make live, or a payload from `preload`. */
		q: LiveText<Q> | Preloaded<Q>;
		/** The client to use instead of the one `setSurreal` provided. */
		client?: SurrealLike | undefined;
		/** The current rows, `[]` until the first snapshot; for `bind:rows`. */
		rows?: LiveRowOf<Q>[];
		/** Rendered with the current rows and their state. */
		children?: Snippet<[LiveRowOf<Q>[], QueryViewState]>;
		/** Rendered instead of `children` when there are no rows. */
		empty?: Snippet;
		/** Rendered until the first snapshot. */
		loading?: Snippet;
		/** Rendered on failure. */
		error?: Snippet<[SurrealQueryError]>;
	} & ParamsProp<Q>;

	let {
		q,
		params,
		client,
		rows = $bindable(),
		children,
		empty,
		loading,
		error,
	}: Props = $props();

	// The client is read once: a query's client does not change under it.
	// svelte-ignore state_referenced_locally
	const live = handle(
		'live',
		() => q,
		() => params,
		client ? { client } : undefined,
	);

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

	$effect.pre(() => {
		rows = current;
	});

	$effect(() => {
		if (failed && !error) throw failure;
	});
</script>

{#if failed && failure}
	{@render error?.(failure)}
{:else if ready}
	{#if current.length === 0 && empty}
		{@render empty()}
	{:else}
		{@render children?.(current, {
			loading: live.stale,
			stale: live.stale,
			error: failure,
		})}
	{/if}
{:else}
	{@render loading?.()}
{/if}
