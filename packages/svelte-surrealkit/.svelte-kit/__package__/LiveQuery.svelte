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

	`children` renders with the rows once the first snapshot arrives (an empty
	result renders `children([])`), `loading` until then, and `error` on
	failure. With no `error` snippet a failure is thrown, so the nearest
	`<svelte:boundary>` handles it.
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

	type Props = {
		/** The SELECT to make live, or a payload from `preload`. */
		q: LiveText<Q> | Preloaded<Q>;
		/** The client to use instead of the one `setSurreal` provided. */
		client?: SurrealLike | undefined;
		/** Rendered with the current rows. */
		children: Snippet<[LiveRowOf<Q>[]]>;
		/** Rendered until the first snapshot. */
		loading?: Snippet;
		/** Rendered on failure. */
		error?: Snippet<[SurrealQueryError]>;
	} & ParamsProp<Q>;

	let { q, params, client, children, loading, error }: Props = $props();

	// The client is read once: a query's client does not change under it.
	// svelte-ignore state_referenced_locally
	const live = handle(
		'live',
		() => q,
		() => params,
		client ? { client } : undefined,
	);

	$effect(() => {
		if (live.error && !error) throw live.error;
	});
</script>

{#if live.status === 'error' && live.error}
	{@render error?.(live.error)}
{:else if live.status === 'success'}
	{@render children(live.data as LiveRowOf<Q>[])}
{:else}
	{@render loading?.()}
{/if}
