<!--
	@component
	A one-shot query, rendered from its result.

	```svelte
	<Query q="SELECT * FROM user LIMIT $lim START $start" params={{ lim, start }}>
		{#snippet children(users)}
			{#each users as user (user.id)}<p>{user.name}</p>{/each}
		{/snippet}
		{#snippet loading()}<p>Loading…</p>{/snippet}
		{#snippet error(failure)}<p>{failure.message}</p>{/snippet}
	</Query>
	```

	`q` is a key of the generated `Queries` registry, or a `Preloaded` payload
	from a `load` function. It types the `children` argument and `params`,
	which is required when the query has a required parameter. The query runs
	again when `params` changes to different values.

	`children` renders once there is a result, `loading` until then, and
	`error` on failure. With no `error` snippet a failure is thrown, so the
	nearest `<svelte:boundary>` handles it.
-->
<script lang="ts" generics="Q extends QueryText">
	import type {
		DataOf,
		ParamsProp,
		Preloaded,
		QueryText,
		SurrealLike,
		SurrealQueryError,
	} from 'surrealkit-query';
	import type { Snippet } from 'svelte';

	import { handle } from './runes.svelte.js';

	type Props = {
		/** The query text, or a payload from `preload`. */
		q: Q | Preloaded<Q>;
		/** The client to use instead of the one `setSurreal` provided. */
		client?: SurrealLike | undefined;
		/** Rendered with the result. */
		children: Snippet<[DataOf<Q>]>;
		/** Rendered until the first result. */
		loading?: Snippet;
		/** Rendered on failure. */
		error?: Snippet<[SurrealQueryError]>;
	} & ParamsProp<Q>;

	let { q, params, client, children, loading, error }: Props = $props();

	// The client is read once: a query's client does not change under it.
	// svelte-ignore state_referenced_locally
	const query = handle(
		'query',
		() => q,
		() => params,
		client ? { client } : undefined,
	);

	$effect(() => {
		if (query.error && !error) throw query.error;
	});
</script>

{#if query.status === 'error' && query.error}
	{@render error?.(query.error)}
{:else if query.status === 'success'}
	{@render children(query.data as DataOf<Q>)}
{:else}
	{@render loading?.()}
{/if}
