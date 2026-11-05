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

	Snippets, all optional:

	- `children(result, state)` renders once there is a result. When `params`
	  change, the previous result stays, with `state.stale` set, until the new
	  one lands.
	- `empty` renders instead of `children` when the result is `undefined`,
	  `null` or an empty array.
	- `loading` renders until the first result.
	- `error` renders on failure. Without it, a failure with a result to show
	  renders `children` with `state.error` set, and a failure with none is
	  thrown, so the nearest `<svelte:boundary>` handles it.

	`bind:data` exposes the same result outside the snippets.
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
	import type { QueryViewState } from './state.js';

	type Props = {
		/** The query text, or a payload from `preload`. */
		q: Q | Preloaded<Q>;
		/** The client to use instead of the one `setSurreal` provided. */
		client?: SurrealLike | undefined;
		/** The result, once there is one; for `bind:data`. */
		data?: DataOf<Q> | undefined;
		/** Rendered with the result and its state. */
		children?: Snippet<[DataOf<Q>, QueryViewState]>;
		/** Rendered instead of `children` when the result is empty. */
		empty?: Snippet;
		/** Rendered until the first result. */
		loading?: Snippet;
		/** Rendered on failure. */
		error?: Snippet<[SurrealQueryError]>;
	} & ParamsProp<Q>;

	let {
		q,
		params,
		client,
		data = $bindable(),
		children,
		empty,
		loading,
		error,
	}: Props = $props();

	// The client is read once: a query's client does not change under it.
	// svelte-ignore state_referenced_locally
	const query = handle(
		'query',
		() => q,
		() => params,
		client ? { client } : undefined,
	);

	const current = $derived(query.data as DataOf<Q>);
	const failure = $derived(query.error);
	// A failure with a result still to show renders it, unless an `error`
	// snippet claims it.
	const failed = $derived(
		failure !== undefined && (error !== undefined || current === undefined),
	);
	const ready = $derived(
		!failed && (query.status !== 'pending' || query.stale),
	);
	const isEmpty = $derived(
		current == null || (Array.isArray(current) && current.length === 0),
	);

	$effect.pre(() => {
		data = current;
	});

	$effect(() => {
		if (failed && !error) throw failure;
	});
</script>

{#if failed && failure}
	{@render error?.(failure)}
{:else if ready}
	{#if isEmpty && empty}
		{@render empty()}
	{:else}
		{@render children?.(current, {
			loading: query.stale,
			stale: query.stale,
			error: failure,
		})}
	{/if}
{:else}
	{@render loading?.()}
{/if}
