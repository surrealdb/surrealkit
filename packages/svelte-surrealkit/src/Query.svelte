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
<script lang="ts" generics="S extends QueryProp, R = DataOf<TextOf<S>>">
	import type {
		DataOf,
		Preloaded,
		SurrealLike,
		SurrealQueryError,
	} from 'surrealkit-query';
	import type { Snippet } from 'svelte';

	import type { ParamsFor, QueryProp, TextOf } from './props.js';
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
		/** The result, once there is one; for `bind:data`. */
		data?: DataOf<Q> | undefined;
		/**
		 * Maps the result before `children` and the `empty` check. `bind:data`
		 * still gets the result as it came.
		 */
		select?: (data: DataOf<Q>) => R;
		/**
		 * What `select` returned, or the result without one; for
		 * `bind:selected`. Updated with what `children` renders.
		 */
		selected?: R;
		/** Rendered with the result (or what `select` made of it) and its state. */
		children?: Snippet<[R, QueryViewState]>;
		/** Rendered instead of `children` when the result is empty (after `select`). */
		empty?: Snippet;
		/** Rendered until the first result. */
		loading?: Snippet;
		/** Rendered on failure. */
		error?: Snippet<[SurrealQueryError]>;
	};

	type Q = TextOf<S>;

	type Props = Common & { q: S } & ParamsFor<S>;

	let {
		q,
		params,
		client,
		enabled = true,
		data = $bindable(),
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
	const query = isHandle(q)
		? q
		: handle(
				'query',
				() => q as string | Preloaded,
				() => params,
				client
					? { client, enabled: () => enabled }
					: { enabled: () => enabled },
			);
	const active = $derived(isHandle(q) || enabled);

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
	const shown = $derived(
		(select && current !== undefined ? select(current) : current) as R,
	);
	const isEmpty = $derived(
		shown == null || (Array.isArray(shown) && shown.length === 0),
	);

	$effect.pre(() => {
		data = current;
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
			loading: query.stale,
			stale: query.stale,
			error: failure,
		})}
	{/if}
{:else}
	{@render loading?.()}
{/if}
