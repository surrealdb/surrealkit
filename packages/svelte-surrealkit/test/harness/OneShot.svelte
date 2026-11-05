<script lang="ts">
	import type { Preloaded, SurrealLike } from 'surrealkit-query';

	import { Query, setSurreal } from '../../src/index.js';

	let {
		db,
		q,
		params,
	}: {
		db: SurrealLike;
		q: string | Preloaded;
		params?: Record<string, unknown>;
	} = $props();

	// svelte-ignore state_referenced_locally
	setSurreal(db);
</script>

<Query {q} {params}>
	{#snippet children(result)}
		<pre>{JSON.stringify(result)}</pre>
	{/snippet}
	{#snippet loading()}
		<p>loading</p>
	{/snippet}
	{#snippet error(failure)}
		<p>error: {failure.message}</p>
	{/snippet}
</Query>
