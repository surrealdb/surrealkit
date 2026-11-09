<script lang="ts">
	import type { SurrealLike } from 'surrealkit-query';

	import { LiveQuery, setSurreal } from '../../src/index.js';

	type Row = { id: string; name: string };

	let {
		db,
		q,
		params,
	}: {
		db: SurrealLike;
		q: string;
		params?: Record<string, unknown>;
	} = $props();

	// svelte-ignore state_referenced_locally
	setSurreal(db);

	let rows: unknown[] = $state([]);
	const asRows = (list: unknown[]) => list as Row[];
</script>

<p data-testid="count">{rows.length}</p>
<LiveQuery {q} {params} bind:data={rows} />
<LiveQuery {q} {params}>
	{#snippet children(list, state)}
		<ul data-stale={String(state.stale)}>
			{#each asRows(list) as row (row.id)}
				<li>{row.name}</li>
			{/each}
		</ul>
	{/snippet}
	{#snippet empty()}
		<p>nobody</p>
	{/snippet}
</LiveQuery>
