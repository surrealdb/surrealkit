<script lang="ts">
	import type { SurrealLike } from 'surrealkit-query';

	import { LiveQuery, setSurreal } from '../../src/index.js';

	type Row = { id: string; name: string };

	let {
		db,
		q,
		params,
		copies = 1,
	}: {
		db: SurrealLike;
		q: string;
		params?: Record<string, unknown>;
		copies?: number;
	} = $props();

	// svelte-ignore state_referenced_locally
	setSurreal(db);

	const asRows = (rows: unknown[]) => rows as Row[];
</script>

{#each Array.from({ length: copies }, (_, i) => i) as copy (copy)}
	<LiveQuery {q} {params}>
		{#snippet children(rows)}
			<ul data-copy={copy}>
				{#each asRows(rows) as row (row.id)}
					<li>{row.name}</li>
				{/each}
			</ul>
		{/snippet}
		{#snippet loading()}
			<p>loading</p>
		{/snippet}
		{#snippet error(failure)}
			<p>error: {failure.code}</p>
		{/snippet}
	</LiveQuery>
{/each}
