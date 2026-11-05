<script lang="ts">
	import type { SurrealLike } from 'surrealkit-query';

	import { LiveQuery, setSurreal } from '../../src/index.js';

	type Row = { id: string; name: string };

	let { db }: { db: SurrealLike } = $props();

	// svelte-ignore state_referenced_locally
	setSurreal(db);

	const pick = (rows: unknown[]) =>
		(rows as Row[]).filter((row) => row.name.length <= 3);
	let shown = $state<ReturnType<typeof pick>>([]);
</script>

<p data-testid="shown">{shown.length} shown</p>
<LiveQuery q="SELECT * FROM user" select={pick} bind:selected={shown}>
	{#snippet children(rows)}
		<ul>
			{#each rows as row (row.id)}
				<li>{row.name}</li>
			{/each}
		</ul>
	{/snippet}
</LiveQuery>
