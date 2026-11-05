<script lang="ts">
	import type { SurrealLike } from 'surrealkit-query';

	import { createLive, LiveQuery } from '../../src/index.js';

	type Row = { id: string; name: string };

	let { db }: { db: SurrealLike } = $props();

	// svelte-ignore state_referenced_locally
	const users = createLive('SELECT * FROM user', undefined, { client: db });
	const asRows = (list: unknown[]) => list as Row[];
</script>

<p data-testid="count">{users.data.length}</p>
<LiveQuery q={users}>
	{#snippet children(list)}
		<ul>
			{#each asRows(list) as row (row.id)}
				<li>{row.name}</li>
			{/each}
		</ul>
	{/snippet}
</LiveQuery>
