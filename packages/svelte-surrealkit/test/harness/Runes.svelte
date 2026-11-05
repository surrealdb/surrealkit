<script lang="ts">
	import type { SurrealLike } from 'surrealkit-query';

	import { createLive } from '../../src/index.js';

	type Row = { id: string; name: string };

	let { db }: { db: SurrealLike } = $props();

	let lim = $state(2);
	let start = $state(0);
	// svelte-ignore state_referenced_locally
	const users = createLive(
		'SELECT * FROM user ORDER BY name LIMIT $lim START $start',
		() => ({ lim, start }),
		{ client: db },
	);
	const rows = $derived(users.data as Row[]);
</script>

<button type="button" onclick={() => (start += 2)}>next</button>
<button type="button" onclick={() => (lim = lim)}>same</button>
<p>{users.status}</p>
<ul>
	{#each rows as row (row.id)}
		<li>{row.name}</li>
	{/each}
</ul>
