<script lang="ts">
	import type { SurrealLike } from 'surrealkit-query';

	import { LiveQuery, setSurreal } from '../../src/index.js';

	type Row = { id: string; name: string };

	let { db, letter }: { db: SurrealLike; letter: string } = $props();

	// svelte-ignore state_referenced_locally
	setSurreal(db);

	const starting = (rows: unknown[], first: string) =>
		(rows as Row[]).filter((row) => row.name.startsWith(first));
</script>

<LiveQuery q="SELECT * FROM user" select={(rows) => starting(rows, letter)}>
	{#snippet children(rows)}
		<ul>
			{#each rows as row (row.id)}
				<li>{row.name}</li>
			{/each}
		</ul>
	{/snippet}
	{#snippet empty()}
		<p>nobody starts with {letter}</p>
	{/snippet}
</LiveQuery>
