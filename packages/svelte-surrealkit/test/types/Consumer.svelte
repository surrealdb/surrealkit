<!--
	Type goldens for the components and runes, against a module `surreal kit
	compile` wrote. A marker comment names an error svelte-check must report
	on the next line; golden.mjs fails on a missing or an unmarked error.
-->
<script lang="ts">
	import type { RecordId } from 'surrealdb';

	import {
		createLive,
		createQuery,
		LiveQuery,
		type Preloaded,
		Query,
	} from '../../src/index.js';
	import './register.js';

	const BY_NAME =
		'SELECT name, email FROM user WHERE name = $name LIMIT $limit';
	let name = $state('ada');
	let limit = $state(10);

	const byName = createQuery(BY_NAME, () => ({ name, limit }));
	byName.data satisfies Array<{ email: string; name: string }> | undefined;
	// @ts-expect-error a required parameter is missing
	createQuery(BY_NAME);
	// @ts-expect-error a parameter has the wrong type
	createQuery(BY_NAME, () => ({ name: 1 }));
	createQuery('SELECT kind FROM event');

	const events = createLive('SELECT * FROM event');
	events.data satisfies Array<{ id: RecordId<'event'>; kind: string }>;
	// @ts-expect-error FROM ONLY is not a row stream
	createLive('SELECT * FROM ONLY user:alice');

	const untyped = createLive('SELECT * FROM elsewhere');
	untyped.data satisfies unknown[];

	let { posts }: { posts: Preloaded<'SELECT kind FROM event'> } = $props();
	createLive(() => posts).data satisfies Array<{ kind: string }>;
</script>

<Query q={BY_NAME} params={{ name, limit }}>
	{#snippet children(rows)}
		{#each rows as row (row.email)}
			{row.name satisfies string}
		{/each}
	{/snippet}
</Query>
<Query q="SELECT * FROM ONLY user:alice">
	{#snippet children(alice)}
		{alice?.id satisfies RecordId<'user'> | undefined}
	{/snippet}
</Query>
<LiveQuery
	q="SELECT * FROM post WHERE published = $published AND author = $author"
	params={{ published: true, author: undefined }}
>
	{#snippet children(rows)}
		{rows.map((row) => row.title satisfies string)}
	{/snippet}
</LiveQuery>
<LiveQuery q={posts}>
	{#snippet children(rows)}
		{rows.map((row) => row.kind satisfies string)}
	{/snippet}
</LiveQuery>
<LiveQuery q="SELECT * FROM elsewhere">
	{#snippet children(rows)}
		{rows satisfies unknown[]}
	{/snippet}
</LiveQuery>

<!-- expect-error: Property 'params' is missing -->
<Query q={BY_NAME}>
	{#snippet children(rows)}
		{rows.length}
	{/snippet}
</Query>
<!-- expect-error: Type 'number' is not assignable to type 'string' -->
<Query q={BY_NAME} params={{ name: 1 }}>
	{#snippet children(rows)}
		{rows.length}
	{/snippet}
</Query>
<Query q="SELECT kind FROM event">
	{#snippet children(rows)}
		<!-- expect-error: Property 'nope' does not exist -->
		{rows.map((row) => row.nope)}
	{/snippet}
</Query>
<!-- expect-error: LiveTextError -->
<LiveQuery q="SELECT * FROM ONLY user:alice">
	{#snippet children(rows)}
		{rows.length}
	{/snippet}
</LiveQuery>
<!-- expect-error: Type 'number' is not assignable to type 'never' -->
<LiveQuery q="SELECT kind FROM event" params={{ stray: 1 }}>
	{#snippet children(rows)}
		{rows.length}
	{/snippet}
</LiveQuery>
