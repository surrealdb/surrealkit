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
		type DataOf,
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

	let kinds: Array<{ kind: string }> = $state([]);
	const eventKinds = createLive('SELECT kind FROM event');
	const pickKinds = (rows: Array<{ kind: string }>) =>
		rows.map((row) => row.kind);
	let pickedKinds = $state<ReturnType<typeof pickKinds>>([]);
	let wrongPicked: number[] = $state([]);
	const byNameLive = createLive(BY_NAME, () => ({ name, limit }));
	const byNameOnce = createQuery(BY_NAME, () => ({ name, limit }));
	let wrong: number[] = $state([]);
	let alice: DataOf<'SELECT * FROM ONLY user:alice'> = $state();
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
<LiveQuery q="SELECT kind FROM event" bind:rows={kinds}>
	{#snippet children(rows, state)}
		{rows.length}{state.stale satisfies boolean}{state.error?.code}
	{/snippet}
	{#snippet empty()}
		nothing
	{/snippet}
</LiveQuery>
<LiveQuery q="SELECT kind FROM event" bind:rows={kinds} />
<Query q="SELECT * FROM ONLY user:alice" bind:data={alice} />
<!-- expect-error: Type 'number[]' is not assignable to type '{ kind: string; }[]' -->
<LiveQuery q="SELECT kind FROM event" bind:rows={wrong} />
<LiveQuery q={eventKinds}>
	{#snippet children(rows)}
		{rows.map((row) => row.kind satisfies string)}
	{/snippet}
</LiveQuery>
<LiveQuery q="SELECT kind FROM event" enabled={false} />
<LiveQuery q={eventKinds}>
	{#snippet children(rows)}
		<!-- expect-error: Property 'nope' does not exist -->
		{rows.map((row) => row.nope)}
	{/snippet}
</LiveQuery>
<LiveQuery q={byNameLive}>
	{#snippet children(rows)}
		{rows.map((row) => row.email satisfies string)}
	{/snippet}
</LiveQuery>
<Query q={byNameOnce}>
	{#snippet children(rows)}
		{rows.map((row) => row.email satisfies string)}
	{/snippet}
</Query>
<!-- expect-error: is not assignable to type 'undefined' -->
<LiveQuery q={byNameLive} params={{ name, limit }} />
<!-- expect-error: is not assignable to type 'undefined' -->
<Query q={byNameOnce} params={{ name, limit }} />
<LiveQuery
	q="SELECT kind FROM event"
	select={(rows) => rows.map((row) => row.kind)}
>
	{#snippet children(kinds)}
		{kinds satisfies string[]}
	{/snippet}
	{#snippet empty()}
		none
	{/snippet}
</LiveQuery>
<Query q={BY_NAME} params={{ name, limit }} select={(rows) => rows.length}>
	{#snippet children(count)}
		{count satisfies number}
	{/snippet}
</Query>
<!-- expect-error: Property 'nope' does not exist -->
<LiveQuery
	q="SELECT kind FROM event"
	select={(rows) => rows.filter((row) => row.nope)}
/>
<LiveQuery
	q="SELECT kind FROM event"
	select={pickKinds}
	bind:selected={pickedKinds}
/>
<!-- expect-error: Type 'number[]' is not assignable to type 'string[]' -->
<LiveQuery
	q="SELECT kind FROM event"
	select={pickKinds}
	bind:selected={wrongPicked}
/>
