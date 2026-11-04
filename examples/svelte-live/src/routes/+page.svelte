<script lang="ts">
	import { RecordId, Surreal } from 'surrealdb';
	import { createLive, getSurreal, LiveQuery, Query } from 'svelte-surrealkit';

	import type { Typed } from '$lib/generated/queries';

	let { data } = $props();

	const PAGE_SIZE = 5;
	let start = $state(0);
	let team = $state('red');
	let name = $state('');

	const db = getSurreal() as Typed<Surreal>;

	// GROUP ALL is not a live clause either, so this count re-runs on each
	// change to the table. It starts from the server's count.
	const total = createLive(() => data.count);
	const count = $derived(total.data[0]?.total ?? 0);

	async function add() {
		if (!name.trim()) return;
		await db.query('CREATE person SET name = $name, team = $team', { name: name.trim(), team });
		name = '';
	}

	async function remove(id: RecordId<'person'>) {
		await db.query('DELETE $id', { id });
	}
</script>

<main>
	<h1>People</h1>
	<form method="POST" action="?/signout"><button type="submit">Sign out</button></form>

	<form onsubmit={(event) => (event.preventDefault(), add())}>
		<input bind:value={name} placeholder="name" aria-label="name" />
		<select bind:value={team} aria-label="team">
			<option>red</option>
			<option>blue</option>
			<option>green</option>
		</select>
		<button type="submit">Add</button>
	</form>

	<h2>Everyone, a page at a time</h2>
	<!--
		A live window: LIMIT and START are not live clauses, so the component
		watches the table and re-runs this SELECT on each change. The first page
		comes from the server; paging kills that live query and opens the next.
	-->
	<LiveQuery q={data.people} params={{ lim: PAGE_SIZE, start }}>
		{#snippet children(people)}
			<ul>
				{#each people as person (String(person.id))}
					<li>
						{person.name} <small>({person.team})</small>
						<button type="button" onclick={() => remove(person.id)}>remove</button>
					</li>
				{/each}
			</ul>
		{/snippet}
		{#snippet loading()}<p>Loading…</p>{/snippet}
		{#snippet error(failure)}<p role="alert">{failure.message}</p>{/snippet}
	</LiveQuery>
	<nav>
		<button type="button" disabled={start === 0} onclick={() => (start -= PAGE_SIZE)}>Previous</button>
		<span>{start + 1}–{Math.min(start + PAGE_SIZE, count)} of {count}</span>
		<button type="button" disabled={start + PAGE_SIZE >= count} onclick={() => (start += PAGE_SIZE)}>Next</button>
	</nav>

	<h2>Team {team}</h2>
	<!-- The same team picker drives this query's $team. -->
	<LiveQuery q="SELECT id, name FROM person WHERE team = $team" params={{ team }}>
		{#snippet children(members)}
			<p>{members.map((member) => member.name).join(', ') || 'Nobody yet.'}</p>
		{/snippet}
	</LiveQuery>

	<h2>Once, not live</h2>
	<Query q="SELECT count() AS total FROM person GROUP ALL">
		{#snippet children(rows)}<p>{rows[0]?.total ?? 0} people when this rendered.</p>{/snippet}
	</Query>
</main>
