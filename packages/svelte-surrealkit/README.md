# svelte-surrealkit

Use `svelte-surrealkit` to show typed SurrealQL queries and live queries in Svelte 5 and SvelteKit. The query text types its rows and parameters, from the module that `surreal kit compile` generates.

```svelte
<LiveQuery q="SELECT * FROM user ORDER BY name LIMIT $lim START $start" params={{ lim, start }}>
	{#snippet children(users)}
		{#each users as user (String(user.id))}<p>{user.name}</p>{/each}
	{/snippet}
</LiveQuery>
```

When `lim` or `start` changes, the old live query is killed and the next page opens. When the component is destroyed, its live query is killed.

## Install

```bash
npm install svelte-surrealkit surrealdb
```

Peer dependencies: `svelte` 5.7 or later, and `surrealdb` 2.1.1 or later.

## Set up

1. Register the generated queries in `src/app.d.ts`:

   ```ts
   import type { Queries } from '$lib/generated/queries';

   declare module 'surrealkit-query' {
   	interface Register {
   		queries: Queries;
   	}
   }
   ```

2. Give the browser's client to every query, in the root layout:

   ```svelte
   <!-- src/routes/+layout.svelte -->
   <script lang="ts">
   	import { Surreal } from 'surrealdb';
   	import { setSurreal } from 'svelte-surrealkit';
   	import { browser } from '$app/environment';

   	let { children } = $props();
   	const db = new Surreal();
   	if (browser) void db.connect('wss://<your-host>', { namespace: 'app', database: 'app' });
   	setSurreal(db);
   </script>

   {@render children()}
   ```

   The server render does not subscribe, so it does not need a connected client.

   When the generated module exports `liveQueries` (SurrealDB 4.1 and later), pass it: `setSurreal(db, { live: liveQueries })`. Live queries then subscribe as the engine describes them. See [the core README](../surrealkit-query/README.md#with-the-engines-live-entries).

## Components

`<Query>` runs a query once. `<LiveQuery>` keeps the rows current.

| Prop | Purpose |
| --- | --- |
| `q` | The query text (a key of `Queries`), or a payload from `preload`. |
| `params` | The query's parameters. Required when the query has a required parameter. |
| `children` | A snippet that gets the result (`<Query>`) or the current rows (`<LiveQuery>`). |
| `loading` | A snippet shown until the first result. |
| `error` | A snippet that gets a `SurrealQueryError`. Without it, the error is thrown to the nearest `<svelte:boundary>`. |
| `client` | A client to use instead of the one from `setSurreal`. |

A `<LiveQuery>` with an empty result renders `children([])`, not `loading`.

## Runes

`createQuery` and `createLive` give the same queries as reactive state. Give the parameters as a getter, so the query follows `$state` and `$derived` values:

```svelte
<script lang="ts">
	import { createLive } from 'svelte-surrealkit';

	let team = $state('red');
	const members = createLive('SELECT id, name FROM person WHERE team = $team', () => ({ team }));
</script>

{#if members.loading}<p>Loading…</p>{/if}
{#each members.data as member (String(member.id))}<p>{member.name}</p>{/each}
```

The handle has `data`, `error`, `status`, `loading` and `refetch()`. `createLive`'s `data` is always an array.

The query starts the first time the handle is read in an effect (markup counts), and stops when nothing reads it. This works in a component and in a shared `.svelte.ts` module. Outside a component, pass `{ client }` as the third argument.

## Server rendering in SvelteKit

Run the query in `load` with `preload`, and give the payload to the component. You write the query text once:

```ts
// src/routes/+page.server.ts
import { preload } from 'svelte-surrealkit';

export const load = async ({ locals }) => ({
	people: await preload(locals.db, 'SELECT * FROM person ORDER BY name LIMIT $lim START $start', {
		lim: 10,
		start: 0,
	}),
});
```

```svelte
<!-- src/routes/+page.svelte -->
<LiveQuery q={data.people} params={{ lim: 10, start }}>
	{#snippet children(people)}…{/snippet}
</LiveQuery>
```

The first page renders from the payload. In the browser the component opens its live query. When `start` changes, the component leaves the payload and opens the next page.

> WARNING: Use a client for each request in `load` (for example, one made in `hooks.server.ts` and put on `locals`), and close it after the request. A module-level server client is shared by every user.

## Live queries and pagination

Only some `SELECT` clauses can be live. A query with a clause that is not live, such as `ORDER BY`, `LIMIT` or `START`, runs its `SELECT` again on each change. The [surrealkit-query README](../surrealkit-query/README.md#which-clauses-are-live) has the full table and the lifecycle.

## Development

```bash
bun install
bun run test         # component tests, against a mocked engine
bun run typecheck    # svelte-check, then the type goldens in test/types
bun run build
```
