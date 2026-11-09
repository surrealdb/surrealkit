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

1. Register the generated queries. Set `register = "surrealkit-query"` under `[typegen.queries]` in `surrealkit.toml`, and the generated module does it for you. Without it, add the augmentation yourself in `src/app.d.ts` (see [the core README](../surrealkit-query/README.md#register-the-generated-queries)).

2. Give the browser's client to every query, in the root layout:

   ```svelte
   <!-- src/routes/+layout.svelte -->
   <script lang="ts">
   	import { Surreal } from 'surrealdb';
   	import { setSurreal } from 'svelte-surrealkit';
   	import { browser } from '$app/environment';
   	import { liveQueries } from '$lib/generated/queries';

   	let { children } = $props();
   	const db = new Surreal();
   	if (browser) void db.connect('wss://<your-host>', { namespace: 'app', database: 'app' });
   	setSurreal(db, { live: liveQueries });
   </script>

   {@render children()}
   ```

   The server render does not subscribe, so it does not need a connected client. Connect the browser's client with `reconnect: { attempts: -1 }`: the SDK otherwise stops retrying after five attempts, and live views stay `disconnected` until a reload.

   `liveQueries` holds the engine's facts about each live query, so they subscribe as the engine describes them. See [the core README](../surrealkit-query/README.md#with-the-engines-live-entries).

## Components

`<Query>` runs a query once. `<LiveQuery>` keeps the rows current. They take the same props and snippets, so switching one for the other is a one-word change. The differences are only the ones live and one-shot need:

| | `<Query>` | `<LiveQuery>` |
| --- | --- | --- |
| `q` | Any registered query | A single `SELECT` whose result is an array of rows (others are a type error) |
| `data` before the first result | `undefined` | `[]` |
| Updates | Runs once per query and parameters; `refetch()` on a handle runs it again | Follows every change to the records, by record id or by running the `SELECT` again |
| Handle for `q` | `createQuery` | `createLive` |

| Prop | Purpose |
| --- | --- |
| `q` | The query text (a key of `Queries`), a payload from `preload`, or a handle from `createLive` / `createQuery`. |
| `params` | The query's parameters. Required when the query has a required parameter. |
| `children` | A snippet that gets the result (`<Query>`) or the current rows (`<LiveQuery>`), and a state object (see below). |
| `empty` | A snippet shown instead of `children` when there are no rows (`<Query>`: the result is `undefined`, `null` or an empty array). Without it, `children([])` renders. |
| `loading` | A snippet shown until the first result. |
| `error` | A snippet that gets a `SurrealQueryError`. |
| `select` | A function that maps the rows (or result) before `children` and the `empty` check: a filter, a sort, a grouping. `children` gets its typed return value. |
| `bind:selected` | What `select` returned (or the rows or result, without `select`), updated with what `children` renders: for a "N shown" count outside the component. |
| `bind:data` | The same rows or result, outside the snippets: for a tab count or a derived map. They come from the component's own query, so there is no second live query. |
| `enabled` | While `false`, nothing subscribes and nothing renders. Default `true`. |
| `client` | A client to use instead of the one from `setSurreal`. |

Every snippet is optional. With no `loading` or `error` snippet, nothing renders until there are rows, which is what a `<LiveQuery>` inside a `<select>` needs. When you convert markup that showed "Loading…" through an `{#if}` or a ternary, move that text into a `loading` snippet, or it is no longer shown:

```svelte
<select bind:value={sprint}>
	<LiveQuery q="SELECT id, name FROM sprint ORDER BY name">
		{#snippet children(sprints)}
			{#each sprints as s (String(s.id))}<option value={String(s.id)}>{s.name}</option>{/each}
		{/snippet}
	</LiveQuery>
</select>
```

### Using the rows outside the component

To use the same rows in a component and elsewhere (a tab count, a derived map), make the query once with `createLive` and give its handle to `<LiveQuery>` as `q`. The query text appears once, both uses share one live query, and both are typed:

```svelte
<script lang="ts">
	import { createLive, LiveQuery } from 'svelte-surrealkit';

	const research = createLive("SELECT * FROM card WHERE kind = 'research'");
</script>

<button>Research {research.data.length}</button>
<LiveQuery q={research}>
	{#snippet children(cards)}{#each cards as card (String(card.id))}<p>{card.title}</p>{/each}{/snippet}
</LiveQuery>
```

`createQuery` handles work the same way with `<Query>`. A handle carries its own parameters, so `params` beside it is a type error. Its own client and `enabled` apply too, and the component's are ignored.

`bind:data` also exposes the rows, but the bound variable must be declared with the query's row type, which repeats the query text: `let cards: LiveRowOf<"SELECT …">[] = $state([])`. Svelte cannot infer it from the binding. Prefer the handle.

> NOTE: Keep the query text a literal at the call (`createLive("…")`, `q="…"`). SurrealKit types the queries it finds as literals; it does not follow a constant, so `const Q = "…"` with `q={Q}` is not typed.

### Turning a query off

`enabled={false}` stops the component's query: nothing subscribes, nothing renders, and `bind:data` is `[]` (`<LiveQuery>`) or `undefined` (`<Query>`). Set it back to `true` and the query starts again. The runes take the same option as a getter: `createLive(q, params, { enabled: () => signedIn })`.

### Filtering, sorting and grouping rows

`select` maps the rows before they reach `children`, and `empty` renders when its result is empty. The component still shows `loading` and `error` for the query underneath:

```svelte
<LiveQuery q={signals} select={(rows) => rows.filter((s) => s.source === source)}>
	{#snippet children(shown)}{#each shown as s (String(s.id))}<Signal {s} />{/each}{/snippet}
	{#snippet empty()}<p>No signals from {source}.</p>{/snippet}
	{#snippet loading()}<p>Loading…</p>{/snippet}
</LiveQuery>
```

`select` re-runs when the rows change, or when a value it reads changes (`source` above). `bind:data` still gets the rows as they came.

To use the selected rows outside the component too (a "12 shown" header, counts on filter chips), bind them with `bind:selected`. Define `select` once, and type the bound variable by its return value. Type the function's parameter by the handle's rows, so the query text is not repeated:

```svelte
<script lang="ts">
	import { createLive, LiveQuery } from 'svelte-surrealkit';

	const signals = createLive('SELECT * FROM signal ORDER BY at DESC');
	let source = $state('mail');
	const pick = (rows: typeof signals.data) => rows.filter((s) => s.source === source);
	let shown = $state<ReturnType<typeof pick>>([]);
</script>

<h2>{shown.length} shown</h2>
<LiveQuery q={signals} select={pick} bind:selected={shown}>
	{#snippet children(rows)}{#each rows as s (String(s.id))}<Signal {s} />{/each}{/snippet}
</LiveQuery>
```

`shown` and `children` get the same value on the same update, so the header and the list always agree.

### The state passed to `children`

`children` gets a second argument, `{ loading, stale, error }`:

| Field | Meaning |
| --- | --- |
| `stale` | The rows are the previous parameters' rows. When `params` change, they stay on screen until the new rows land, so you can dim them. |
| `loading` | Newer rows are on their way. Today this is exactly when `stale` is set. |
| `error` | A failure that left rows to show, when there is no `error` snippet. With an `error` snippet, a failure always renders it. With neither rows nor an `error` snippet, the failure is thrown to the nearest `<svelte:boundary>`. |

```svelte
<LiveQuery q="SELECT * FROM card ORDER BY at LIMIT $lim START $start" params={{ lim, start }}>
	{#snippet children(cards, { stale })}
		<ul class:dim={stale}>{#each cards as card (String(card.id))}<li>{card.title}</li>{/each}</ul>
	{/snippet}
</LiveQuery>
```

Rows are never kept across `cacheFor(db).reset()`, so one signed-in user's rows are never shown, stale, to the next.

### Parameters and query text

- `params` are compared by value. A new object literal on every render, such as `params={{ lim, start }}`, runs nothing again until a value changes.
- `q="…"` is a plain attribute, so the SurrealQL inside it cannot contain `"`. Use single-quoted SurrealQL strings (`WHERE kind = 'research'`), or write the text as an expression, `` q={'…'} `` or ``q={`…`}``. SurrealKit extracts all three forms.
- Never interpolate values into `q` (`q="… {x} …"`): Svelte would build a new string, which is not in the registry and is not typed. Pass values through `params`.

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

The handle has `data`, `error`, `status`, `loading`, `stale` and `refetch()`. `createLive`'s `data` is always an array. `stale` works as in the components: when the parameters change, `data` keeps the previous result until the new one lands.

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

## Signing in

Connect both the browser and the server as the signed-in user, with record access (`DEFINE ACCESS … TYPE RECORD`). Table permissions then apply to every query, including live queries.

1. Sign in on the server, in a form action: `db.signin({ namespace, database, access, variables })` returns the user's access token. Store it in an `httpOnly` cookie.
2. In `hooks.server.ts`, connect a client for each request with that token (`authentication: token`), put it on `locals`, and close it after the request.
3. Give the token to the page from `+layout.server.ts`. In `+layout.svelte`, connect the browser's client with it and call `setSurreal(db)`.
4. When the user signs in or out, call `db.authenticate(token)` or `db.invalidate()` in the browser, then `cacheFor(db).reset()`, so no rows from the previous user stay on screen.

> WARNING: Root, namespace and database credentials must never reach the browser. Do not put them in `PUBLIC_*` or `VITE_*` variables. A browser client signed in as root can read and change everything.

[examples/svelte-live](../../examples/svelte-live) implements this pattern.

## Live queries and pagination

Only some `SELECT` clauses can be live. A query with a clause that is not live, such as `ORDER BY`, `LIMIT` or `START`, runs its `SELECT` again on each change. The [surrealkit-query README](../surrealkit-query/README.md#which-clauses-are-live) has the full table and the lifecycle.

## Development

```bash
bun install
bun run test         # component tests, against a mocked engine
bun run typecheck    # svelte-check, then the type goldens in test/types
bun run build
```
