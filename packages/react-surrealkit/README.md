# react-surrealkit

Use `react-surrealkit` to show typed SurrealQL queries and live queries in React and Next.js. The query text types its rows and parameters, from the module that `surreal kit compile` generates.

```tsx
'use client';
import { useLive } from 'react-surrealkit';

export function People({ start }: { start: number }) {
	const people = useLive('SELECT * FROM person ORDER BY name LIMIT $lim START $start', { lim: 10, start });
	return <ul>{people.data.map((p) => <li key={String(p.id)}>{p.name}</li>)}</ul>;
}
```

When `start` changes, the old live query is killed and the next page opens. When the component unmounts, its live query is killed.

## Install

```bash
npm install react-surrealkit surrealdb
```

Peer dependencies: `react` 18.3 or later, and `surrealdb` 2.1.1 or later.

## Set up

1. Register the generated queries. Set `register = "surrealkit-query"` under `[typegen.queries]` in `surrealkit.toml`, and the generated module does it for you. Without it, add the augmentation yourself in a `.d.ts` file the compiler includes (see [the core README](../surrealkit-query/README.md#register-the-generated-queries)).

2. Give the browser's client to every hook:

   ```tsx
   'use client';
   import { SurrealProvider } from 'react-surrealkit';
   import { db } from './db';
   import { liveQueries } from './generated/queries';

   export function Providers({ children }: { children: React.ReactNode }) {
   	return (
   		<SurrealProvider client={db} live={liveQueries}>
   			{children}
   		</SurrealProvider>
   	);
   }
   ```

   `liveQueries` holds the engine's facts about each live query, so they subscribe as the engine describes them. See [the core README](../surrealkit-query/README.md#with-the-engines-live-entries).

## Hooks

| Hook | Result |
| --- | --- |
| `useQuery(q, params?, options?)` | Runs the query once. `data` is the query's result. |
| `useLive(q, params?, options?)` | Keeps the rows current. `data` is always an array. |

Both return `data`, `error`, `status`, `loading`, `stale` and `refetch()`. When the parameters change, `data` keeps the previous result, with `stale: true`, until the new one lands, so you can dim it. A result is never kept across `cacheFor(db).reset()`. `params` is required when the query has a required parameter. `options.client` overrides the provider's client. While `options.enabled` is `false`, nothing subscribes: `data` is `[]` (`useLive`) or `undefined` (`useQuery`).

The hooks follow parameters by value: a new object with equal values does not run the query again. Hooks that show the same query and parameters share one live query. React's strict mode does not open a second one.

## Next.js App Router

Run the query in a Server Component with `preload` from `react-surrealkit/server`, and give the payload to a client component. You write the query text once:

```tsx
// app/people/page.tsx
import { preload } from 'react-surrealkit/server';
import { getDb } from '@/lib/db.server';
import { People } from './people';

export default async function Page() {
	const people = await preload(await getDb(), 'SELECT * FROM person ORDER BY name LIMIT $lim START $start', {
		lim: 10,
		start: 0,
	});
	return <People people={people} />;
}
```

```tsx
// app/people/people.tsx
'use client';
import { type Preloaded, useLive } from 'react-surrealkit';

type Page = 'SELECT * FROM person ORDER BY name LIMIT $lim START $start';

export function People({ people }: { people: Preloaded<Page> }) {
	const live = useLive(people);
	return <ul>{live.data.map((p) => <li key={String(p.id)}>{p.name}</li>)}</ul>;
}
```

The first render uses the payload. In the browser the hook opens its live query. The payload holds plain strings, so it crosses the Server Component boundary, and SDK values such as `RecordId` arrive as themselves.

> WARNING: On the server, use a client for each request (for example, React's `cache()` around the connect). A module-level server client is shared by every user.

## Signing in

Connect both the browser and the server as the signed-in user, with record access (`DEFINE ACCESS … TYPE RECORD`). Table permissions then apply to every query, including live queries.

1. Sign in on the server, in a server action or route handler: `db.signin({ namespace, database, access, variables })` returns the user's access token. Store it in an `httpOnly` cookie.
2. In Server Components, connect a client for each request with that token (`authentication: token`), for example in a `cache()`-wrapped `getDb()`.
3. Pass the token to the client provider as a prop. Connect the browser's client with it and give the client to `<SurrealProvider>`.
4. When the user signs in or out, call `db.authenticate(token)` or `db.invalidate()` in the browser, then `cacheFor(db).reset()`, so no rows from the previous user stay on screen.

> WARNING: Root, namespace and database credentials must never reach the browser. Do not put them in `NEXT_PUBLIC_*` variables. A browser client signed in as root can read and change everything.

## Live queries and pagination

Only some `SELECT` clauses can be live. A query with a clause that is not live, such as `ORDER BY`, `LIMIT` or `START`, runs its `SELECT` again on each change. The [surrealkit-query README](../surrealkit-query/README.md#which-clauses-are-live) has the full table and the lifecycle.

## Development

```bash
bun install
bun run test         # hook tests, against a mocked engine
bun run typecheck    # includes the type goldens in test/types
bun run build
```
