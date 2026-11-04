# surrealkit-query

Use `surrealkit-query` to run typed SurrealQL queries and live queries from a shared cache. It is the core of the framework bindings:

- **[svelte-surrealkit](../svelte-surrealkit):** `<Query>`, `<LiveQuery>`, `createQuery` and `createLive` for Svelte 5 and SvelteKit.
- **[react-surrealkit](../react-surrealkit):** `useQuery` and `useLive` for React and Next.js.

Most apps use a binding and never call this package directly. Read this page for the type setup and for how live queries behave.

## Requirements

- The `surrealdb` JavaScript SDK, version 2.1.1 or later.
- A query module that `surreal kit compile` generates (the `[typegen.queries] typescript` setting). It exports `Queries`: every query the project sends, keyed by its exact text.

## Register the generated queries

Add this once, in any `.ts` or `.d.ts` file that the compiler includes (in SvelteKit, `src/app.d.ts`):

```ts
import type { Queries } from '$lib/generated/queries';

declare module 'surrealkit-query' {
	interface Register {
		queries: Queries;
	}
}
```

Every binding reads this registration. After it:

- Query text that is a key of `Queries` gives typed rows and typed parameters.
- A missing parameter, a parameter of the wrong type, or a parameter the query does not take is a compile error.
- Query text that is not in `Queries` is untyped: rows are `unknown`.

To make unknown query text a compile error, add `strict: true` to `Register`.

When the engine types live queries, the generated module also exports `LiveQueries` (a type) and `liveQueries` (the same entries as a value). Register the type, and pass the value to the bindings:

```ts
declare module 'surrealkit-query' {
	interface Register {
		queries: Queries;
		liveQueries: LiveQueries;
	}
}
```

```ts
setSurreal(db, { live: liveQueries });          // svelte-surrealkit
<SurrealProvider client={db} live={liveQueries}> // react-surrealkit
```

A live query that the engine refuses, where running the `SELECT` again cannot serve the refused clause, is then a compile error. Examples are `FROM ONLY` and several targets.

> NOTE: The module augmentation must name `surrealkit-query`, also when you import the bindings only.

## How a live query works

You give a live query a plain `SELECT`. The binding makes it live. When the first component shows it:

1. One request sends `LIVE SELECT …` and the `SELECT` together. The live query is registered before the rows are read, so no change falls between them.
2. Each change notification updates the rows, in one of two ways (see the next section).
3. Components that show the same query text and parameters share one live query.

The binding then manages the live query:

| Event | Result |
| --- | --- |
| The last component that shows the query is destroyed | The live query is killed. |
| The parameters change to different values | A keyed query: the old live query is killed and a new one opens. A window query: its watcher stays (see below) and the `SELECT` runs with the new values. |
| The parameters change to equal values | Nothing happens. |
| The SDK reconnects after a dropped connection | Each shown live query opens again and reads its rows again. |
| The engine kills the live query | The query goes to its error state, with the code `killed`. |
| A live query resolves after its component was destroyed | It is killed at once. |

A query that is not shown keeps its last rows for five minutes (`gcTime`), so it renders at once if it is shown again.

## Which clauses are live

`LIVE SELECT` accepts a projection, one target, `WHERE` and `FETCH`. It refuses every other `SELECT` clause. The binding uses two modes:

| Mode | When | How rows change |
| --- | --- | --- |
| **Keyed** | The `SELECT` has only a projection, one target and `FETCH`. | Each notification is applied by record id: `CREATE` adds the row, `UPDATE` replaces it, `DELETE` removes it. |
| **Window** | The `SELECT` has `WHERE`, `ORDER BY`, `LIMIT`, `START`, `GROUP`, `SPLIT`, `OMIT`, `VALUE`, `WITH`, `TIMEOUT`, `PARALLEL`, `TEMPFILES` or `EXPLAIN`. | A `LIVE SELECT * FROM <target>` watches the target. Each notification runs the `SELECT` again. |

Window queries share their watcher. All window queries on one connection with the same watch text and the same values for the parameters it reads use one `LIVE SELECT`. For example, a page of `user`, a filtered list of `user` and a count of `user` share one `LIVE SELECT * FROM user`, and each runs its own `SELECT` again on a notification. The watcher is killed when the last of them is destroyed.

In window mode the rows are always the ones the engine computes, so pagination works: `SELECT * FROM user ORDER BY name LIMIT $lim START $start` stays correct when a record moves into or out of the page. A burst of notifications runs the `SELECT` at most twice: once, and once more if a change arrived during the first run. Rows that did not change keep their object identity, so keyed `{#each}` blocks and React keys render only what changed.

A keyed query whose rows have no `id` (for example `SELECT name FROM user`) also runs the `SELECT` again on each notification, because there is no id to apply a notification to.

`WHERE` is in window mode because the engine does not notify a record that an update moves out of the filter (SurrealDB 3.3). If you apply notifications directly, that record stays in the rows. Set `trustWhere: true` on the cache to apply `WHERE` notifications by id when your engine sends them, or when no update moves a record out of the filter.

Both modes watch only the records of the target. A change to any other record does not update the rows, even when the query reads it. This includes a record that `FETCH` joins, a record that a graph traversal (`->knows->person`) reaches, and a record that a subquery reads. A notification for the target's own record does carry the current fetched values.

### With the engine's live entries

When the bindings have `liveQueries`, the engine's entry for a query decides its mode, not the text:

| Engine entry | Mode | Subscription |
| --- | --- | --- |
| No refusal, no `WHERE` | Keyed | The entry's `query`, as written. |
| No refusal, `exit: "delete"` | Keyed | The entry's `query`, as written. |
| No refusal, `exit: "silent"` | Window | `LIVE SELECT * FROM <target>`, because a record leaving the filter is not notified. |
| `refusal.windowable: true`, `exit` absent or `"delete"` | Window | The entry's `query`, as written. |
| `refusal.windowable: true`, `exit: "silent"` | Window | `LIVE SELECT * FROM <target>`. |
| `refusal.windowable: false` | Not live | None. The types reject it, and at run time it is an `unsupported` error. |

`exit` is what the engine sends when an update moves a record out of the `WHERE`. The engine checks it on its own build. `trustWhere` applies only to queries without an entry.

These queries cannot be live, and go to the error state with the code `unsupported`:

- More than one statement.
- `SELECT … FROM ONLY …`.
- More than one target (`FROM a, b`).
- A statement that is not a `SELECT`.

The types reject the first two cases: a live query must have one array of rows as its result.

## Live row types

A live row has the type of the `SELECT`'s row. The engine will also type each notification (its action and row, or its patch operations for `DIFF`), keyed by the same query text. The bindings will use those types when the generated module includes them, with no change to your code. `DIFF` live queries are not supported yet.

## Server rendering

`preload(db, text, params)` runs a query on the server and returns a payload. Give the payload to a component, rune or hook as `q`:

- It renders the server's result at once.
- It is typed from the registry by its text, so you write the query once.
- A live query still opens its live query in the browser and reads its rows again, so no change is missed.

The payload holds plain strings. SDK values (`RecordId`, `DateTime`, and the other value classes) are CBOR-encoded, so they arrive as themselves through SvelteKit's `load` and through React Server Component props.

> WARNING: On the server, use a client for each request. A module-level client is shared by every request and every user, and so is its session.

## After a sign-in

A cache key holds the query text and parameters, but not the signed-in identity. After `signin`, `signup`, `authenticate` or `invalidate`, call `cacheFor(db).reset()`. It removes what the previous identity saw, and opens each shown query again under the new one.

## API

| Export | Purpose |
| --- | --- |
| `cacheFor(db)` | The cache for a client. There is one cache for each client. |
| `QueryCache` | `observe`, `observeLive`, `fetch`, `hydrate`, `invalidate`, `reset`, `clear`, `dispose`, `stats`. |
| `preload(db, text, params)` | A typed server payload. |
| `planLive(text, options)` | How a `SELECT` is made live (keyed, window or unsupported). |
| `SurrealQueryError` | A failed query: `code`, `query`, `params`, `cause`. |
| `Register`, `QueryText`, `DataOf`, `ParamsOf`, `LiveRowOf`, `Preloaded` | The registry types. |

## Development

```bash
bun install
bun run test         # unit tests, against a mocked engine
bun run typecheck    # includes type goldens against a generated module
SURREAL_BIN=$(which surreal) bun run test   # also runs the engine suite
```
