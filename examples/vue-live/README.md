# vue-live

This Vite and Vue 3 app shows typed queries and live queries with `vue-surrealkit`. It is the task list from [next-live](../next-live), on the same schema:

- A live list follows creates, updates and deletes from any tab.
- A search drives a one-shot query by its parameters.
- The list shows an error while the database is down, and recovers by itself.

Every query's types come from `surreal kit compile`. Nothing is typed by hand.

## Run it

You need Node.js 20.19 or later, Bun, a `surreal` binary, and a SurrealKit binary whose engine types queries (SurrealDB 4.1).

1. Build the packages:

   ```bash
   (cd ../../packages/surrealkit-query && bun install && bun run build)
   (cd ../../packages/vue-surrealkit && bun install && bun run build)
   ```

2. Install the app. `--install-links` copies the two packages into `node_modules`, so the app has one copy of Vue. Run it again after you rebuild a package:

   ```bash
   npm install --install-links --no-package-lock
   ```

3. Start an in-memory database that allows guests, in a separate terminal:

   ```bash
   npm run db
   ```

4. Apply the schema and add some tasks:

   ```bash
   npm run seed
   ```

5. Start the app and open the URL it prints:

   ```bash
   npm run dev
   ```

To use another database port, set `SURREAL_URL` for the seed and the server routes, and `VITE_SURREAL_URL` for the browser.

## How it fits together

| Piece | Where | What it does |
| --- | --- | --- |
| Schema | `database/schema/` | `person`, `task` (a `"todo" \| "doing" \| "done"` status and an owner link) and the `blocks` relation. Guests may read; only the server writes. |
| Generated types | `src/generated/queries.ts` | `npm run types` writes it from the schema and every query in `src` and `server`. `register = "surrealkit-query"` makes it register itself with the bindings. |
| Browser client | `src/main.ts` | A guest connection that reconnects for as long as the page is open, installed with `app.use(surrealkit, { client, live })`. |
| Live list | `src/App.vue` | `useLive` gives the summary its counts. `<LiveQuery :q="all">` renders the list from the same subscription, with `loading`, `empty` and `error` slots. |
| Writes | `server/api.ts`, `src/api.ts` | `/api/tasks` routes on Vite's dev and preview servers create, move and delete tasks with server-only credentials. They return nothing: changes reach the page by its live query. |
| Owners | `src/components/NewTask.vue` | A `<LiveQuery>` that renders `<option>`s inside a `<select>`. |
| Search | `src/components/Search.vue` | `useQuery` with `params` (the text), `enabled` (only when there is text) and `select` (the status filter). The heading counts its `selected`; `<Query :q="search">` renders it. |
| Type errors | `src/TypeErrors.vue` | `@ts-expect-error` and `@vue-expect-error` cases that `npm run typecheck` must reject. |

> WARNING: Root credentials stay on the server (`SURREAL_USER` and `SURREAL_PASS`, read by `server/api.ts` and the seed). The browser connects as a guest, and the schema lets guests only read.

## What to try

- **Two tabs.** Add, move or delete a task in one tab. The other follows without a reload.
- **Search.** Type `the`, then pick a status. The text runs the query again; the status filter does not, and the count follows the filter.
- **Stop the database.** The list shows its error slot and the summary keeps the last counts. Start it again (and seed it, since it is in memory): the list comes back by itself.

## Checks

```bash
npm run types:check   # the generated module is up to date (needs SURREALKIT_BIN)
npm run typecheck     # vue-tsc and tsc, including src/TypeErrors.vue
npm run build
```
