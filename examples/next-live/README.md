# next-live

This Next.js App Router app shows typed queries and live queries with `react-surrealkit`. It is a task list:

- The first paint is rendered on the server.
- The list goes live in the browser.
- Server actions write, and every open tab follows the change.
- A search box drives a one-shot query by its parameters.

Every query's types come from `surreal kit compile`. Nothing is typed by hand.

## Run it

You need Node.js 20.19 or later, Bun, a `surreal` binary, and a SurrealKit binary whose engine types queries (SurrealDB 4.1).

1. Build the packages:

   ```bash
   (cd ../../packages/surrealkit-query && bun install && bun run build)
   (cd ../../packages/react-surrealkit && bun install && bun run build)
   ```

2. Install the app:

   ```bash
   npm install --install-links --no-package-lock
   ```

   `--install-links` copies the two packages into `node_modules` instead of linking them, so the app has one copy of React. Run it again after you rebuild a package.

3. Start an in-memory database in a separate terminal:

   ```bash
   npm run db
   ```

   It allows guest connections, which the browser uses. To use another port, start `surreal` yourself with `--allow-guests`, and set `SURREAL_URL` for the seed and `NEXT_PUBLIC_SURREAL_URL` for the app, before you build it.

4. Apply the schema and add some tasks:

   ```bash
   npm run seed
   ```

5. Build and start the app, and open `http://localhost:3000`:

   ```bash
   npm run build && npm start
   ```

## How it fits together

| Piece | Where | What it does |
| --- | --- | --- |
| Schema | `database/schema/` | `person`, `task` (a `"todo" \| "doing" \| "done"` status and an owner link) and the `blocks` relation. Every table is readable by guests and writable only by the server. |
| Generated types | `src/generated/queries.ts` | `npm run types` writes it from the schema and every query in `src`. `register = "surrealkit-query"` makes it register itself with the bindings. |
| Request client | `src/lib/db.server.ts` | `cache()` gives each request its own client, signed in with server-only credentials. `after()` closes it once the response is sent. |
| First paint | `src/app/page.tsx`, `src/lib/tasks.server.ts` | `preload` runs the task query on the server. Its return type is the client component's prop type, so the text is written once. |
| Live list | `src/components/Board.tsx` | `useLive(payload)` shows the server's rows at once, then follows changes. The summary reads the hook, and `<LiveQuery q={result}>` renders the list from the same subscription, with `loading`, `empty` and `error`. |
| Writes | `src/app/actions.ts` | Server actions create, move and delete tasks. They return nothing: the change reaches the page through its live query. |
| Search | `src/components/Search.tsx` | `<Query>` with `params` (the text), `enabled` (only when there is text) and `select` (the status filter). |
| Browser client | `src/app/providers.tsx` | A guest connection that reconnects for as long as the page is open, with the engine's `liveQueries`. |
| Type errors | `src/type-errors.tsx` | `@ts-expect-error` cases that `npm run typecheck` must reject. |

> WARNING: Root credentials stay on the server (`SURREAL_USER` and `SURREAL_PASS`, read by `db.server.ts` and the seed). The browser connects as a guest, and the schema lets guests only read.

## What to try

- **Two tabs.** Add, move or delete a task in one tab. The other follows without a reload.
- **Search.** Type `the`, then pick a status. The text runs the query again; the status filter does not. `Generate the types` shows the task it blocks.
- **Stop the database.** The list shows its error and the summary keeps the last counts. Start the database again (and seed it, since it is in memory): the list comes back on its own.
- **Break a type.** In `Board.tsx`, read `task.titel`, or pass `params` to `<LiveQuery q={all}>`, and run `npm run typecheck`.

## Checks

```bash
npm run types:check   # the generated module is up to date (needs SURREALKIT_BIN)
npm run typecheck     # tsc, including src/type-errors.tsx
npm run build
```
