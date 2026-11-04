# svelte-live

This SvelteKit app shows typed queries and live queries with `svelte-surrealkit`. Users sign in with record access. The app renders the first page and the count on the server, pages through a live window, follows a team filter, and keeps the count live.

The typed-query module in `src/lib/generated/queries.ts` is written by hand, in the shape `surreal kit compile` generates. The app runs on any SurrealDB server, because types come from that module and not from the server. When the engine types queries (SurrealDB 4.1), `npm run types` generates the same module from the queries in `src`.

## Run it

You need Node.js 20.19 or later, Bun, and a `surreal` binary.

1. Build the packages:

   ```bash
   (cd ../../packages/surrealkit-query && bun install && bun run build)
   (cd ../../packages/svelte-surrealkit && bun install && bun run build)
   ```

2. Install the app:

   ```bash
   npm install --no-package-lock
   ```

3. Start an in-memory database on port 8000 in a separate terminal:

   ```bash
   npm run db
   ```

   To use another port, start `surreal` yourself, and set `SURREAL_URL` for the seed and `VITE_SURREAL_URL` for the app (for example `ws://127.0.0.1:8001`).

4. Apply the schema and add some people. The seed script signs in as root; the app never does:

   ```bash
   npm run seed
   ```

5. Start the app and open `http://localhost:5173`:

   ```bash
   npm run dev
   ```

6. Select **Sign up** with any email and password. These are users of the local database only.

## How sign-in works

The browser and the server both connect as the signed-in user, with the record access in `database/schema/account.surql`. Table permissions then apply to every query, including live queries.

1. The login form posts to the server (`src/routes/login/+page.server.ts`). The server signs up or signs in with the record access and gets the user's access token.
2. The server stores the token in an `httpOnly` cookie.
3. For each request, `src/hooks.server.ts` connects a new client with that token and closes it after the request. `load` functions use it through `locals.db`.
4. `src/routes/+layout.server.ts` gives the token to the page. The browser connects its own client with it (`src/routes/+layout.svelte`) and calls `setSurreal(db)`.
5. When the token changes (sign in or sign out), the layout authenticates or invalidates the browser session. It then calls `cacheFor(db).reset()`, so no rows from the previous user stay on screen.

> WARNING: Root, namespace and database credentials stay on the server, in tooling such as the seed script. Never put them in `PUBLIC_*` or `VITE_*` variables, and never give them to `connect()` in the browser. A browser client signed in as root can read and change everything.

The browser must have the token to authenticate its connection, so page scripts can read it. Keep the token short-lived (`DURATION FOR TOKEN`). The cookie is `httpOnly`, so it is not readable from `document.cookie`.

## What to try

- **Page through the list.** **Next** and **Previous** change `$start`, and the page's `SELECT` runs again with the new value. The window, the team list and the count are all window queries on `person`, so they share one `LIVE SELECT * FROM person`, which stays open while any of them is shown.
- **Write from somewhere else.** Run a `CREATE`, `UPDATE` or `DELETE` on `person` in `surreal sql` (namespace `demo`, database `demo`). The page, the team list and the count change without a reload.
- **Move someone to another team.** The team list drops them. The engine sends no notification for a record that leaves a live `WHERE`, so the binding runs the `SELECT` again on each change to the table.
- **Sign out.** The page goes back to the login form, and the cache is reset.
- **Check the types.** Run `npm run check`. Change a parameter name in `src/routes/+page.svelte`, or read a field the query does not select, and the check fails.

## Where things are

| File | Purpose |
| --- | --- |
| `src/app.d.ts` | Registers the generated `Queries`, and types `locals`. |
| `src/hooks.server.ts` | Connects a client for each request, as its user. |
| `src/routes/login/+page.server.ts` | Signs up and signs in with record access. |
| `src/routes/+layout.server.ts` | Gives the user's token to the browser. |
| `src/routes/+layout.svelte` | Connects the browser's client as the user, and calls `setSurreal`. |
| `src/routes/+page.server.ts` | Preloads the first page and the count. |
| `src/routes/+page.svelte` | `<LiveQuery>`, `createLive` (seeded from the count) and `<Query>`. |
| `src/lib/generated/queries.ts` | The typed-query module. |
| `database/schema/` | The schema and the record access that `npm run seed` applies. |
