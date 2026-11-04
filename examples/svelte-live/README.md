# svelte-live

This SvelteKit app shows typed queries and live queries with `svelte-surrealkit`. It renders the first page on the server, pages through a live window, follows a team filter, and keeps a live count.

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

3. Start an in-memory database in a separate terminal. It accepts connections without credentials, so use it for local development only:

   ```bash
   npm run db
   ```

4. Apply the schema and add some people:

   ```bash
   npm run seed
   ```

5. Start the app and open `http://localhost:5173`:

   ```bash
   npm run dev
   ```

## What to try

- **Page through the list.** **Next** and **Previous** change `$start`. The old live query is killed and the next page opens.
- **Write from somewhere else.** Run a `CREATE`, `UPDATE` or `DELETE` on `person` in `surreal sql` (namespace `demo`, database `demo`). The page, the team list and the count change without a reload.
- **Move someone to another team.** The team list drops them. The engine sends no notification for a record that leaves a live `WHERE`, so the binding runs the `SELECT` again on each change to the table.
- **Check the types.** Run `npm run check`. Change a parameter name in `src/routes/+page.svelte`, or read a field the query does not select, and the check fails.

## Where things are

| File | Purpose |
| --- | --- |
| `src/app.d.ts` | Registers the generated `Queries` with the bindings. |
| `src/routes/+layout.svelte` | Connects the browser's client and calls `setSurreal`. |
| `src/routes/+page.server.ts` | Preloads the first page with a client for the request. |
| `src/routes/+page.svelte` | `<LiveQuery>`, `createLive` and `<Query>`. |
| `src/lib/generated/queries.ts` | The typed-query module. |
| `database/schema/person.surql` | The schema `npm run seed` applies. |
