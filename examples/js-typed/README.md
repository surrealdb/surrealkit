# Plain JavaScript, typed

`db.query("…")` in a `.js` file, typed by SurrealKit with no TypeScript source: a `// @ts-check`
(or `checkJs`) project gets each query's result type from a declarations file.

```text
database/schema/001_people.surql   the schema
surrealkit.toml                    [typegen.queries] typescript = "src/queries.d.ts"
src/app.js                         typed code: `db.query(…)` returns the rows' types
src/queries.d.ts                   the types, and `liveQueries` as `export declare const` (generated)
src/queries.js                     the `liveQueries` value, typed by the declarations (generated)
src/bad.js                         a wrong field; the typecheck fails on this file and only on it
```

A `typescript` path ending in `.d.ts` is what asks for the pair. `surrealkit compile` writes both
files; `surrealkit compile --check` fails when they are stale. Typing the queries needs
SurrealDB 4.1.

```bash
npm install --no-package-lock
npm run generate     # surrealkit compile
npm run typecheck    # tsc -p jsconfig.json --noEmit: reports bad.js, nothing else
```

`npm run typecheck` is meant to fail, on `src/bad.js`:

```text
src/bad.js(10,39): error TS2339: Property 'nick' does not exist on type '{ age?: number | undefined; email: string; name: string; }'.
```

The crate's `tests/js_example.rs` runs this check without `npm install`, when `SURREALKIT_TSC` is
a `tsc` binary and `SURREALKIT_TS_SDK` is the `surrealdb` package directory:

```bash
SURREALKIT_TSC=…/typescript/bin/tsc SURREALKIT_TS_SDK=…/node_modules/surrealdb \
  cargo test -p surrealkit --test js_example -- --ignored
```
