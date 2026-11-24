---
name: effect-surrealkit
description: "Use in a TypeScript project that depends on effect-surrealkit (an effect-surrealkit dependency, a [typegen.emit.*] command naming it in surrealkit.toml, or a contentMappers entry naming it): writing or changing a SurrealDB statement built from the generated descriptors or authored in a .surql file, running `effect-surrealkit statements`, importing a .surql file, handling an error from `run` or a transaction, reading as a record user, paging, or testing statements on the in-memory engine."
---

# effect-surrealkit statements

effect-surrealkit turns a SurrealKit schema document into Effect modules, builds typed SurrealDB statements from them or from `.surql` files, and runs and tests them. Every rule below cites where it comes from: a section of the package README (`packages/effect-surrealkit/README.md`), a conformance test under `packages/effect-surrealkit/test/`, or SurrealDB's documentation. The engine versions the package pins are SurrealDB 3.3.4 and 4.0.1. Read a reference only when the task needs it:

- [references/generating.md](references/generating.md): the `statements` command, what `--schema` checks, `--verify`, and the content mapper.
- [references/errors.md](references/errors.md): which error `run` fails with, what was written, and duplicates.
- [references/testing.md](references/testing.md): the testing exports and what each proves.

## End to end

Generate the descriptors, generate the authored statements with the schema, build, run and test:

```sh
effect-surrealkit --input schema.json --out src/generated/surreal --store app
effect-surrealkit statements --input queries --out src/generated/statements --schema schema.json --verify
```

```ts
import { Effect } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { layer, run } from 'effect-surrealkit/client';
import { create, param, select } from 'effect-surrealkit/statement';
import { expectIndex, memoryLayer } from 'effect-surrealkit/testing';
import { PersonCreate, PersonTable } from './generated/surreal/index.js';

// From the descriptor, named <slice>.<operation>, every value bound.
const byEmail = select(PersonTable, 'uniq_email', {
  email: param('email', SurrealSchema.String),
}).build('people.byEmail');
// SELECT * FROM ONLY ⟨person⟩ WITH INDEX uniq_email WHERE email = $email LIMIT 1;
const register = create(PersonTable).content(param('person', PersonCreate)).build('people.register');

// A taken email is a typed error whose index is the literal 'uniq_email'.
const signUp = (person: PersonCreate) =>
  run(register, { person }).pipe(
    Effect.catchTag('StatementIndexConflict', (error) => Effect.succeed({ taken: error.index })),
  );
const live = signUp(ada).pipe(Effect.provide(layer(db)));

// The same code on a fresh in-memory engine with the schema loaded.
const test = Effect.gen(function* () {
  yield* signUp(ada);
  yield* expectIndex(byEmail, { email: ada.email }, 'uniq_email');
  return yield* run(byEmail, { email: ada.email });
}).pipe(Effect.provide(memoryLayer({ schema })));
```

## Writing statements

- Build every statement you can from the generated descriptors (`select(PersonTable)`, `create`, `update`, `upsert`, `insert`, `relate`): the types come from the descriptors alone, and a misspelt field, a wrong binding type or a missing binding is a compile error. (README, "Statements"; "Reads and writes")
- Name each statement `<slice>.<operation>` (`people.byEmail`), the form the `statements` command gives a part (`accounts.bindOwner`). The name is the run's span and metric label, and pinning writes `billing.openInvoices` to `billing/openInvoices.surql`. (README, "Statements from .surql files"; "Running statements"; "Pinning built statements")
- Never put an input value into statement text. Bind it with `param(name, codec)`, and build a record id from bound key parts with `recordId(table, key)`, which renders `type::record`: SurrealDB 3.3 and 4.0 parse no binding inside a record-id literal (`person:$k`). Where SurrealQL takes no binding (`SHOW CHANGES SINCE $since`), there is no builder; do not template the value in. (README, "Reads and writes"; "What the statements do not know"; `test/conformance.test.ts`, "record ids and ranges from bound key parts")
- Read one record by its record id, `select(table, recordId(table, key)).only()`, which SurrealDB reads by key, never by a table scan. Look a row up by a unique index with `select(table, index, values)`, which adds `WITH INDEX` because SurrealDB 3.3 and 4.0 otherwise plan such a lookup through any index on a prefix of its fields. (README, "Reads and writes"; `test/conformance.test.ts`, "reads the records by key, never by a table scan"; "the index a lookup reads")
- Page with an id range, `select(table, idRange(table, { after })).limit(rows)`, starting after the last id read, never with `START`, and without `ORDER BY id`: a range is returned in id order and read by key, while `START` scans past every skipped row and skips a row after a delete. (README, "Paging through a table"; `test/conformance.test.ts`, "paging: an id range after the last id read, not START"; https://surrealdb.com/docs/surrealql/statements/select, "Record ranges", read 2026-11-23)
- For a restricted session, build from `readAs(table)` (and `readAs(store)` for links, steps and `FETCH`): SurrealDB returns NONE for a field the session may not read and leaves out a row it may not see, without an error. (README, "What the statements do not know"; `test/conformance.test.ts`, "permissions, as a record user")
- A value that may already be taken: use SurrealDB's own statements first, `upsert(table, id)`, `INSERT IGNORE` in authored text, and `insert(...).onDuplicate(...)` (`ON DUPLICATE KEY UPDATE`). (README, "A value that may already be taken"; https://surrealdb.com/docs/surrealql/statements/insert, read 2026-11-23)
- Where a duplicate is a domain error, catch `StatementIndexConflict` (its `index` typed as the statement's unique indexes) and `StatementRecordExists`. Both are pinned on SurrealDB 3.3.4 and 4.0.1 only; on another version a duplicate may arrive as `StatementRequestError`, so run `checkConstraintErrors` against it first. Authored text declares the indexes it can duplicate with `uniqueIndexes`. (README, "A value that may already be taken"; "Authored SurrealQL"; `test/conformance.test.ts`, "constraint errors: a duplicate on a unique index and a record that exists")
- Declare `writes: false` (`.build(name, { writes: false })`, `part`, `make`) only for a statement known to read: it makes a decode failure a plain `StatementDecodeError` that says nothing was written. Leave it out when unsure. (README, "Running statements")
- Keep a multi-statement script in SurrealQL: a `.surql` file, or `part` and `sequence(...).define({ params, result })` with exactly the names the script still needs. SurrealDB reads a parameter nothing bound as NONE without an error, so the contract is what catches a misspelt binding. (README, "Authored SurrealQL"; `test/conformance.test.ts`, "parameters")

## Generating

- Generate authored statements with `effect-surrealkit statements --input <dir> --out <dir>`; each scope directory becomes `<out>/<scope>/statements.ts`. Never edit a generated file: regenerate it. (README, "Statements from .surql files")
- Pass `--schema <file>` with the schema document. The table and function names check, the projected-field check and the binding checks (`binds`, `rejectsNone`) all need it; without it a misspelt field or a wrong binding kind generates silently. (README, "Statements from .surql files"; "What bindings are used as"; "Projected fields")
- Run `--check` in CI: it writes nothing and exits 1 when the output is missing, stale or edited. (README, "Statements from .surql files")
- Add `--verify` to send each complete text once, with no bindings, to a discarded in-memory engine, so a parse error or a wrong result count fails generation; `--verify-schema <file>` loads schema files first. It needs `@surrealdb/node`, and the texts are executed. (README, "Checking texts on an engine: --verify"; `test/verify.test.ts`; `test/analyze-engine.test.ts`)
- The content mapper is the second supported way to the same modules: `.surql` imports type-checked by `tsc --noEmit --runExternalCode` from TypeScript 7.1, with one `contentMappers` entry written in the checked `tsconfig.json` whose `input` is the command's `--input`. Details in [references/generating.md](references/generating.md). (README, "Importing .surql files: the content mapper")

## Running

- Provide the client with `layer(db)`; statements run with that connection's or session's authentication. Run one statement with `run`, an atomic batch with `transaction([bind(statement, input), ...])`, and separate results in one request with `pipeline`. (README, "Running statements")
- An interrupted or timed-out write may have been applied, and a `StatementWriteOutcomeError` means the request succeeded and its writes are committed: read the data back before sending a write again. (README, "Running statements")
- `{ retry: schedule }` resends only on `StatementConflict`. (README, "Running statements")

## Testing

- Test on `memoryLayer({ schema })` from `effect-surrealkit/testing`, a fresh in-process engine that needs the dev dependencies `@surrealdb/node` and `@surrealdb/node-native`. (README, "Testing")
- Prove a lookup's plan with `expectIndex(statement, input, index)` or `expectNoIndex(statement, input, index)`, and a restricted session's view with `recordUser({ access, variables })`. (README, "Testing"; `test/testing.test.ts`, "EXPLAIN"; "recordUser")
- Before relying on the typed duplicate errors on a new engine version, run `checkConstraintErrors` against a disposable database there. (README, "Testing"; `test/testing.test.ts`, "checkConstraintErrors")
- Without an engine, `fake(script)` is the real client over a scripted transport, so it refuses what the real one refuses. (README, "Testing")

## Supported versions

- Node 22.15 or later and `effect` 4.1.5 or later, by the owner's ruling of 2026-11-23: the install line names `effect@4.1.5`, and the `.surql` loaders need Node 22.15 for `module.registerHooks` and `module.stripTypeScriptTypes`. SurrealDB 3.3 and 4.0, pinned on 3.3.4 and 4.0.1. (README, "effect-surrealkit"; "Importing .surql files: the content mapper"; "Running statements")
