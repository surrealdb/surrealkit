# Testing statements

Every statement here cites the package README (`packages/effect-surrealkit/README.md`) or a test under `packages/effect-surrealkit/test/`.

The memory database needs the optional peer `@surrealdb/node` and its peer `@surrealdb/node-native` as dev dependencies; it is imported only when a memory database starts. (README, "Testing")

| Export of `effect-surrealkit/testing` | Use it to |
| --- | --- |
| `memoryLayer({ schema })` | run the code under test on a fresh in-process, in-memory engine with the schema text loaded; `memory(options)` is the same as a scoped effect |
| `layerFrom(connection, { namespace, database })` | do the same over a connection a test already has |
| `recordUser({ access, variables })` | sign a record user up through `DEFINE ACCESS … TYPE RECORD` and get a client running as that session, to see row and field permissions |
| `expectIndex(statement, input, index)` | prove that the engine's plan of a `SELECT` reads `index`; `expectNoIndex(statement, input, index)` proves it does not, and `explain(statement, input)` returns the plan |
| `schemaDrift(store)` | list the differences between a store registry and what the database defines |
| `checkConstraintErrors` | check, on a disposable database, that a duplicate and an existing id still fail as `StatementIndexConflict` and `StatementRecordExists` on this engine version |
| `verifyStatements({ scopes, schema })` | run what `statements --verify` runs, in process |
| `fake(script)`, `fakeLayer(script)` | answer each statement by name without an engine, with `result(value)`, `thrown(message)`, `conflict()`, `failure(message)`, `indexConflict(index)` and `recordExists()` |

(README, "Testing")

- `expectIndex` runs the statement with `EXPLAIN` appended, which plans it without running it; SurrealDB 3.3 and 4.0 answer with the same plan format. (README, "Testing"; `test/testing.test.ts`, "EXPLAIN")
- `recordUser` is scoped: provide `Effect.scoped` around the test. (README, "Testing"; `test/testing.test.ts`, "recordUser")
- The scripted client is the real client over a scripted transport: it refuses a binding its codec refuses and decodes a canned result with the statement's codec, so a wrong shape fails as it would against an engine. A statement the script does not name fails as a request. (README, "Testing"; `test/fake.test.ts`)
- To run a suite on SurrealDB 3.3 as well, point it at a server the suite starts itself or at a throwaway container, as this package's suite does with `SURREAL_BIN` and `SURREAL_URL`. (README, "Development")
