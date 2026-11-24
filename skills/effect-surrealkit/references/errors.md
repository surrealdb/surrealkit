# Errors from running a statement

Every statement here cites the package README (`packages/effect-surrealkit/README.md`), a test under `packages/effect-surrealkit/test/`, or SurrealDB's documentation.

## Which error, and was anything written

| Error | When | Written? |
| --- | --- | --- |
| `StatementEncodeError` | a binding failed its codec or is not a value the SDK sends without loss | no, nothing was sent |
| `StatementThrown` | a `THROW` whose message the statement declares | not by a transaction |
| `StatementUnexpectedThrow` | a `THROW` whose message it does not declare | as above |
| `StatementConflict` | a transaction conflict that `retry` did not resolve | rolled back |
| `StatementIndexConflict` | a duplicate on a unique index the statement declares, named by `index` | not by that statement |
| `StatementRecordExists` | a `CREATE` or `INSERT` of a record id that exists | not by that statement |
| `StatementRequestError` | any other failure: transport, authentication, another SurrealQL error | unknown for a transport failure, otherwise no |
| `StatementEmptyResult` | a `create` or `relate` returned nothing; the session may lack permission | not by that statement |
| `StatementDecodeError` | the answer of a statement known not to write does not decode | no |
| `StatementWriteOutcomeError` | the same, for a statement not known to be read-only | yes: read back before resending |

Error messages, default inspection and JSON output never show raw server diagnostics or the duplicated value and record id of a constraint error; read `cause` directly and privately when those are needed. (README, "Running statements")

## A value that may already be taken

Lead with what SurrealDB does natively. (README, "A value that may already be taken"; https://surrealdb.com/docs/surrealql/statements/insert, read 2026-11-23)

1. `upsert(table, id)` changes the record when it is there and creates it when it is not; `upsert(table).where(...)` does the same for the record a condition finds.
2. `insert(table, rows).onDuplicate(...)` is `ON DUPLICATE KEY UPDATE`: it updates the record a row collides with, by id or by a unique index.
3. `INSERT IGNORE` skips a colliding row and inserts the others. No builder writes it; use authored text (`part`, `make`).

A write that expects the value to be free fails with a typed error. Each write builder declares its table's unique indexes, so `StatementIndexConflict`'s `index` is the union of their names; `deleteFrom` declares none, and authored text declares them with `uniqueIndexes` on `make` or `sequence(...).define`. (README, "A value that may already be taken"; "Authored SurrealQL")

On SurrealDB 3.3.4 and 4.0.1, as pinned by `test/conformance.test.ts`, "constraint errors: a duplicate on a unique index and a record that exists":

- `INSERT IGNORE` raises neither error.
- `ON DUPLICATE KEY UPDATE` still raises `StatementIndexConflict` for a taken unique value under another explicit id, and for a value its own assignments make a duplicate.
- `RELATE` onto an existing edge id stays a `StatementRequestError`.

What the typed duplicate does not claim (README, "Running statements"):

- The duplicate is read from the engine's message, pinned on 3.3.4 and 4.0.1 only. On another version it may be a `StatementRequestError`; `checkConstraintErrors` tells.
- The message names the index and not its table.
- A duplicate raised inside an event, or inside a `block` or a `family`, is a `StatementRequestError`.
- A record that exists is read from SurrealDB's structured error, not from a message.

## `writes`

A statement's `writes` is `true` (write builders, a text the analyzer finds a write in), absent (unknown: a `select` with a condition, authored text with no write found, `make` by default) or `false` (a `select` of stored fields only, or declared). Only `false` makes a result that does not decode a `StatementDecodeError`; otherwise it is a `StatementWriteOutcomeError` whose writes are committed. A declared `false` that contradicts the text is refused when it is made. Declare `false` only for known reads. (README, "Running statements")
