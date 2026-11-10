# Python typed queries

`db.query("…")` and `db.live_query("…")` typed by SurrealKit for the
[`surrealdb`](https://pypi.org/project/surrealdb/) Python SDK, checked by pyright and mypy in strict
mode, and run against a real server.

```text
database/schema/001_cards.surql   the schema: card (title, status, due, owner, tags) and person
database/queries/cards.surql      named queries: ListCards, CardsByOwner, CountByStatus
surrealkit.toml                   [typegen.queries] files + [typegen.python] out
app/surreal_queries.py            the typed module (generated; do not edit)
app/main.py                       typed code: query() and live_query() with parameters
app/bad.py                        deliberate mistakes, each marked for both checkers
app/runtime_check.py              runs the typed clients against a server and checks the values
pyrightconfig.json, mypy.ini      strict, and an ignore that hides nothing is an error
```

## Generate

```bash
cd examples/python-typed
surrealkit compile          # reads surrealkit.toml here; writes app/surreal_queries.py and schema.json
surrealkit compile --check  # CI: fail if the committed generated files are stale
```

`compile` applies `database/schema` to a throwaway in-memory engine and types the queries
against it, so nothing connects to a database. Typing queries needs a SurrealKit built against
SurrealDB 4.1; on an older engine `compile` reports `queries-unsupported-engine` and writes an
empty module.

It reads the literal `query(...)` and `live_query(...)` calls in `app/main.py` and
`app/runtime_check.py` (`[typegen.python] files`) and the named queries in `database/queries`.
`app/bad.py` is not read: its unknown query text must stay unknown.

## Check the types

```bash
pyright                     # strict; pythonVersion 3.12
mypy                        # strict; warn_unused_ignores
```

Both must pass. `app/bad.py` has four mistakes (a wrong parameter type, a literal the column does
not allow, a text the module does not know, a field that is not selected), each with mypy's
`# type: ignore[code]` and pyright's `# pyright: ignore[rule]`. A mistake that stops being an
error turns its ignore into an error (`warn_unused_ignores`, `reportUnnecessaryTypeIgnoreComment`),
and `enableTypeIgnoreComments: false` keeps pyright from honouring mypy's comments, so each
checker is held to its own.

## Run it against a server

`app/runtime_check.py` defines the namespace and database (a fresh 4.1 server needs it), applies the
schema, seeds three cards, then fetches through `TypedAsyncSurreal` and `TypedSurreal` and asserts
what the SDK actually returned, and subscribes with `live_query` through a CREATE, an UPDATE and a
DELETE. It exits non-zero on any failure.

```bash
surreal start --bind 127.0.0.1:8000 --user root --pass root memory &
SURREAL_URL=ws://127.0.0.1:8000/rpc python -m app.runtime_check
```

## What the SDK returns

- `SELECT *` leaves a NONE `option<…>` field out of the row, and an explicit projection
  (`SELECT title, due`) returns it as `None`. The field is `NotRequired[… | None]`, which covers
  both; read it with `.get(...)`.
- A `set<string>` is a `SurrealSet`, a list that has no element type.
- A live notification is a dict with `action`, `id`, `record`, `result` and `session`; `KILLED`
  ends the generator.
