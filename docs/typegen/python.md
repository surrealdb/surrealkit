# Python typegen

`[typegen.python]` renders the `queries` section of
[schema document v2](schema-document-v2.md#19-queries) as one Python module for the
[`surrealdb`](https://pypi.org/project/surrealdb/) SDK. The module needs Python 3.10 or later.
With `models = "pydantic"` it needs pydantic 2, and below Python 3.11 it imports `NotRequired`
and `Never` from `typing_extensions`.

## Configuration

```toml
[typegen.python]
out = "app/surreal_queries.py"   # required; must end in .py
files = ["**/*.py"]              # globs relative to the project root
methods = ["query", "query_raw", "live_query"]
hints = "trust"                  # "trust" | "unknown"
models = "typeddict"             # "typeddict" | "pydantic"
```

- `out` is the generated module. SurrealKit owns it, and it is never read as a source.
- `files` chooses the `.py` files read. `.pyi` files, `.venv`, `venv`, `__pycache__`,
  `site-packages`, `node_modules` and hidden directories are never read.
- `methods` are the call names that carry a query, as `receiver.method(…)`: the query is the first
  positional argument or `query=`.
- `hints = "unknown"` types a statement or parameter that the engine only hints at as `Value`.
- `models = "pydantic"` writes rows as `BaseModel` classes and validates each statement's result
  into them.

A project may set `[typegen.python]` alone. Named queries (`.surql` files) come from
`[typegen.queries] files`. A text written in both TypeScript and Python is one query.

## What is read

A query is one string literal: plain or triple-quoted, with an optional `r`, `u`, `R` or `U` prefix.
Escapes are decoded and a line break is `\n`, so the text is what Python sends. A call that is
skipped is counted by reason in the compile summary and raises no diagnostic:

| reason | example |
|---|---|
| f-string | `db.query(f"SELECT {x}")` |
| bytes | `db.query(b"SELECT 1")` |
| concatenation | `db.query("SELECT " "1")`, `db.query("a" + b)` |
| not a literal | `db.query(sql)`, `db.query("…".format(x))` |
| unsupported escape | `db.query("\N{DIGIT ONE}")` |

## Types

| SurrealQL kind | Python |
|---|---|
| `bool`, `int`, `float`, `number`, `string` | `bool`, `int`, `float`, `int \| float`, `str` |
| `decimal`, `datetime`, `uuid`, `bytes` | `decimal.Decimal`, `datetime.datetime`, `uuid.UUID`, `bytes` |
| `duration`, `table`, `file` | `Duration`, `Table`, `File` |
| `record<…>` | `RecordID` |
| `geometry<…>` | the matching `Geometry…` class; `Geometry \| GeometryCollection` for any |
| `bigdecimal` | `BigDecimal` |
| `none`, `null` | `None`, `NullType` |
| `any`, `unknown`, `date`, `time`, `timestamp`, `interval`, `range`, `regex`, `function` | `Any` |
| object with fields | a `TypedDict` (a model with `models = "pydantic"`) |
| `object` | `dict[str, Value]` |
| `array<T>` | `list[T]` (`list[Never]` for a zero-length array) |
| `set<T>` | `SurrealSet` |
| tuple | `tuple[…]` |
| literal, enum, union of literals | `Literal["a", "b"]` |
| `option<T>` as an object field | `NotRequired[T]` (`T \| None = None` in a model) |
| `option<T>` elsewhere, an optional statement | `T \| None` |
| union | `A \| B` |
| `DEFINE TYPE` name | expanded inline |

An `option<T>` field that is NONE is absent from the row the SDK returns, which is why it is
`NotRequired`. A key that is not an identifier or is a keyword makes that class use the functional
`TypedDict("X", {...})` syntax, or a model field with `Field(alias=…)`.

Types are named after the query: a named query `ListCards` has `ListCardsRow` (statement 0;
`ListCardsRow1`, … for later statements), `ListCardsParams`, and `ListCardsRow` plus the field path
for a nested object (`ListCardsRowOwner`). An inline query is `Query_` and the first 8 hex digits
of the SHA-256 of its text. A name that is taken gets a number.

## The client

```python
from app.surreal_queries import TypedAsyncSurreal

db = TypedAsyncSurreal(connection)          # an AsyncSurreal(...) connection
(rows,) = await db.query("SELECT name FROM person WHERE age >= $min", {"min": 18})
```

`TypedAsyncSurreal` wraps an `AsyncSurrealConnection` and `TypedSurreal` a
`BlockingSurrealConnection`; each keeps the connection as `.raw`. `query()` has one `@overload` per
query text and returns a tuple with one element per statement. A query with parameters takes them
as the `…Params` dict; one whose parameters are all optional may leave the argument out; one with
none takes no argument. Parameters the host language fills (`__host…`) are left out.

There is **no overload for a text the module does not list**. A `str` fallback would also accept a
known text with the wrong parameters, so an unknown text is a type error, and `db.raw.query(…)` is
the way to send it. With one query, `query()` has a single signature instead of overloads.

### Live queries

A query of one statement that `LIVE SELECT` can run also gets a `live_query()` overload, keyed by
the same text. It yields the notifications the subscription delivers (an `AsyncIterator` for the
async client, an `Iterator` for the blocking one):

```python
async for notification in db.live_query("SELECT * FROM user WHERE team = $team", {"team": team}):
    if notification["action"] == "CREATE":
        print(notification["result"]["name"])
```

A notification is one of `…Create`, `…Update` and `…Delete`, each a `TypedDict` with `action`,
`id`, `record` and `result`; `…Notification` is their union. `KILLED` ends the stream and is not
yielded. A statement `LIVE SELECT` refuses has no overload, and the module has a comment naming the
clauses.

## Limits

- **No fallback overload.** See above: an unknown text goes through `db.raw`.
- **`SurrealSet` has no element type.** The SDK's `SurrealSet` is a plain `list[Any]` subclass, so
  `set<string>` is `SurrealSet` and the element type is lost.
- **Open objects.** An object the engine says may carry undeclared keys is the declared
  `TypedDict`; Python's `TypedDict` cannot say "and other keys". The extra keys are present at
  runtime. A pydantic model allows them (`extra="allow"`).
- **Only literals.** A query built at runtime has no type.
- **Not checked at runtime.** A `TypedDict` describes the SDK's dicts and checks nothing. Only
  `models = "pydantic"` validates.
