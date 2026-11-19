# effect-surrealkit

[Effect](https://effect.website) v4 schemas for SurrealDB values exactly as
the JavaScript SDK (`surrealdb` 2.x) decodes and encodes them, an emitter
that turns a SurrealKit schema document v2 into typed Effect modules, and
typed statements built from those modules.

```sh
npm i effect-surrealkit effect@4.0.6 surrealdb@2.1.1
```

Every runtime export imports only `effect` and `surrealdb` value classes, so it
runs in browsers, Workers, Bun and Node. The emitter (`effect-surrealkit/emitter`)
is host-neutral too; only the `effect-surrealkit` bin touches the filesystem.

- [Generating modules](#generating-modules) from a schema document
- [Statements](#statements): typed queries and writes from the generated modules
- [Two codec worlds](#two-codec-worlds) and the [value schemas](#values)
- [What the codecs refuse](#what-the-codecs-refuse-and-why)

## Generating modules

`effect` and `surrealdb` are peer dependencies: the project that holds the
generated modules must declare both itself. The modules import `surrealdb`
(type-only, erased) so that declaration emit (`declaration: true`, `tsc --build`)
names SDK types as `"surrealdb"`; under pnpm, which does not hoist, a project
that does not declare it gets TS2742/TS2883 ("cannot be named … not portable")
on a fresh build.

SurrealKit describes a schema as a [schema document v2](../../docs/typegen/schema-document-v2.md)
(`surrealkit compile`, or `typegen` with `[typegen] schema_version = 2`). The
emitter reads one and writes Effect modules:

Usually SurrealKit runs it as an [external emitter](../../docs/typegen/external-emitters.md),
so `surrealkit compile`, `typegen` and `sync` produce the Effect modules directly:

```toml
# surrealkit.toml
[typegen.emit.effect]
command = "effect-surrealkit --store app"
out = "src/generated/surreal"
```

SurrealKit then sets `SURREALKIT_EMIT_PROTOCOL=1`, passes the document on
stdin and takes the files from the protocol reply on stdout; it owns `out`,
writes atomically and runs `--check` itself, so `--out`, `--input` and
`--check` are refused in that mode. Standalone, the bin writes the files:

```sh
effect-surrealkit --input schema.json --out src/generated/surreal --store app
```

The document comes from `--input` or stdin. Options can also come from a JSON
file (`--config emit.json`):

| Option | Meaning |
| --- | --- |
| `store` (required) | Logical store name. Record-id brands are `<store>/<table>`, so ids of same-named tables in different stores are different types. |
| `int` | `"safe-number"` (default) or `"bigint"` (`Int64`) |
| `datetime` | `"sdk"` (default) or `"effect-datetime"` (millisecond, lossy) |
| `fields` | Per field, keyed `table.path`: `int`, `datetime`, and presence overrides per operation |
| `acknowledge` | `{ "<error code>": "<reason>" }`: error diagnostics generation may proceed past |
| `names` | PascalCase names for tables whose default names collide |
| `patch` | Also emit `<Table>Patch` JSON Patch unions (not yet qualified against an engine) |
| `importExtension` | `".js"` (default) or `".ts"`: the extension of the generated modules' imports of each other (`--import-extension`) |

The default `.js` imports are what `tsc` and bundlers resolve to the `.ts`
sources. Node's own type stripping (`node file.ts`; `--experimental-strip-types`
before Node 23.6) maps nothing, so loading the generated sources there needs
`importExtension: ".ts"`, and a project that typechecks them then sets
`allowImportingTsExtensions` (with `noEmit`) or `rewriteRelativeImportExtensions`.
As an external emitter, pass it in the command:

```toml
[typegen.emit.effect]
command = "effect-surrealkit --store app --import-extension .ts"
out = "src/generated/surreal"
```

The emitter decodes the document strictly (unknown properties anywhere are an
error), recomputes its `semanticDigest` (a hand-edited document is refused),
and fails on any `error` diagnostic the options do not acknowledge. Output is
a pure function of the document, the options and the emitter version: no
timestamps, no physical namespace or database names, stable ordering, the
emitter's own fixed formatting. Writes are atomic per file, unchanged files are
not touched, and generated files no longer produced are removed (files without
the generated header are never touched). `--check` writes nothing and exits 1
when the directory is missing, edited, out of date or has stale files.

Access definitions may include an `authenticate` expression. It contributes to
the document digest and generated manifest, while older v2 documents without
the property remain accepted with their original digest. Regenerate older
documents to capture any `AUTHENTICATE` policy declared in their schema.

### What is generated

```text
ids.ts          PersonId, TeamId, … (branded RecordIdFor per table); OrgOrTeamId + OrgOrTeamTagged for record<org | team>
person.ts       PersonFields, Person, PersonCreate, PersonMerge, PersonContent, PersonTable;
                PersonJsonFields, PersonFromJson (the JSON world, after the native exports)
member-of.ts    … plus MemberOfEdge (relation endpoints)
team-size.ts    TeamSizeTable (a view: id and query only)
functions.ts    fn_greet = { name, args: Schema.Tuple([...]), returns }
manifest.ts     store, semanticDigest, emitter version, codec profile, peer versions, source
index.ts        re-exports everything
```

Each table gets one schema per operation, derived from the field-presence
matrix of the contract (section 7):

| Schema | Use |
| --- | --- |
| `Person` | A stored row as a privileged reader selects it: the plain struct, which `refine` works on. |
| `PersonRow` | `NoneAsAbsent(Person)`: decode rows the SDK returned with this one, so NONE (an `undefined` property) is an absent optional field. |
| `PersonCreate` | `CREATE … CONTENT` / `INSERT`; the id is an operation argument. |
| `PersonMerge` | `UPDATE … MERGE`: omission keeps a value, `undefined` (NONE) removes an `option<T>` field and re-applies a `DEFAULT ALWAYS` one (an `option<T>` included). |
| `PersonContent` | `UPDATE … CONTENT`: a full replacement. |
| `PersonTable` | The descriptor: `{ name, type, id, select, create, merge, content, fields, presence, relations, permissions, edge? }`. |
| `PersonFromJson` | The same selected row as SurrealDB's HTTP `/sql` renders it, decoding to the same types as `Person`; `PersonJsonFields` holds its field schemas for `Struct.pick` and field maps, and `PersonJsonEncoded` its JSON shape. Never the default; see [Two codec worlds](#two-codec-worlds). |

`Merge` is never `Partial<Create>`: READONLY, COMPUTED, `id` and relation
endpoints are excluded, and NONE is only accepted where the matrix allows it.
Every cell of the matrix is checked against a disposable engine in
`test/presence.test.ts`.

Recognised assertions become client-side checks where the check is exact:
literal membership narrows the type, int and float ranges become
`isGreaterThan…`/`isLessThan…`, and `string::len` / `array::len` become
code-point and array length checks. `string::is_email`, decimal and duration
ranges stay server-side.

### Refinements

`PersonFields` holds the field schemas that are identical in every operation;
these are the keys `refine` and `replaceField` accept:

```ts
import { Schema } from 'effect';
import { refine, replaceField } from 'effect-surrealkit';
import { PersonTable } from './generated/surreal/index.js';

// Narrowing (checks, brands) applies to select, create, merge and content alike,
// keeping each field's presence in each. Unknown keys are type errors.
const People = refine(PersonTable, {
  name: (s) => s.check(Schema.isMinLength(2)),
  email: (s) => s.pipe(Schema.brand('Email')),
});

// Changing a field's type is a separate, conspicuous call.
const Scored = replaceField(PersonTable, 'score', Schema.Literal(1));
```

An object with declared children is usually not the same in every operation:
`UPDATE … MERGE` merges nested objects, so its children are partial in Merge,
and children with a `DEFAULT` are optional in Create but required in Select.
Such a field is inlined in each variant, is not refinable, and is named with
its reason in the comment above `PersonFields`. An object that is the same
wherever it is used (a `READONLY` one, which Merge and Content exclude) is
listed and refines like any other field.

### Interpretation notes

- A field with no `TYPE` (and no declared children), or `TYPE any`, can hold
  NONE, so it is optional in every operation that includes it.
- Array and set elements are written whole, so the children of an element
  object follow the Create rules in Create, Merge and Content.
- Views get an id and a descriptor; their row shape is not derived.
  Lightweight relations get a select row and an edge, no write schemas.
- A `VALUE` that reads its input but has a fallback gets the conservative
  (required) contract; override it per field (`fields`) if the fallback is
  intended.
- A field declared without `TYPE` but with declared children behaves like an
  implicit parent: when a child is required it is printed as an object closed
  over them (what the engine enforces) and is required; otherwise it is
  `Schema.Unknown`, always selected and resettable.
- An `object` without declared children is closed on a SCHEMAFULL table
  unless its field is `FLEXIBLE`: SurrealDB 3.2 and 3.3 refuse every
  undeclared key of it (`Found field 'o.x', but no such field exists`), so it
  can hold only `{}` and is printed `Schema.Record(Schema.String, Schema.Never)`.
  A `FLEXIBLE` parent does not open a declared child object. A `FLEXIBLE`
  object, an `array<object> FLEXIBLE` element, and an object on a SCHEMALESS
  table keep their keys and are open records.
- `regex` and `function` kinds become `Unsupported(kind)` (with a warning):
  the values have no SDK representation.

### Printer: SchemaRepresentation versus a template printer

The emitter prints modules with its own small template printer. The
alternative was to build live schemas from the codecs and print them with
`SchemaRepresentation.toRepresentation` → `toCodeDocument`, which every codec
supports (each carries `toCode` metadata; `test/to-code.test.ts` prints a
struct of all of them). It works mechanically but was not adopted because:

- it emits one line per schema, so readable output needs an external
  formatter in the generation path;
- every declaration and check is re-emitted with its `expected` annotation
  (`SurrealSchema.Int.annotate({ "expected": "a safe integer (SurrealDB int)" })`);
- `Schema.brand` is TypeScript-only and absent from representations, so
  per-store record-id brands cannot be printed;
- the module structure (an `ids.ts` imported by every table, shared
  `PersonFields`, descriptors, presence maps, edges, the manifest) is not a
  schema and would need a printer of its own anyway.

## Statements

`effect-surrealkit/statement` describes one request to SurrealDB as a value,
and `effect-surrealkit/client` runs it. A statement holds static SurrealQL
text, a codec for each named binding and a codec for its result, so the text
never depends on input, a value its codec refuses is never sent, and the
answer is decoded strictly. Types come from the generated descriptors alone.

```ts
import { Effect } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { layer, run } from 'effect-surrealkit/client';
import { eq, gte, param, select } from 'effect-surrealkit/statement';
import { Surreal } from 'surrealdb';
import { PersonTable } from './generated/surreal/index.js';

const adults = select(PersonTable)
  .where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
  .where((row) => eq(row.address.city, param('city', SurrealSchema.String)))
  .orderBy((row) => row.name)
  .project((row) => ({ id: row.id, name: row.name, nickname: row.nickname }))
  .build('adults in a city');
// adults.sql: SELECT id AS id, name AS name, nickname AS nickname FROM person
//             WHERE age >= $minimum AND address.city = $city ORDER BY name ASC;

const db = new Surreal();
await db.connect(url, { namespace, database, authentication });

const rows = await Effect.runPromise(
  run(adults, { minimum: 18, city: 'Leeds' }).pipe(Effect.provide(layer(db))),
);
// ReadonlyArray<{ id: PersonId; name: string; nickname: string | undefined }>
```

A misspelt field, a string compared with an int, a missing or extra binding
and a wrong binding type are compile errors. `row` has one expression per
field of the table's select schema; an object field carries its children
(`row.address.city`), and an optional field, or anything read through one,
may be NONE.

### Expressions

| Function | SurrealQL |
| --- | --- |
| `param(name, codec)` | `$name`, encoded through `codec` when the statement runs |
| `literal(value)`, `literal(value, codec)` | a constant in the text, as the SDK prints it (`s"…"`, `d"…"`, `r"…"`) |
| `eq` `ne` `lt` `lte` `gt` `gte` | `=` `!=` `<` `<=` `>` `>=`; the right side may be a primitive |
| `and(…)` `or(…)` `not(x)` | `AND` `OR` `!` |
| `isNone(x)` `isNotNone(x)` | `x = NONE`, `x != NONE` |
| `inside(x, xs)` `contains(xs, x)` | `INSIDE`, `CONTAINS`; `xs` may be an array of primitives |
| `at(array, i)` | `array[i]`, or NONE past the end |
| `cast(kind, x)` | `<kind>x` |
| `call(fn, …args)` | a `DEFINE FUNCTION`, through its generated descriptor |
| `select(…).toExpr()` | a subquery |
| `unsafeExpr({ sql, codec, params })` | raw SurrealQL: nothing is checked, and it must never be built from input |

Operands compare when their stored types overlap: a branded or refined
binding compares with its column, a string never compares with an int. A
binding is named, so the same expression can be used twice; one name with
two codecs is refused, as are names SurrealDB sets itself (`$auth`, `$this`,
`$value`, …).

### Reads and writes

| Builder | Result |
| --- | --- |
| `select(table)` | rows; `.project(…)` named expressions, `.value(…)` one expression per row |
| `select(table).only()` | one row or `undefined` (adds `LIMIT 1`) |
| `select(table, id).only()` | the record or `undefined` |
| `create(table)`, `create(table, id)` | the created row (`CREATE ONLY … CONTENT`) |
| `update(table, id).merge(x)` / `.content(x)` | the row, or `undefined` when the record is not there |
| `update(table).merge(x).where(…)` | every changed row |
| `deleteFrom(table, id)`, `deleteFrom(table).where(…)` | nothing; `.returning('before')` for the deleted rows |
| `relate(edgeTable, from, to)` | the edge (`RELATE ONLY`); the endpoints must be ids the relation declares |

Data is always bound, and must fit the descriptor's schema for the operation
(`create`, `merge`, `content`). `.returning('none' | 'after' | 'before')` or a
projection changes the result type with the `RETURN` clause. Builders are
immutable, so a base query can be shared.

### Authored SurrealQL

A multi-statement script stays SurrealQL. Describe each text as a `part`
(the `$names` it reads and does not define, the names it `LET`s, how many
results it returns, what it can `THROW`) and run parts in one scope with
`sequence`. The contract must name exactly the names the sequence still
needs: a missing or extra binding is a compile error, checked again when the
statement is defined.

```ts
import { part, sequence } from 'effect-surrealkit/statement';

const bindOwner = part({
  sql: 'LET $owner = $boundOwner; LET $principal = $auth.id;',
  free: ['boundOwner'],
  defines: ['owner', 'principal'],
  slots: 2,
});
const requireOwner = part({
  sql: "IF $owner != $principal { THROW 'Not the owner' };",
  free: ['owner', 'principal'],
  throws: ['Not the owner'],
});
const readProfile = part({
  sql: 'SELECT VALUE name FROM ONLY $owner;',
  free: ['owner'],
});

const profile = sequence(bindOwner, requireOwner, readProfile).define({
  name: 'read own profile',
  params: { boundOwner: PersonId }, // exactly the unbound names
  result: SurrealSchema.String,     // the last result; earlier ones must be NONE
});
```

SurrealDB reads a parameter nothing bound as NONE, without an error (3.3
and 4.0), so a misspelt binding would quietly change what a script does;
that is what the contract is for. `make({ sql, params, result })` declares a
statement with no such check.

### Running statements

```ts
import { bind, layer, run, SurrealClient, transaction } from 'effect-surrealkit/client';

const program = Effect.gen(function* () {
  const name = yield* run(profile, { boundOwner: id });
  const [created, people] = yield* transaction([
    bind(createPerson, { person }),
    bind(adults, { minimum: 18, city: 'Leeds' }),
  ]);
  return { name, created, people };
}).pipe(Effect.provide(layer(db)));
```

`layer(db)` takes an SDK connection or session, and statements run with that
session's authentication. `make(db)` returns the same client as a plain
value, for an application that names its own services, one per session.

| Error | Meaning |
| --- | --- |
| `StatementEncodeError` | a binding failed its codec; nothing was sent |
| `StatementThrown<Message>` | a `THROW` the statement declares |
| `StatementConflict` | a transaction conflict; nothing changed, send it again |
| `StatementRequestError` | any other failure, with the SDK's error as `cause` |
| `StatementDecodeError` | the answer is not what the statement declares; a write may have applied |

Interrupting the effect aborts the request (the engine may still finish it).
Each run is a span named after the statement, carrying the static text and
no binding values. `{ retry }` is passed to the SDK.

`transaction` sends its statements as one atomic request. They share one
variable scope, as SurrealQL has it: a binding name used by two of them must
carry the same value. The SDK refuses a top-level `RETURN` followed by
another statement inside a transaction.

### What the statements do not know

- **Permissions.** A generated `select` schema is the row a privileged
  reader gets. For a restricted session SurrealDB returns NONE for a field
  it may not read and leaves out a row it may not see, without an error.
  Declare the row that session really gets and build from
  `readAs(table, schema)`, or project the fields it may read.
- **Authored text.** The parts of a sequence are declared, not inferred: a
  result codec that does not match the script fails when it runs, not when
  it compiles.
- **Coverage.** `GROUP BY`, `SPLIT`, `FETCH`, graph traversal, full-text and
  vector operators, `UPSERT`, `INSERT`, live queries and views have no typed
  builder yet; use `unsafeExpr` or authored SurrealQL.

The engine suite (`test/statement-engine.test.ts`) runs every builder against
the corpus schema and the modules generated from it, and passes on SurrealDB
3.3.4 and 4.0.1.

## Two codec worlds

The default exports describe **native** values: what the SDK hands back after
CBOR decoding (`DateTime`, `Decimal`, `RecordId`, `Set`, …) and accepts for
bound variables. Decode a query result with them; encode bindings with them.

Ordinary SurrealDB **JSON** (`/sql` over HTTP, JSON exports) is lossy — NONE
and NULL collapse, decimals become strings, sets become arrays, record ids
become text, ints beyond `±(2^53 - 1)` cannot survive `JSON.parse`. Codecs for
that representation are named `<Name>FromJson` (with `<Name>JsonEncoded`
types) and are never the default.

Every generated table also gets the JSON world of its selected row:
`<Table>JsonFields`, `<Table>FromJson` and `<Table>JsonEncoded`. They decode
the JSON SurrealDB 3.2 and 3.3 render to the same types as the native
exports (brands and checks included), so projections are derived the same
way: `Schema.Struct(Struct.pick(PersonJsonFields, ['name', 'born']))`. What
JSON loses, and what these codecs then do:

| Value | SurrealDB JSON | JSON world |
| --- | --- | --- |
| NONE | an absent key in rows and objects, `null` inside arrays | optional fields are absent keys; `option<T>` elements read as `null` |
| NULL | `null` | `T \| null` fields read as `null`; inside arrays NONE and NULL are the same `null` |
| `decimal` | text, normalised (`1.50dec` → `"1.5"`) | exact through text, never a JS number |
| `int` beyond `±(2^53 - 1)` | a JSON number, already rounded by `JSON.parse` | `Int64FromJson` fails loudly; in a `number` field it decodes as the rounded number |
| `float` NaN, ±Infinity | `null` | fails to decode; `-0` renders as `0` |
| `datetime` | RFC 3339 text with nanoseconds | exact as `DateTime`; milliseconds under the `effect-datetime` profile |
| `record` | SurrealQL record-id text; string keys of letters and digits unquoted (`p:1s`) | a bare key that is not all digits is a string key |
| `set` | a sorted array | a `Set`; duplicates fail |
| `range` | SurrealQL range text | parsed by the SDK, which prints string bounds without quotes when encoding |
| `bytes`, `geometry`, `file`, `table` | byte arrays, GeoJSON, `f"bucket:/key"`, the bare name | exact |

`test/engine.test.ts` stores values of every kind through the SDK and reads
them back over HTTP `/sql`; `test/roundtrip.test.ts` reads every round-trip
row of the corpus both ways and requires the same value.

```ts
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

const Person = SurrealSchema.NoneAsAbsent(
  Schema.Struct({
    id: SurrealSchema.RecordIdFor('person'),
    name: SurrealSchema.String,
    nickname: Schema.optionalKey(SurrealSchema.String),
    born: SurrealSchema.DateTime,
    balance: SurrealSchema.Decimal,
    tags: SurrealSchema.SetOf(SurrealSchema.String),
  }),
);

const [rows] = await db.query<[unknown[]]>('SELECT * FROM person');
const people = rows.map(Schema.decodeUnknownSync(Person));
```

## Values

| SurrealQL | Schema | Decoded type |
| --- | --- | --- |
| `int` | `Int` (`int: "safe-number"`, default) / `Int64` (`int: "bigint"`) | `number` / branded `bigint` |
| `float` | `Schema.Number` | `number` |
| `decimal` | `Decimal` | SDK `Decimal` |
| `number` | `SurrealNumber` | `number \| bigint \| Decimal` |
| `string` | `String` | `string` (well-formed Unicode) |
| `datetime` | `DateTime` (`datetime: "sdk"`, default) / `DateTimeEffect` | SDK `DateTime` / Effect `DateTime.Utc` |
| `duration` | `Duration` | SDK `Duration` |
| `uuid` | `Uuid` | SDK `Uuid` |
| `bytes` | `Bytes` | `Uint8Array` |
| `record<t>` | `RecordIdFor("t", { key? })` | `RecordId<"t">` |
| `record` | `RecordIdAny` | `RecordId` |
| `record<a \| b>` | a union of `RecordIdFor`s; `taggedRecordId({ a, b })` for `{ _tag, id }` | |
| `table<a \| b>` | `TableFor(["a", "b"])` | `Table<"a" \| "b">` |
| `file<bucket>` | `FileRefFor(["bucket"])` | SDK `FileRef` |
| `geometry<…>` | `GeometryOf([...])`, `GeometryPoint`, … | SDK geometry classes |
| `range` | `Range` | SDK `Range` |
| `set<T, N>` | `SetOf(T, { length?, equality? })` | `ReadonlySet<T>` |
| literals | `Schema.Literal`, `DecimalLiteral`, `DurationLiteral`, `Int64Literal` | |
| `regex`, `function` | `Unsupported(kind)` | `never` |

`table`, `edge`, `fn`, `refine` and `replaceField` (and the `EdgeIn`,
`EdgeOut`, `FunctionArgs` types) are what generated modules are built from;
see [Generating modules](#generating-modules).

`NoneAsAbsent(schema)` removes properties whose value is `undefined` (how the
SDK surfaces NONE) before decoding, so exact `Schema.optionalKey` fields work
on projections and object values; `null` is kept.

Every schema carries `toCode` metadata, so
`SchemaRepresentation.toCodeDocument` prints a schema built from them back to
source.

## What the codecs refuse, and why

Measured against SurrealDB 3.2.4 and 3.3.0 with surrealdb.js 2.1.1. Each of
these is a silent corruption or a hang without the check:

- **Integers.** The SDK decodes an `int` as a `number` up to `2^53` and as a
  `bigint` beyond, except that `-(2^53 + 1)` arrives as the number `-2^53`
  (already rounded). `Int` fails beyond `±(2^53 - 1)`; `Int64` accepts only
  exactly representable numbers, so `-2^53` fails as ambiguous. A bigint
  outside int64 is refused before sending: the server wraps it
  (`2^63` is stored as `-2^63`).
- **Decimals** are limited to a 96-bit mantissa and 28 fractional digits.
  A larger mantissa makes the server answer with an error the SDK never
  resolves (the query hangs); extra fractional digits are rounded away.
  The SDK's string constructor is permissive (`new Decimal("abc")` is zero,
  `new Decimal("+1.5")` is 0.5), so `decimalFromString` never uses it: it
  parses sign, digits and exponent itself and builds the value from integers.
- **Datetimes** outside `-262143-01-01 … +262142-12-31` hang the query the same
  way. The SDK formats pre-1970 datetimes with fractional seconds one second
  late, so `DateTimeFromJson` uses its own RFC 3339 parser and formatter.
- **Durations** cannot be negative; the SDK silently sends a negative
  `Duration` as `0ns`.
- **Strings** with a lone UTF-16 surrogate cannot be stored in UTF-8; the CBOR
  encoder would replace it with U+FFFD.
- **Sets** are only sent deduplicated (under SurrealDB value equality) and of
  their exact declared length: SurrealDB 3.3.0 aborts the server on a
  duplicate-bearing write to an `option<set<T, N>>` field.
- **Bytes** arrive as an `ArrayBuffer`; `Bytes` normalises them to
  `Uint8Array`.
- Configure the SDK with its default `useNativeDates: false`; `DateTime`
  rejects JS `Date`s, which lose nanoseconds.

## Development

```sh
bun install
bun run lint
bun run typecheck
bun run test
```

The property suites round-trip every codec through the SDK's CBOR codec in
process (`Arbitrary.schema` + `Arbitrary.checkEffect`). Golden output lives in
`test/golden/` (regenerate with `UPDATE_GOLDEN=1 bun run test`): the contract's
`example.v2.json`, the same with the `bigint`/`effect-datetime`/`patch`
options, the whole corpus, the integer-keyed corpus tables in the `bigint` profile (the
whole corpus in that profile is checked line by line against the `corpus`
golden instead of being pinned twice) and the field-presence evidence tables. The corpus
inputs are real `surrealkit compile` output, pinned byte for byte by
SurrealKit's own test suite under `typegen_corpus/compiled/`, and every input
document is validated against the published JSON Schema. The golden
comparison ignores the emitter version in `manifest.ts` (and SurrealKit's pin
ignores the SurrealKit and engine versions), so a release does not mean
regenerating them.
`test/types` compiles the golden modules with positive and `@ts-expect-error`
fixtures under `strict` and `exactOptionalPropertyTypes`.

The engine and field-presence suites run against a real, disposable server
when one of these is set, and are skipped (with a notice) otherwise:

```sh
SURREAL_BIN=/usr/local/bin/surreal bun run test          # starts `surreal start … memory`
SURREAL_URL=ws://127.0.0.1:8000/rpc bun run test         # a throwaway container, root/root
```

The round-trip properties (`test/roundtrip.test.ts`) run the generated corpus
modules against a database prepared by `surrealkit test --exec`: create →
select → merge → select for the corpus tables, every record-id key kind (int64
extremes included, under the `bigint` profile), `relate` on an enforced
relation and function calls, with values from `Arbitrary.schema` over the
generated schemas and `Arbitrary.checkEffect` required to report `Passed`. The
SurrealKit binary is the workspace build (`cargo build`) or `SURREALKIT_BIN`:

```sh
SURREAL_BIN=/usr/local/bin/surreal bun run test:roundtrip            # starts its own server
SURREALDB_HOST=ws://127.0.0.1:8000 SURREALDB_USER=root SURREALDB_PASSWORD=root \
  bun run test:roundtrip                                               # a disposable server
```

`scripts/roundtrip.mjs` compiles the corpus's portable modules, then runs
`surrealkit test --exec`; the suite's first check is that the prepared
database's `SURREALKIT_EXEC_SCHEMA_DIGEST` equals the compiled `semanticDigest`.
