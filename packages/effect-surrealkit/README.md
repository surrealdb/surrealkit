# effect-surrealkit

[Effect](https://effect.website) v4 schemas for SurrealDB values exactly as
the JavaScript SDK (`surrealdb` 2.x) decodes and encodes them, and an emitter
that turns a SurrealKit schema document v2 into typed Effect modules.

```sh
npm i effect-surrealkit effect@4.0.6 surrealdb@2.1.1
```

Every runtime export imports only `effect` and `surrealdb` value classes, so it
runs in browsers, Workers, Bun and Node. The emitter (`effect-surrealkit/emitter`)
is host-neutral too; only the `effect-surrealkit` bin touches the filesystem.

- [Generating modules](#generating-modules) from a schema document
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
| `PersonTable` | The descriptor: `{ name, type, id, select, create, merge, content, fields, presence, relations, permissions, edge?, digest }`. |
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
