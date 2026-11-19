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
is host-neutral too; only the `effect-surrealkit` bin touches the filesystem, and
only [`effect-surrealkit/testing`](#testing) can load `@surrealdb/node`.

- [Generating modules](#generating-modules) from a schema document
- [Statements](#statements): typed queries and writes from the generated modules,
  and [statements from `.surql` files](#statements-from-surql-files)
- [Testing](#testing) with an in-memory database, record users, index
  expectations and a scripted client
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
_store.ts       store = { name, tables, views, edges, functions }, each by its SurrealDB name
index.ts        re-exports everything
```

`store` registers every table, view, relation endpoint and function of the
store under its SurrealDB name (`store.tables.person`,
`store.edges.member_of`, `store.functions["fn::greet"]`), so a record link
(`RecordId<"person">`) or a graph step resolves to the descriptor of its
target. Its file name has a leading underscore because table modules are
kebab-case words, so no table can be written there.

Each table gets one schema per operation, derived from the field-presence
matrix of the contract (section 7):

| Schema | Use |
| --- | --- |
| `Person` | A stored row as a privileged reader selects it: the plain struct, which `refine` works on. |
| `PersonRow` | `NoneAsAbsent(Person)`: decode rows the SDK returned with this one, so NONE (an `undefined` property) is an absent optional field. |
| `PersonVisible` | The row as a session reads it when field permissions may hide fields: every field (at any depth) whose `select` permission is not FULL is optional. `Person` itself when no permission can hide a field. Statements read it through `readAs(PersonTable)`. |
| `PersonCreate` | `CREATE … CONTENT` / `INSERT`; the id is an operation argument. |
| `PersonMerge` | `UPDATE … MERGE`: omission keeps a value, `undefined` (NONE) removes an `option<T>` field and re-applies a `DEFAULT ALWAYS` one (an `option<T>` included). |
| `PersonContent` | `UPDATE … CONTENT`: a full replacement. |
| `PersonTable` | The descriptor: `{ name, type, id, select, create, merge, content, fields, presence, relations, permissions, fieldPermissions, indexes, edge? }`. |
| `PersonFromJson` | The same selected row as SurrealDB's HTTP `/sql` renders it, decoding to the same types as `Person`; `PersonJsonFields` holds its field schemas for `Struct.pick` and field maps, and `PersonJsonEncoded` its JSON shape. Never the default; see [Two codec worlds](#two-codec-worlds). |

A descriptor's `indexes` holds every `DEFINE INDEX` of the table by name,
with its kind and the field paths it covers kept as literal types
(`indexes.idx_owner_status` is `{ type: "standard", columns: [["owner"],
["status"]] }`), plus the analyzer of a `FULLTEXT` index, the vector shape
of an `HNSW` or `DISKANN` one and the condition of a `COUNT … WHERE` one.
`fieldPermissions` holds each top-level field's `select`, `create` and
`update` permission (`true` for FULL, `false` for NONE, or the `WHERE`
expression), with those of its declared children under `fields`.

`Merge` is never `Partial<Create>`: READONLY, COMPUTED, `id` and relation
endpoints are excluded, and NONE is only accepted where the matrix allows it.
Every cell of the matrix is checked against a disposable engine in
`test/presence.test.ts`.

Recognised assertions become client-side checks where the check is exact:
literal membership narrows the type, int and float ranges become
`isGreaterThan…`/`isLessThan…`, and `string::len` / `array::len` become
code-point and array length checks. `string::is_email`, decimal and duration
ranges stay server-side.

### What changes for existing users of the emitter

Regenerating with this version only adds to the output; every existing
export keeps its name, its schema and its type:

- every table descriptor gains `fieldPermissions`, `indexes` and `visible`,
  and every table module a `<Table>Visible` export (with its `Type` and
  `Encoded` types), so a table whose PascalCase name is another table's
  plus `Visible` (`person` and `person_visible`) now needs a `names` entry;
- a new module, `_store.ts`, exports `store`, and `index.ts` re-exports it.
  A SurrealKit-run emitter records the new file in `.surrealkit-emit.json`
  as it does every generated file.

Code that spells out a descriptor's full type, or compares one with
`toEqual`, sees the new keys. Hand-written descriptors without them keep
working with every builder.

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

In an open struct's Merge schema, the rest record accepts `SurrealMergeValue`:
a member can be `undefined` (NONE), including inside an object patch. This lets
a declared removable child such as `address.zip` pass both its own codec and
the rest-record codec. Array and set elements still use the stored-value codec.

### Interpretation notes

- A field with no `TYPE` (and no declared children), or `TYPE any`, can hold
  NONE, so it is optional in every operation that includes it.
- Array and set elements are written whole, so the children of an element
  object follow the Create rules in Create, Merge and Content. Collections
  whose item kind is `any` use the kind declared on the `*` field when
  present. Otherwise their items use `SurrealValue` or `SurrealJsonValue`,
  which exclude NONE.
- Views get an id and a descriptor; their row shape is not derived.
  Lightweight relations get a select row and an edge, no write schemas.
- A `VALUE` that ignores its input is selected, and excluded from Create,
  Merge and Content. The engine accepts a value supplied for such a field and
  discards it, storing what the `VALUE` produces, so a schema that allowed it
  would let a caller's value be thrown away without an error.
- The emitter reserves `id` for the table's record ID and `in` and `out` for
  relation endpoints. The record ID declarations in `ids.ts` put dependencies
  before the keys that reference them.
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
  table keep their keys and are open records. Open records, flexible structs
  and schemaless rows use `SurrealValue` for extra keys in native schemas and
  `SurrealJsonValue` in JSON schemas.
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
// adults.sql: SELECT id AS id, name AS name, nickname AS nickname FROM ⟨person⟩
//             WHERE age >= $minimum AND address.city = $city ORDER BY name ASC;

const db = new Surreal();
await db.connect(url, { namespace, database, authentication });

const rows = await Effect.runPromise(
  run(adults, { minimum: 18, city: 'Leeds' }).pipe(Effect.provide(layer(db))),
);
// ReadonlyArray<{ id: PersonId; name: string; nickname: string | undefined }>
```

A misspelt field, a string compared with an int, a missing or extra binding
and a wrong binding type are compile errors. Where a mistake would otherwise
only print types, the expected type at the failing position is a message,
so the first line of the diagnostic says what is wrong:

```text
Argument of type '{ email: string; }' is not assignable to parameter of type '"binding `name` has no value"'.
Argument of type '...' is not assignable to parameter of type '"`bogus` is not a field `person` accepts in MERGE"'.
Type 'Expr<...>' is not assignable to type '"ORDER BY takes a field path of the row, like `row.name` or `at(row.pins, 0)`"'.
```

`test/diagnostics.test.ts` pins the diagnostic of every negative type
fixture. In a hover, a statement's bindings, its `Input` (in the order the
statement reads them) and its result print as plain object types;
`test/hover.test.ts` pins them. `row` has one expression per
field of the table's select schema; an object field carries its children
(`row.address.city`), and an optional field, or anything read through one,
may be NONE.

### Expressions

| Function | SurrealQL |
| --- | --- |
| `param(name, codec)` | `$name`, encoded through `codec` when the statement runs |
| `literal(value)`, `literal(value, codec)` | a constant in the text (`s"…"`, `d"…"`, `r"…"`), every part checked as its codec checks a binding |
| `eq` `ne` `lt` `lte` `gt` `gte` | `=` `!=` `<` `<=` `>` `>=`; the right side may be a primitive |
| `and(…)` `or(…)` `not(x)` | `AND` `OR` `!` |
| `isNone(x)` `isNotNone(x)` | `x = NONE`, `x != NONE` |
| `inside(x, xs)` `contains(xs, x)` | `INSIDE`, `CONTAINS`; `xs` may be an array of primitives |
| `at(array, i)` | `array[i]`, or NONE past the end; an element of a field path is a field path |
| `cast(kind, x)` | `<kind>x` |
| `call(fn, …args)` | a `DEFINE FUNCTION`, through its generated descriptor |
| `select(…).toExpr()` | a subquery |
| `unsafeExpr({ sql, codec, params })` | raw SurrealQL: nothing is checked, and it must never be built from input |

A constant is validated as a value is: a string must be well-formed (inline
lists included) and an integer must fit a signed 64-bit int. A datetime is
written with `dateTimeToString`, because the SDK's formatter is a second
late before 1970 with a fractional second; a record id whose key holds a
datetime, a `Set`, a `Map`, bytes and class instances have no faithful
literal and are refused (bind them).

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
| `select(table, index, values)` | by a unique index: the row or `undefined` |
| `select(table, idRange(table, bounds))` | the records whose ids the range spans |
| `create(table)`, `create(table, id)` | the created row (`CREATE ONLY … CONTENT`) |
| `update(table, id).merge(x)` / `.content(x)` | the row, or `undefined` when the record is not there |
| `update(table).merge(x).where(…)` | every changed row |
| `deleteFrom(table, id)`, `deleteFrom(table).where(…)` | nothing; `.returning('before')` for the deleted rows |
| `relate(edgeTable, from, to)` | the edge (`RELATE ONLY`); the endpoints must be ids the relation declares |
| `upsert(table, id).merge(x)` / `.content(x)` | the record, created or changed (`UPSERT ONLY`) |
| `upsert(table).merge(x).where(…)` | every changed record, or the one it created |
| `insert(table, rows)` | every inserted row; `rows` is one row or an array; `.onDuplicate(…)` for `ON DUPLICATE KEY UPDATE` |
| `insertRelation(edgeTable, edges)` | every inserted edge (`INSERT RELATION`) |

A record id of a table is built from its key parts by `recordId(table,
key)`: one part, an array of parts for an array key, or an object of parts
for an object key, each a binding or a constant. Each part must be what the
table's id codec declares for that position (a compile error names the
part that is not), and the id decodes with that codec. It is the subject
`select`, `create`, `update` and `deleteFrom` take, and a `relate`
endpoint:

```ts
const reading = recordId(ReadingTable, [
  param('city', SurrealSchema.String),
  param('at', SurrealSchema.DateTime),
]);
select(ReadingTable, reading).only().build();
// SELECT * FROM ONLY type::record(s"reading", [$city, $at]);
```

`idRange(table, bounds)` is the records whose ids fall between two keys,
a `select` subject that reads many rows: `from` (included) or `after`
(excluded) starts it, `to` (excluded, as SurrealQL's `..` excludes it) or
`through` (included) ends it, and a side left out is open:

```ts
select(ReadingTable, idRange(ReadingTable, {
  from: [param('city', SurrealSchema.String), param('start', SurrealSchema.DateTime)],
  through: [param('city', SurrealSchema.String), param('end', SurrealSchema.DateTime)],
})).build();
// SELECT * FROM type::record(s"reading", [$city, $start]..=[$city, $end]);
```

Both render as `type::record`, whose key and bounds may be bindings:
SurrealDB 3.3 and 4.0 parse no binding as a scalar key in a record id or
range literal (`reading:$k`, `n:$a..$b`), and read the records
`type::record` names by key, never by a table scan. A `RELATE` endpoint
that is not a bare binding is parenthesised, which is the only way RELATE
parses `type::record(…)`. The conformance suite pins all of it.

`select(table, index, values)` looks a row up by a unique index of the
descriptor: `values` holds one operand per field of the index, a nested
field by its path, and is rendered as
`SELECT * FROM ONLY ⟨member⟩ WITH INDEX member_region_number WHERE region = $region AND number = $number LIMIT 1`.
An index that is not unique, a missing or extra field and a value that
never compares with its field are compile errors:

```ts
const byRegionNumber = select(MemberTable, 'member_region_number', {
  region: param('region', SurrealSchema.String),
  number: param('number', SurrealSchema.Int),
}).build();
// Statement<{ region: …; number: … }, Member | undefined>
```

`WITH INDEX` is there because, with `LIMIT 1`, SurrealDB 3.3 and 4.0 plan
such a lookup through any index on a prefix of its fields whose name sorts
first (`member_region` before `member_region_number`); the conformance
suite pins both. `select(readAs(table), index, values)` looks up through
the row a restricted session reads.

`.orderBy(…)` takes a field path of the stored row (`row.name`,
`row.address.city`, `at(row.pins, 0)`), the only thing SurrealDB parses
after `ORDER BY`; anything else is a compile error. SurrealDB resolves an
`ORDER BY` path against the projection before the stored row, so a
projection alias named like the field a path starts from must be that same
field (`age: row.age`); any other alias of that name is refused when the
statement is built.

A table name is always delimited where it is the subject of a statement
(`FROM`, `FROM ONLY`, `CREATE`, `UPDATE`, `DELETE`, `RELATE`), as `⟨name⟩`,
or with backticks when it holds `⟩` or `\`: bare, `from` and `ONLY` are
keywords there, `none` is NONE and `a.b` is a field path (SurrealDB 3.3 and
4.0 accept both delimiters in every one of those positions).

Data is always bound, and must fit the descriptor's schema for the operation
(`create`, `merge`, `content`): a value of another shape, or with a field the
schema does not have, is a compile error naming the field. That includes a
field inside a nested object or an array element (`settings.notify.x`,
`phones[*].x`): SurrealDB 3.3 and 4.0 refuse an undeclared key of a declared
child object at any depth, and 4.0 refuses one in an array element, which 3.3
stores and the element's closed schema then cannot read. An open object
(`FLEXIBLE`, or a schemaless table) accepts any key below it. The check
descends only where the value's type differs from the schema's, and stops at
a union. Builders are immutable, so a base query can be shared.

The `RETURN` clause decides the result, per verb and subject:

| `.returning(…)` | CREATE / RELATE | UPDATE | UPSERT | DELETE | INSERT |
| --- | --- | --- | --- | --- | --- |
| `'none'` | `undefined` | `undefined`, or `[]` for a table | the same | the same | `[]` |
| `'after'` (the default but DELETE) | the record | the record, `undefined` if missing | the record | `undefined` per record | each record |
| `'before'` | refused: always NONE | the record as it was | `undefined` for a record it created | the deleted record | `undefined` for a row it inserted |
| `'diff'` | the record's `Diff` | `Diff` per record | `Diff` | `Diff` per record | `Diff` per row |
| `(row) => ({ … })` | the projection | per record | per record | read from `$before.…` | per row |

`.returningValue((row) => expr)` is `RETURN VALUE`, one expression per
record (`$before.…` for a DELETE). `Diff` is an array of `PatchOperation`s,
the four SurrealDB writes: `add`, `remove`, `replace` (path `""` with the
whole record for one a statement created, NONE for one it deleted) and
`change`, a text patch of a string.

An `UPSERT` may create the record or change it, so its data must fit both
the create schema and the merge (or content) schema: what CREATE requires,
and nothing MERGE refuses, such as a READONLY field. An `UPSERT` of one
record takes no condition (SurrealDB does not evaluate it) and renders
nothing as `RETURN VALUE NONE`: SurrealDB 3.3 checks the data of
`UPSERT <record> … RETURN NONE` against the schema as if the record were
new, so a MERGE of some fields of an existing record fails there.

An `INSERT` row is the create row with an optional `id`; an edge of
`INSERT RELATION` also has its `in` and `out`, ids the relation declares.
`.onDuplicate((input) => ({ field: value }))` sets fields of the record a
row collides with, from the row the statement tried to insert (`$input.…`),
bindings and constants; each field must be one the merge schema has. The
stored record's own fields are not offered: SurrealDB 4.0 reads them there
and 3.3 reads them as NONE. The conformance suite pins every cell of the
table and both differences.

### Grouping

`.groupBy((row) => ({ role: row.role, city: row.address.city }))` groups
the selected records by named field paths, which every result row holds
(SurrealDB refuses a key the projection leaves out); `.groupAll()` makes
one group of every record. `.aggregate((row) => ({ … }))` adds the
aggregates of each group. Its `row` holds the stored fields as values only
an aggregate takes, so projecting a field that is neither grouped nor
aggregated is a compile error (SurrealDB would answer it with the array of
the group's values):

```ts
const cities = select(PersonTable)
  .where((row) => gte(row.score, 10))
  .groupBy((row) => ({ city: row.address.city }))
  .aggregate((row) => ({ people: count(), total: sum(row.score), names: arrayGroup(row.name) }))
  .orderBy('people', 'DESC')
  .build();
// ReadonlyArray<{ city: string; people: number; total: number; names: ReadonlyArray<string> }>
```

| Aggregate | SurrealQL | Result |
| --- | --- | --- |
| `count()`, `count(boolField)` | `count()` | int |
| `sum(field)` | `math::sum` | the field's number kind without its checks; the int 0 for no value, so a decimal sum that may be empty is `Decimal \| number` |
| `mean`, `stddev`, `variance` | `math::mean` … | float, NaN for no value |
| `median` | `math::median` | float, NONE for no value |
| `min`, `max` | `math::min`, `math::max` | the field's value, or ±Infinity for no value |
| `timeMin`, `timeMax` | `time::min`, `time::max` | the datetime, NONE for no value |
| `arrayGroup`, `arrayDistinct` | `array::group`, `array::distinct` | the distinct values, NONE among them for an optional field |

A group has no value of a field when every record holds NONE there, or
when `GROUP ALL` reads no record; the result types say so where SurrealDB
3.3 and 4.0 answer it differently from a value. `GROUP ALL` is always one
row. `arrayGroup` refuses an array field: SurrealDB 4 flattens the arrays of
a group and 3.3 does not. Aggregates take fields of the row, so none can
nest in another (4.0 refuses that, 3.3 yields 0). `.orderBy(alias)`,
`.limit` and `.start` follow; the conformance suite pins every result above.

### SPLIT and OMIT

`.split('pins')` makes one row per element of an array field, which holds
that element in the row; a record whose array is empty stays one row with
the empty array, so the field reads as `Element | Array` (and NONE stays
NONE for an optional field). Conditions come before SPLIT, so the builder
it returns has no `where`. A projection after it must hold the split field
under its own name: SurrealDB 4 resolves the SPLIT target as a projection
alias and 3.3 as the stored field, and both refuse a target the projection
leaves out. Sets are not split.

`.omit('ssn', 'notes')` leaves top-level fields out of `SELECT *`
(`SELECT * OMIT ssn, notes`), and out of the row type; conditions and sort
keys still read them. A projection names the fields it keeps instead, so
the two do not combine. The conformance suite pins both clauses.

### Blocks

`block()` writes a guarded, branching script in TypeScript instead of
SurrealQL text, and builds it into an ordinary statement: static text, a
codec per binding, a result codec derived from what the branches return,
and the `THROW` messages as typed `throws`.

```ts
import { block, call, create, eq, lt, ne, object, param, select, update } from 'effect-surrealkit/statement';

const amount = param('amount', SurrealSchema.Int);

export const pay = block()
  .let('wallet', () =>
    select(WalletTable, param('wallet_id', WalletId)).forUpdate().only().build())
  .throwIfNone('wallet', 'Wallet not found')          // later steps read $wallet without NONE
  .throwIf(($) => ne($.wallet.owner, param('payer', AccountId)), 'Not your wallet')
  .let('key', ($) => call(fn_payment_key, $.wallet.id, param('request', SurrealSchema.String)))
  .let('previous', ($) =>
    select(PaymentTable).where((row) => eq(row.key, $.key)).only().build())
  .ifNotNone('previous', (arm) =>
    arm.return(($) => ({ kind: 'replayed', payment: $.previous.id, amount: $.previous.amount })))
  .elseIf(($) => $.wallet.frozen, (arm) => arm.throw('Wallet is frozen'))
  .elseIf(($) => lt($.wallet.balance, amount), (arm) =>
    arm.return(($) => ({ kind: 'declined', balance: $.wallet.balance })))
  .else((arm) =>
    arm
      .let('balance', ($) => call(fn_debit, $.wallet.balance, amount))
      .let('payment', ($) =>
        create(PaymentTable).content(object({ wallet: $.wallet.id, amount, key: $.key })).build())
      .do(($) =>
        update(WalletTable, $.wallet.id).merge(object({ balance: $.balance })).returning('none').build())
      .return(($) => ({ kind: 'paid', payment: $.payment.id, balance: $.balance })))
  .build('pay');

// Result<typeof pay>:
//   | { readonly kind: 'replayed'; readonly payment: PaymentId; readonly amount: number }
//   | { readonly kind: 'declined'; readonly balance: number }
//   | { readonly kind: 'paid'; readonly payment: PaymentId; readonly balance: number }
// Thrown<typeof pay>: 'Wallet not found' | 'Not your wallet' | 'Wallet is frozen'
// Input: { wallet_id: WalletId; payer: AccountId; request: string; amount: number }
```

```sql
{
	LET $wallet = (SELECT * FROM ONLY $wallet_id FOR UPDATE);
	IF $wallet = NONE { THROW s"Wallet not found" };
	IF $wallet.owner != $payer { THROW s"Not your wallet" };
	LET $key = fn::payment_key($wallet.id, $request);
	LET $previous = (SELECT * FROM ONLY ⟨payment⟩ WHERE key = $key LIMIT 1);
	IF $previous != NONE { { kind: s"replayed", payment: $previous.id, amount: $previous.amount } } ELSE IF $wallet.frozen { THROW s"Wallet is frozen" } ELSE IF $wallet.balance < $amount { { kind: s"declined", balance: $wallet.balance } } ELSE { LET $balance = fn::debit($wallet.balance, $amount); LET $payment = (CREATE ONLY ⟨payment⟩ CONTENT { wallet: $wallet.id, amount: $amount, key: $key } RETURN AFTER); UPDATE ONLY $wallet.id MERGE { balance: $balance } RETURN NONE; { kind: s"paid", payment: $payment.id, balance: $balance } }
};
```

| Step | SurrealQL | Types |
| --- | --- | --- |
| `.let(name, ($) => value)` | `LET $name = value` | `value` is an expression or a built statement of one result; later steps read `$.name`, with a field expression per field when it holds a row (`$.wallet.balance`), NONE while the row may be |
| `.throwIf(($) => condition, message)` | `IF condition { THROW message }` | `message` joins `throws` |
| `.throwIfNone(name, message)` | `IF $name = NONE { THROW message }` | later steps read the local without NONE |
| `.do(($) => statement)` | the statement | its result must be NONE (`.returning('none')` on a record) |
| `.return(($) => value)` | `value`, last | an expression, or an object of expressions and primitives, which decodes as a struct; a constant on its own is a `literal` |
| `.throw(message)` | `THROW message`, last | contributes no result |
| `.if(($) => condition, (arm) => …)`, `.ifNotNone(name, (arm) => …)`, `.elseIf(…)`, `.else(…)` | `IF … ELSE IF … ELSE`, last | each branch is a block of its own; its locals are visible only in it |

The result is the union of what the branches can yield: a branch that
neither returns nor throws, and an `if` without `else` whose conditions all
fail, yield NONE, so the result includes `undefined`; a branch that throws
adds nothing. `object(fields)` is the same object as an expression, for
`CONTENT` and `MERGE` data built from locals, checked against the table's
schema as any data is.

A local is named like a SurrealQL parameter and may not be a local already,
a binding the block reads, or a name SurrealDB sets (`$auth`, `$value`,
`$after`, …): a compile error with the reason as its first line, except for
a binding only a later step reads, which is refused when the block is built
(every check runs again then). A select by id can read the record
`FOR UPDATE` (call `.forUpdate()` first): a transaction that read it fails
with a `StatementConflict` when a concurrent write changes the record before
it commits; no lock is held. SurrealDB 3.3 and 4.0 accept it only on a
record id held by a parameter, so the id is a binding or a local (`LET` a
field such as `$.invite.team` first).

The whole block is one SurrealQL statement, `{ … }`, and never writes
`RETURN`. SurrealDB 3.3.4 and 4.0.1 agree on what that gives
(`test/conformance.test.ts`):

- **All or nothing, even on its own.** Outside a transaction a `THROW` fails
  only its own statement and the statements after it still run; inside a
  block it stops the rest of the block and undoes what the block wrote. So a
  guard after a write is a guard in `run` too.
- **Nothing shared.** A `LET` inside a block is local to it, in a
  transaction as well, so a block's `defines` is empty: another statement
  of a transaction may bind a name the block uses for a local, and each
  reads its own value. A block's bindings follow the rules of any
  statement: one that another statement `LET`s, or that two statements bind
  to different values (the same block twice, say), is refused.
- **Safe anywhere in a transaction.** A `RETURN` inside an `IF` ends a
  transaction on SurrealDB 3, skipping what follows, and the SDK cannot see
  it; a branch's last expression is its value on both. For the same reason
  a statement that writes `RETURN` is refused as a local's value: in a
  `LET`'s value it ends the block on SurrealDB 4 and is the value on 3.

A block is also a value: a built block can be another block's local.
`test/consistent.ts` checks that the analyzer, reading a block's text, finds
exactly its bindings, defines, result count and messages; the suite applies
it to every block it builds (`test/blocks.ts`, whose two scripts run on both
engines in `test/block-engine.test.ts` over the schema in
`test/fixtures/scripts`, generated as `test/golden/scripts`). A block builds
without the analyzer, so builder bundles stay without it.

What blocks leave out: loops (`FOR`), which would need a body whose value is
discarded and a way to type what it changes; arithmetic and built-in
functions, which have no typed expressions yet (`call` a `DEFINE FUNCTION`,
or `unsafeExpr`); and the empty-result check of a `create` bound to a local,
since only a statement's last result is checked. A block costs the compiler
about what nine statements do (`test/budget.test.ts`).

### Authored SurrealQL

A multi-statement script stays SurrealQL. Describe each text as a `part`
(the `$names` it reads and does not define, the names it `LET`s, how many
results it returns, what it can `THROW`) and run parts in one scope with
`sequence`. `part` checks the description against the text with the
package's own analyzer when it is declared: a `free`, `defines`, `slots` or
`throws` that differs from what the text says is refused with the
difference, and so is a text with holes. The contract must name exactly the
names the sequence still needs: a missing or extra binding is a compile
error, checked again when the statement is defined.

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
statement with no such check, except that text which `LET`s a name at the
top level must declare `defines`, so a transaction can refuse a binding the
`LET` would shadow. `make` also takes `writes` (`true`: the text certainly
changes data or schema; `false`: it certainly does not; left out: unknown;
see [Running statements](#running-statements) for what each means when a
result does not decode) and `nonEmpty` (the last result is a row whenever
the statement applies, so an empty one is reported as
`StatementEmptyResult`, false unless declared); builders set both
themselves. Only `part` and `sequence` import the
analyzer, so code that uses only the builders does not bundle it.

### Statements from `.surql` files

`effect-surrealkit statements` writes the parts for you. `--input` holds one
directory per scope, each with `.surql` files; every scope becomes
`<out>/<scope>/statements.ts`, and `<out>/index.ts` re-exports each scope as a
namespace:

```sh
effect-surrealkit statements --input queries --out src/generated/statements
```

```text
queries/accounts/bindOwner.surql     LET $owner = $boundOwner; LET $principal = $auth.id;
queries/accounts/requireOwner.surql  IF $owner != $principal { THROW 'Not the owner' };
```

```ts
// src/generated/statements/accounts/statements.ts (excerpt)
export const bindOwner = $statement.part({
	name: "accounts.bindOwner",
	source: "accounts/bindOwner.surql",
	sql: `LET $owner = $boundOwner; LET $principal = $auth.id;`,
	free: ["boundOwner"],
	defines: ["owner", "principal"],
	slots: 2,
	throws: [],
	throwsAt: {},
} as const);
```

```ts
import { accounts } from './generated/statements/index.js';

const guarded = sequence(accounts.bindOwner, accounts.requireOwner).define({
  params: { boundOwner: PersonId }, // a missing or extra name is a compile error
  result: Schema.Undefined,
});
```

A file name must be a JavaScript identifier (letters, digits, underscores)
and not a reserved word; names that differ only in case, and anything in
`--input` other than scope directories of `.surql` files, are errors
(dotfiles are ignored). Output follows the emitter's rules: atomic writes,
unchanged files untouched, stale generated files removed, files without the
generated header never touched (nor overwritten), and `--check` writes
nothing and exits 1 when anything is missing, stale or edited. `--watch`
regenerates on change; `--import-extension .ts` changes the index's imports;
`--hole-open <text>` and `--hole-close <text>` change the delimiters of a
hole (default `__` and `__`). Any error stops
generation with its file, line and column; warnings are printed.

The analyzer is `effect-surrealkit/analyze`, pure and host-neutral:
`analyze(sql, { holes? })` returns `{ kind, sql, free, defines, slots,
throws, throwsAt, holes, provisional, transactional, writes, diagnostics }` or fails
with an `AnalyzeError` carrying a line and column.

- **Lexing.** `--`, `//`, `#` and `/* */` comments; `'…'` and `"…"` strings
  with SurrealQL's escapes, prefixed (`r` `d` `u` `s` `f` `b`); `` `…` `` and
  `⟨…⟩` identifiers; regex literals; `$name`. An unterminated string,
  identifier or comment, an invalid escape, unbalanced brackets, an empty
  statement or a character it cannot classify is an error, never a guess.
- **Free and defined names.** `free` is every `$name` read before anything in
  scope defines it, including a read before the `LET` of the same name.
  A top-level `LET` is in `defines` and visible to later statements and
  parts; a `LET` inside `{ }`, a `FOR $x IN …` variable, closure parameters
  (`|$a: int, $b|`) and `DEFINE FUNCTION` parameters are local to their body.
  `$auth`, `$access`, `$session` and `$token` are never free and a `LET` of
  one is an error; `$this`, `$self` and `$parent` are never free. `$value`,
  `$input`, `$before`, `$after` and `$event` are ordinary script names (free
  unless defined, with a warning, since `sequence` refuses them as
  bindings), except inside a `DEFINE` other than `FUNCTION` and `PARAM`,
  where the engine sets them.
- **Results.** Each terminated top-level statement is one result (`LET`,
  `IF`, `FOR`, `BEGIN` and `COMMIT` included). A text with its own `BEGIN`,
  `COMMIT` or `CANCEL` is `transactional`, which the client's
  `transaction()` cannot send. A `RETURN` inside `BEGIN … COMMIT` followed by
  another statement is an error: SurrealDB 3 skips what follows, 4 runs it.
- **THROW.** `throws` holds every string-literal message, at any depth;
  any other `THROW` is a `dynamicThrow` warning and reaches the client as a
  `StatementUnexpectedThrow`.
- **Writes.** `writes` is true when `CREATE`, `INSERT`, `UPDATE`, `UPSERT`,
  `DELETE`, `RELATE`, `DEFINE`, `REMOVE` or `ALTER` starts a statement or a
  subquery anywhere in the text, outside strings and comments. `part` and
  `sequence` carry it to the statement as `writes: true`, so a client that
  cannot decode the result says the writes are committed. No write found
  does not prove the text read-only (a function it calls may write), so a
  part is `writes: false` only when declared so.
- **Fragments.** A single expression or block without a final `;` is a
  `fragment`: no results, generated as a `text(…)` value, usable only to fill
  a hole. Several statements with an unterminated last one are an error.
- **Holes and templates.** Outside strings and comments, a name matching
  `[A-Z][A-Z0-9_]*` between two literal delimiters, `__NAME__` by default,
  is a hole; `{ holes: { open, close } }` (or the command's `--hole-open`
  and `--hole-close`) sets other delimiters, which are matched as text,
  never compiled into a pattern. A text with holes is a template, generated
  as `text(…)` with its `holes`; its `free` and `defines` are `provisional`,
  because a fill can read or define names. `fill(template, fills)` returns
  the filled text, failing on an unknown hole, a missing fill or a fill
  with holes of its own (unless `allowHoles`); each fill must lex on its own
  with balanced brackets. Analyse the result to get a part.

What it does not check: it lexes, it does not parse, so it does not know
whether a text is valid SurrealQL, what a statement returns (only how many
results), whether a fragment really is one expression, or what a function
body reads when it is called; a `DEFINE PARAM` is reported as a warning
and later reads of it stay free.

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

A transaction takes statements paired with their input by `bind`, which
checks the input against the statement's bindings; a hand-built
`{ statement, input }` does not compile.

`layer(db)` takes an SDK connection or session, and statements run with that
session's authentication. `make(db)` returns the same client as a plain
value, for an application that names its own services, one per session.

| Error | When | Was anything written? |
| --- | --- | --- |
| `StatementEncodeError` | a binding failed its codec, or its encoded value is not one the SDK sends without loss (`path` says where inside it) | no, nothing was sent |
| `StatementScopeError` | `transaction` refused its statements: a binding another statement `LET`s, one name bound to two different values, or a statement with its own `BEGIN`, `COMMIT` or `CANCEL` | no, nothing was sent |
| `StatementThrown<Message>` | a `THROW` the statement declares; in a transaction, `statement` names the one that declares it (`one of …` when several do); `source` and `lines` say where, when the part records them | not by a transaction; a single statement's earlier statements commit on their own |
| `StatementUnexpectedThrow` | a `THROW` whose message the statement does not declare (a dynamic message), with the thrown text as `reason` and the SDK's error as `cause` | as for `StatementThrown` |
| `StatementConflict` | a transaction conflict that `retry` did not resolve | no; send it again |
| `StatementRequestError` | any other failure of the request: transport, authentication, a SurrealQL error, or the SDK refusing to send (a `RETURN` followed by another statement in a transaction), with the SDK's error as `cause` | unknown for a transport failure; otherwise no |
| `StatementEmptyResult` | a `create` or `relate` that returns rows (`nonEmpty`) returned NONE or an empty list: SurrealDB reports a write the session may not make as an empty result, without an error, so the session may lack permission | not by that statement |
| `StatementDecodeError` | the answer of a statement or transaction known not to write (`writes: false`) is not what it declares: the number of results, a value where NONE belongs, or a last result its codec refuses | no |
| `StatementWriteOutcomeError` | the same, for a request not known to be read-only; `request` names the statement or transaction and `cause` is the `StatementDecodeError` (naming the statement and result), with `committed: true` and a message saying the request succeeded and either its writes are committed (`writes: true`) or it may have written (`writes` unknown) | yes, whatever it wrote is committed: read it back before resending |

An `update` or `delete` by condition may match nothing, so its empty list is
a result, not an error; so is a denied one (the conformance suite pins
both).

**An interrupted or timed-out write may have been applied.** Interrupting
the effect (a timeout included) aborts the request on the client, but the
engine may already have run it, or may still finish it. Read the data back
before sending a write again.

Before sending, every encoded binding is checked to be a value the SDK
sends without loss: the package's `SurrealValue` shape (plus `undefined`
for NONE, `ArrayBuffer` for bytes and the SDK's record id and range
values), with well-formed strings and integers the SDK can encode. The SDK
would otherwise send a class instance as its own fields, a `Map` as a
document, or a lone surrogate as U+FFFD. The check never throws; `{
unsafeSkipValueCheck: true }` turns it off.

`{ retry: schedule }` takes an Effect `Schedule` and sends the same,
already encoded request again while the engine reports a
`StatementConflict`; no other error is retried. SurrealDB 4 already retries
a single conflicting statement on the server, so this matters most for
transactions and for SurrealDB 3.

```ts
yield* transaction([bind(bump)], { retry: Schedule.recurs(3) });
```

Each run is a span named after the statement, carrying the static text,
the part sources (`code.file.path`), the number of rows when the result is
a list (`db.response.returned_rows`) and the error tag on failure
(`error.type`); never a binding value. `metrics` updates three Effect
metrics: `effect_surrealkit_statement_runs` (a counter by `statement` and
`outcome`, which is `success`, the error tag or `interrupted`),
`effect_surrealkit_statement_duration` (a timer by `statement`) and
`effect_surrealkit_statement_retries` (a counter by `statement`).

`transaction` sends its statements as one atomic request, joined one after
another (`make` stores every text with exactly one final `;`, so they always
parse). They share one variable scope, as SurrealQL has it, so before
anything is sent it refuses:

- a binding name two statements bind to values that are not certainly
  equal. Values compare structurally by kind: arrays, sets in any order,
  byte arrays, plain objects in any key order, SDK values by class and
  value, and primitives only with their own kind (`1` is not `1n`). What it
  cannot compare with certainty (a `Map`, a class instance, a cycle) is
  refused; the error names the binding and both statements, so rename one.
- a name one statement binds and another `LET`s at the top level, which one
  of them would read instead of its own value. A statement records the
  names it defines: `sequence` takes them from its parts, builders define
  none, and `make` takes them as declared (`defines`).
- a statement whose own text writes `BEGIN`, `COMMIT` or `CANCEL`
  (`transactional`; `make` takes it as declared).

The SDK refuses a top-level `RETURN` followed by another statement inside a
transaction, so put such a statement last.

A statement's `writes` is one of three states, and it decides what a
result that does not decode is reported as:

| `writes` | Set by | A result that does not decode |
| --- | --- | --- |
| `true`: certainly writes | write builders; `part` and `sequence` when the analyzer finds a write; `make({ writes: true })` | `StatementWriteOutcomeError`: the request succeeded and its writes are committed |
| absent: unknown | `select` with a condition, a projected expression or a bound count; authored text with no write found; `make` by default | `StatementWriteOutcomeError`: the request succeeded and may have written, and any writes it made are committed |
| `false`: certainly does not write | `select` of stored fields only; `part({ writes: false })`; a sequence whose every part is; `make({ writes: false })` | `StatementDecodeError`, and nothing was written |

A transaction certainly writes when any statement does and is read-only
only when every statement is. A sequence is the same over its parts, and
`define({ writes })` settles what the parts leave unknown; a declared
`writes` that contradicts the text (a part declared `false` whose text the
analyzer finds a write in, a sequence declared `false` with a writing
part) is refused when it is made. A caller that knows a `select` with
expressions is read-only says so with `.build(name, { writes: false })`.
Declare `false` only for known reads: it is what keeps a decode failure a
plain `StatementDecodeError`.

`StatementWriteOutcomeError`'s `request` names the statement or atomic
batch, and its `cause` identifies the failing statement and result slot.
Read back the receipt or record before deciding to replay. This outcome does
not prove that a row changed: permissions and conditional writes can yield
empty results.


### What the statements do not know

- **Permissions.** A generated `select` schema is the row a privileged
  reader gets. For a restricted session SurrealDB returns NONE for a field
  it may not read and leaves out a row it may not see, without an error
  (`test/conformance.test.ts` pins both, nested fields included). Build
  from `readAs(table)` to read the generated `visible` row instead, in which
  every field whose `select` permission is not FULL may be absent, so every
  condition, projection and result follows:

  ```ts
  const members = select(readAs(MemberTable)).build();
  // ReadonlyArray<{ id: MemberId; handle: string; email?: string; salary?: number; … }>
  ```

  Where the session's real row is narrower still, declare it and build
  from `readAs(table, schema)`, or project the fields it may read. For a
  generated SCHEMAFULL table, TypeScript checks the view's declared
  top-level field names against the descriptor. A view may refine a field's
  codec or brand; native record links must still encode to the declared
  table and key type. For example, `param('email', codecOf(row.email))`
  takes its input type from the visible field and preserves its brand
  through the statement. `refine` and `replaceField` change the `visible`
  row with the others.
- **Authored text.** The analyzer derives a part's names, result count and
  messages, not its result type: a result codec that does not match the
  script fails when it runs, not when it compiles.
- **Coverage.** `FETCH`, graph traversal, full-text and
  vector operators, live queries and views have no typed builder yet; use
  `unsafeExpr` or authored SurrealQL. `ON DUPLICATE KEY UPDATE` takes plain
  assignments only (no `+=`), and an aggregate takes a field, not an
  expression over fields.

The engine suite (`test/statement-engine.test.ts`) runs every builder against
the corpus schema and the modules generated from it, and passes on SurrealDB
3.3.4 and 4.0.1.

## Testing

`effect-surrealkit/testing` helps test code built on statements. Its memory
database needs `@surrealdb/node`, an optional peer dependency, with its own
peer `@surrealdb/node-native`; it is imported only when a memory database
starts, so nothing else in the package needs it installed:

```sh
npm i -D @surrealdb/node @surrealdb/node-native
```

```ts
import { Effect } from 'effect';
import { run } from 'effect-surrealkit/client';
import { expectIndex, memoryLayer, recordUser } from 'effect-surrealkit/testing';

const test = Effect.gen(function* () {
  const rows = yield* run(adults, { minimum: 18, city: 'Leeds' });
  yield* expectIndex(byEmail, { email: 'ann@example.com' }, 'person_email');
  const ann = yield* recordUser({ access: 'user', variables: { name: 'ann' } });
  const visible = yield* ann.client.run(visibleToUsers);
  return { rows, visible };
}).pipe(Effect.scoped, Effect.provide(memoryLayer({ schema })));
```

| Export | What it does |
| --- | --- |
| `memoryLayer({ schema?, namespace?, database? })` | `SurrealClient` and `TestDatabase` over a fresh in-memory engine in this process, with `schema` (SurrealQL text) loaded; closed with the layer. `memory(options)` is the same as a scoped effect. |
| `layerFrom(connection, { namespace, database })` | The same over a connection a test already has (a server, the suite's engine). |
| `loadSchema(text)` | Runs more schema text in the `TestDatabase`. |
| `recordUser({ access, variables? })` | Signs a new record user up through a `DEFINE ACCESS … TYPE RECORD` and returns `{ session, client }`, a client running as that session, closed with the scope: what a restricted session sees, row and field permissions included. |
| `expectIndex(statement, input, index)`, `expectNoIndex(…)` | Succeed when the engine's plan of a `SELECT` does (does not) read `index`; otherwise an `IndexExpectationError` naming the indexes it does read. `explain(statement, input)` returns the plan and its indexes. |
| `fake(script)`, `fakeLayer(script)` | A client with no engine that answers each statement by name: a value (the last result, as the SDK returns it), `result(value)`, `thrown(message)`, `conflict()`, `failure(message)`, or a function of the encoded bindings returning one. `calls` records every request. |

`expectIndex` runs the statement with `EXPLAIN` appended, which plans it
without running it. SurrealDB 3.3 and 4.0 answer with the same plan format, a
tree of `{ operator, attributes, children }` nodes in which a node that reads
an index names it as `attributes.index` (an `IndexScan` for `=`, a range, and
`ORDER BY` with `LIMIT`) and a table scan names none; `test/testing.test.ts`
pins the plan of each builder shape on both engines.

The scripted client is the real client over a scripted transport, so it
refuses what the real one refuses before anything would be sent: a binding
its codec or the value check refuses, the scope rules of a transaction, and,
with the SDK's own message, a transaction whose text has `BEGIN`, `COMMIT` or
`CANCEL` or a `RETURN` followed by another statement. A canned result is
decoded by the statement's codec, so a result of the wrong shape fails as it
would against an engine; a statement the script does not name fails as a
request.

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
the JSON SurrealDB 3.2 and 3.3 render. Declared kinds recover their native
representations where the JSON retains enough information. Open values stay
in their JSON representation. Derive projections with
`Schema.Struct(Struct.pick(PersonJsonFields, ['name', 'born']))`. The table
below describes how the codecs handle JSON conversions.

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
| `float` | `Float` (native IEEE-754, including NaN, infinities and signed zero) / `Schema.Finite` (JSON) | `number` |
| `decimal` | `Decimal` | SDK `Decimal` |
| `number` | `SurrealNumber` / `SurrealNumberFromJson` (finite JSON numbers or decimal strings) | `number \| bigint \| Decimal` |
| `any` | `Schema.UndefinedOr(SurrealValue)` / `SurrealJsonValue` (JSON) | `SurrealValue \| undefined` / `SurrealJsonValue` |
| open `object` | `Schema.Record(Schema.String, SurrealValue)` / `Schema.Record(Schema.String, SurrealJsonValue)` (JSON) | string-keyed record of open values |
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
golden instead of being pinned twice), the field-presence evidence tables and
the `members` fixture (`test/fixtures/members`: field permissions and unique
indexes, compiled with `surrealkit compile` in that directory and applied by
`test/members.test.ts`). The corpus
inputs are real `surrealkit compile` output, pinned byte for byte by
SurrealKit's own test suite under `typegen_corpus/compiled/`, and every input
document is validated against the published JSON Schema. The golden
comparison ignores the emitter version in `manifest.ts` (and SurrealKit's pin
ignores the SurrealKit and engine versions), so a release does not mean
regenerating them.
`test/types` compiles the golden modules with positive and `@ts-expect-error`
fixtures under `strict` and `exactOptionalPropertyTypes`.
`test/budget.test.ts` type-checks 300 generated statements (`test/budget-fixture.ts`:
selects with projections and conditions, writes, sequence contracts) and
holds their cost, in the compiler's instantiation and type counters, under a
fixed limit.

The engine, statement, field-presence and conformance suites need a real
engine. A plain `bun run test` runs them on the in-process, in-memory engine of
`@surrealdb/node` (SurrealDB 4.0.1; a dev dependency). Set one of these to run
them against a server instead, which is also the only way to run the JSON
round trips over HTTP `/sql`; the embedded engine has no HTTP endpoint, so
those are skipped (with a notice) without a server:

```sh
SURREAL_BIN=/usr/local/bin/surreal bun run test          # starts `surreal start … memory`
SURREAL_URL=ws://127.0.0.1:8000/rpc bun run test         # a throwaway container, root/root
```

`test/conformance.test.ts` pins engine and SDK behaviour the documentation
does not state and the statement layer relies on: what each statement
contributes to a request's results, the stateless transaction, unbound and
protected parameters, missing records and tables, `LIMIT`/`START`/`TIMEOUT`
with bindings, `RETURN` modes, and what a record user sees where permissions
deny a table, a write, a field or a function. It names the mode and engine
version it ran on, and where SurrealDB 3 and 4 differ it asserts each.

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
