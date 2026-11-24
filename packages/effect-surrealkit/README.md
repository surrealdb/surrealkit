# effect-surrealkit

[Effect](https://effect.website) v4 schemas for SurrealDB values exactly as
the JavaScript SDK (`surrealdb` 2.x) decodes and encodes them, an emitter
that turns a SurrealKit schema document v2 into typed Effect modules, and
typed statements built from those modules.

```sh
npm i effect-surrealkit effect@4.1.5 surrealdb@2.2.2
```

The peers are `effect` 4.1.5 or later and `surrealdb` 2.2.2 or later, and the
package needs Node 22.21.1 or later where it runs on Node.

Every runtime export imports only `effect` and `surrealdb` value classes, so it
runs in browsers, Workers, Bun and Node. The emitter (`effect-surrealkit/emitter`)
is host-neutral too; only the `effect-surrealkit` bin and the `.surql` loaders
(`effect-surrealkit/mapper`, `effect-surrealkit/register`) touch the filesystem,
and only [`effect-surrealkit/testing`](#testing) can load `@surrealdb/node`.

- [Generating modules](#generating-modules) from a schema document
- [Statements](#statements): typed queries and writes from the generated modules,
  [statements from `.surql` files](#statements-from-surql-files), and
  [importing `.surql` files](#importing-surql-files-the-content-mapper) with no
  generated files
- [Testing](#testing) with an in-memory database, record users, index
  expectations and a scripted client
- [Two codec worlds](#two-codec-worlds) and the [value schemas](#values)
- [What the codecs refuse](#what-the-codecs-refuse-and-why)

Coding agents: the [effect-surrealkit skill](../../skills/effect-surrealkit/SKILL.md)
holds the rules for writing, generating, running and testing statements, each
citing the section of this README, the conformance test or the SurrealDB
documentation it comes from.

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
kebab-case words, so no table can be written there. The module also exports
the registry's type, `Store`, whose entries are `typeof PersonTable` and the
like: a declaration file then names the descriptors, where an inferred type
would copy every one of them and fail with TS7056 on a large schema. A group
with no entries is typed `SurrealSchema.NoEntries`, a named empty type, so
the module does not write `{}` where lint rules report it. `Store` is not
re-exported from `index.ts`, where a table named `store` owns that name.

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

### What changes for existing users' requirements

- The `effect` peer range is `^4.1.5`, raised from `^4.0.6`. A project on
  `effect` 4.0.6 to 4.1.4 sees a peer warning, or a refused install under
  strict peer checks, until it moves to 4.1.5 or later.
- `engines.node` is `>=22.21.1`, raised from `^20.19.0 || >=22.12.0`, and the
  same holds for `vite-plugin-surrealkit`. Node 20 and Node 22 before 22.21.1
  are no longer supported or tested; CI tests Node 22.21.1 and 24. 22.21.1 is
  the first Node 22 release on which the `.surql` imports work without a flag
  (see [the content mapper](#importing-surql-files-the-content-mapper)).
- The `surrealdb` peer range is `^2.2.2`, raised from `^2.1.1`. The client
  relies on what SDK 2.2.2 does: on the embedded engine SDK 2.1.1 fails 10 of
  the package's tests (no `cbor.2` negotiation, no `Float16Array` codec,
  negative zero sent as positive zero), 2.2.0 fails 3 and 2.2.1 fails 1. A
  project on SDK 2.1.x or 2.2.0 to 2.2.1 moves to 2.2.2 or later.

### What changes for existing users of the emitter

Regenerating with this version adds to the output and tightens one schema
(below); every existing export keeps its name and its type:

- every table descriptor gains `fieldPermissions`, `indexes` and `visible`,
  and every table module a `<Table>Visible` export (with its `Type` and
  `Encoded` types), so a table whose PascalCase name is another table's
  plus `Visible` (`person` and `person_visible`) now needs a `names` entry;
- a new module, `_store.ts`, exports `store` and its type `Store`, and
  `index.ts` re-exports `store`.
  A SurrealKit-run emitter records the new file in `.surrealkit-emit.json`
  as it does every generated file.

Code that spells out a descriptor's full type, or compares one with
`toEqual`, sees the new keys. Hand-written descriptors without them keep
working with every builder.

One schema is stricter at run time, with the same type: a closed object with
no fields (the Create, Merge and Content of a SCHEMAFULL table whose only
field is its id, or a `{}` literal kind) is now `SurrealSchema.EmptyObject`
instead of `Schema.Struct({})`, so a value other than `{}` fails to decode or
encode instead of reaching SurrealDB (see
[Interpretation notes](#interpretation-notes)). The generated modules need
an `effect-surrealkit` that exports `EmptyObject`.

### What changes for existing users of the client

- A `CREATE` or `INSERT` of a record id that already exists now fails with
  `StatementRecordExists` instead of `StatementRequestError`, whatever
  made the statement: a write builder, a `block`, a `family`, `part`,
  `sequence` or `make`. `StatementRecordExists` is in every statement's
  error type, a `select` of stored fields included: leaving it out of a
  read would need one more type parameter on every statement, which this
  version does not add.
- A write builder (`create`, `insert`, `insertRelation`, `update`, `upsert`,
  `relate`) over a table with a unique index now fails with
  `StatementIndexConflict` on a duplicate value instead of
  `StatementRequestError`, on the engine versions the client reads (see
  [A value that may already be taken](#a-value-that-may-already-be-taken)).
- `Statement` takes a fourth type parameter, the unique indexes it
  declares, and `StatementError` a second. Left out, each means any
  index, so `Statement<P, R>`, `Statement<P, R, Throws>`, a bare
  `Statement` and `StatementError<Throws>` accept the statements and
  errors they did before. A type compared exactly with `toEqualTypeOf` (or
  similar) sees `never` where a statement declares no unique index.

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
- A closed object with no fields to print is `SurrealSchema.EmptyObject`,
  never `Schema.Struct({})`: the Create, Merge and Content inputs of a
  SCHEMAFULL table with no writable fields, a declared object whose children
  are all excluded from an operation, and the `{}` literal kind. Effect 4's
  `Schema.Struct({})` checks only that a value is not `null` or `undefined`,
  so it would pass `{ stray: 1 }`, a string or an array through unchanged
  (even with `onExcessProperty: "error"`), and an encoded binding would carry
  the stray keys to SurrealDB, which refuses them (`Found field 'stray', but
  no such field exists`; ``Expected `{  }` ``). `EmptyObject` is
  `Schema.Struct({}).check(isEmptyObject())`: it accepts exactly `{}` in both
  directions, its JSON Schema requires `{ "type": "object",
  "additionalProperties": false }`, and it stays a `Schema.Struct` whose type is `{}`, so a caller
  that passes `{}` and `refine` work as before. An open row keeps
  `Schema.Struct({})` as the base of its `Schema.StructWithRest`, where the
  record rest holds the keys.
- `regex` and `function` kinds become `Unsupported(kind)` (with a warning):
  the values have no SDK representation.

### Printer: SchemaRepresentation versus a template printer

The emitter prints modules with its own small template printer. The
alternative was to build live schemas from the codecs, convert them with
`SchemaRepresentation.toRepresentation`, and print them with Effect's code
generator (`Codegen.Module.addSchemas` and `finalize` in 4.1.5). Every codec
carries `toCode` metadata for this. The emitter uses templates because:

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
| `parent(row.field)` | `$parent.field`: the select's row, read from a step's `where` ([details](#record-links-and-graph-traversal)) |
| `literal(value)`, `literal(value, codec)` | a constant in the text (`s"…"`, `d"…"`, `r"…"`), every part checked as its codec checks a binding |
| `eq` `ne` `lt` `lte` `gt` `gte` | `=` `!=` `<` `<=` `>` `>=`; the right side may be a primitive |
| `and(…)` `or(…)` `not(x)` | `AND` `OR` `!` |
| `isNone(x)` `isNotNone(x)` | `x = NONE`, `x != NONE` |
| `inside(x, xs)` `contains(xs, x)` | `INSIDE`, `CONTAINS`; `xs` may be an array of primitives |
| `at(array, i)` | `array[i]`, or NONE past the end; an element of a field path is a field path |
| `add` `sub` `mul` `div` | `+` `-` `*` `/` on numbers; the right side may be a number |
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

Arithmetic follows the engine (SurrealDB 3.3 and 4.0 agree, as
`test/conformance.test.ts` pins): two ints give an int, and an overflow of
64 bits fails the statement; an int and a float, or two floats, a float;
any decimal operand a decimal. `div` of two ints truncates toward zero
(`7 / 2` is `3`), and division by zero or NONE is the float NaN or
±Infinity whatever the operands, so a quotient decodes as its kind or a
float. The result codec is chosen from the operands' codecs (`Int`,
`Int64`, `Float`, `Decimal`, `SurrealNumber`, a numeric literal by its
value); the compiler sees ints and floats as `number` alike. An operand
that may be NONE, a string or any other value the engine refuses is a
compile error. The SDK sends a whole JS number as an int whatever its
codec, so a float binding is cast (`<float>$f / 2`).

Operands compare when their stored types overlap: a branded or refined
binding compares with its column, a string never compares with an int. A
binding is named, so the same expression can be used twice; one name with
two codecs is refused, as are names SurrealDB sets itself (`$auth`, `$this`,
`$value`, …).

### Built-in functions

`effect-surrealkit/functions` has a typed function for each of 406 SurrealDB
built-ins, one namespace per family. Each returns an expression, so it fits
anywhere the builders take one:

```ts
import * as SurrealSchema from 'effect-surrealkit';
import { array, string } from 'effect-surrealkit/functions';
import { param, select } from 'effect-surrealkit/statement';

const people = select(PersonTable)
  .where((row) => string.isEmail(row.email))
  .project((row) => ({
    short: string.slice(row.name, 0, param('length', SurrealSchema.Int)),
    first: array.first(string.words(row.name)),
  }))
  .build('people');
// people.sql: SELECT string::slice(name, 0, $length) AS short,
//             array::first(string::words(name)) AS first
//             FROM ⟨person⟩ WHERE string::is_email(email);
// ReadonlyArray<{ short: string; first: string | undefined }>
```

An argument is an expression whose stored type fits the argument's kind, or
a primitive (or an array of primitives) written with `literal`; anything else
is a compile error, and a value still enters only as a binding or a checked
constant. Trailing optional arguments may be left out (`string.slice(s)`),
a variadic one takes any number of values (`string.join('-', a, b)`), and a
constant has no call (`math.pi` is `math::pi`). An argument that is not a
single term is parenthesised.

The functions are generated from one table,
[`src/functions/signatures.json`](src/functions/signatures.json): per
function its name, its arguments (name, kind, optional), whether the last is
variadic, its result kind, the engine majors it exists in and sample
arguments. Kinds are `any`, `string`, `int`, `int64`, `float`, `number`,
`decimal`, `bool`, `datetime`, `duration`, `uuid`, `bytes`, `record`,
`object`, `geometry`, `range`, `array`, `array<K>` and `option<K>`, each
decoded by the package's codec of that name (`any`, and an element of
`array`, may be NONE).
A result `same:N` is argument `N`'s own codec, used where the result is a
reordering or subset of an array (`array.sort`, `array.distinct`); `item:N`
is an element of it or NONE (`array.first`, `time.max`). `items:N`, on the
variadic argument `N`, is an array of the union of the arguments' element
codecs when every argument is a typed array, the argument's own codec when
it is the only one, and `array` otherwise (`array.concat`). The other
array builders are typed the same way: `with:N,M` is an array of argument
`N`'s elements and the value `M` (`array.append`, `push`, `prepend`,
`insert`, `fill`), `adds:N,M` the same with an array `M` adding its
elements (`array.add`), `elements:N,M` the elements of both arrays
(`array.union`, `difference`), `padded:N,M` those or `null` past the
shorter one (`array.logicalAnd`, `logicalOr`, `logicalXor`), `pairs:N,M`
tuples of one element of each (`array.combine`), `flat:N` the elements of
the arrays `N` holds (`array.flatten`, `group`), `chunks:N` arrays of `N`'s
elements (`array.clump`, `windows`), `transposed:N` arrays of the inner
elements or NONE (`array.transpose`) and `repeats:N` an array of the value
`N` (`array.repeat`). When an argument they name is not a typed expression
(an inlined constant, or an open value or array of open values), the
result is the plain `array` or `array<array>` it was. A result
`float|decimal:N,…` is a float, or `number | Decimal` when one of the
arguments it names may hold a decimal (an element of it, for an array): the
functions SurrealDB computes in the kind of their input (`math.sqrt`,
`math.mean`, `math.stddev`, `math.variance`, `math.lerp`,
`math.lerpangle`, `vector.similarityCosine`, `vector.distanceCosine`).
`math.sqrt(row.amount)` over a decimal field is `number | Decimal`, over an
int field `number`; even a decimal input can give a float (the spread of one
value is the int 0, the root of a negative decimal NaN). A function that
answers NONE for some input is `option<K>` (`parse.emailHost` of a string
that is not an address, `math.median([])`, `vector.sum([])`). After changing
the table, run:

```sh
npm run generate:functions   # node scripts/generate-functions.mjs
```

It writes `src/functions/generated/` (a file per family, `codecs.ts` and
`index.ts`), sorted and Biome-formatted; `test/functions.test.ts` fails when
those files differ from what the table generates. A name is its family's
namespace and the rest of the path in camelCase: `string::is_email` is
`string.isEmail`, `vector::distance::euclidean` is
`vector.distanceEuclidean`, an underscore between two digits stays
(`math::log10_2` is `math.log10_2`) and a reserved word gets a trailing `_`
(`rand::enum` is `rand.enum_`).

| Family | Functions | Family | Functions |
| --- | --- | --- | --- |
| `array` | 59 | `object` | 8 |
| `bytes` | 1 | `parse` | 9 |
| `crypto` | 14 | `rand` | 12 |
| `duration` | 18 (1 constant) | `record` | 5 |
| `encoding` | 6 | `search` | 1 (`analyze`) |
| `geo` | 7 | `string` | 59 |
| `math` | 63 (21 constants) | `time` | 45 (3 constants) |
| `meta` | 2 | `type` | 72 |
| | | `value` | 2 |
| | | `vector` | 23 |

`test/functions-engine.test.ts` calls every entry with its sample arguments,
and with each of its `variants`, on the suite's engine. The variants give a
numeric argument an int, a float and a decimal, a list argument an empty
list, a parsing function a string that does not parse and a string argument
the empty string (only those every listed engine accepts are kept), and a
numeric literal argument carries the codec of its own kind, so a result
derived from an argument is checked for each kind. On a major the entry
lists, every result must decode with the codec the function derives; on one
it does not, the engine must reject the sample call as an unknown function. 32 functions exist only on SurrealDB 4.0:
`time::age`, `time::make_date`, `time::make_time`, `time::make_timestamp`,
`time::trunc`, `vector::distance::cosine`, and `type::date`, `type::time`,
`type::timestamp`, `type::interval`, `type::i8`, `type::i16`, `type::i32`,
`type::u8`, `type::u16`, `type::u32`, `type::u64`, `type::f16`, `type::f32`
with their `type::is_*` checks. None exists only on 3.3. Non-deterministic
functions (`rand::*`, `time::now`, salted hashes) are checked by decoding
alone.

Over the `cbor.2` protocol, which SDK 2.2.2 negotiates, SurrealDB 4 returns
17 of these results as SDK classes of their own: `PlainDate`, `PlainTime`,
`Timestamp`, `Interval` and the sized numbers. Their functions keep the result
types the table records and decode the same values as over `cbor`: a date
becomes midnight UTC (an infinite date becomes the engine's datetime bound), a
time becomes a duration, a timestamp a datetime, an interval its ISO 8601
text, and a sized number a float or a safe integer. A raw query still returns
the SDK classes.

Not in the table: functions that take a closure (`array::map`,
`array::fold`, `array::reduce`, `value::chain`, `value::expect`); the
full-text and vector functions that need a match reference or a KNN operator
(`search::score`, `search::highlight`, `search::offsets`, `search::rrf`,
`search::linear`, `vector::distance::knn`); files (`type::file`, `file::*`);
and what writes, sleeps, uses the network or reads the session (`http::*`,
`sleep`, `session::*`, `api::*`, `sequence::*`, `ml::*`, `ext::*`). The
`set::*` family and the aggregate `count()` are not covered yet.

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
A nested path reads through an object field, optional or not. An index
that is not unique, a missing or extra field, a value that never compares
with its field, and a path through a field that is not one object (a
union of objects, a nullable object) are compile errors, and are refused
when built:

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

A lookup by a value that may be an array, on a field that may hold an
array or another kind (`any`, `array<int> | string`, `array<int> |
set<string>`), is a compile error and is refused when built: SurrealDB 3.3
finds no array through a unique index on such a field (it answers NONE,
even on a schemaless table), and 4.0 finds the row. An array-only field
(`array<int>`, `option<array<int>>`, `array<int> | array<string>`) and a
value that is not an array work on both; `test/lookup-engine.test.ts` runs
them on both engines.

`.orderBy(…)` takes a field path of the stored row (`row.name`,
`row.address.city`, `at(row.pins, 0)`), the only thing SurrealDB parses
after `ORDER BY`; anything else is a compile error. SurrealDB resolves an
`ORDER BY` path against the projection before the stored row, so a
projection alias named like the field a path starts from must be that same
field (`age: row.age`); any other alias of that name is refused when the
statement is built. `.orderByAlias(name, direction)` sorts by a name the
result row holds instead: an alias of the projection (a computed value
such as a search score) or a field of `SELECT *`; a name the row does not
hold is a compile error.

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
record takes no condition (SurrealDB does not evaluate it). Every `UPSERT`
renders nothing as `RETURN VALUE NONE`, because with `RETURN NONE`
SurrealDB 3.3 treats the statement as if no record were there:
`UPSERT <record> … RETURN NONE` checks its data against the schema as if
the record were new, so a MERGE of some fields of an existing record fails,
and `UPSERT <table> … WHERE … RETURN NONE` changes the records the
condition holds for and creates one more. `RETURN VALUE NONE` of a table
answers one NONE per record, which decodes as the empty list.

An `INSERT` row is the create row with an optional `id`; an edge of
`INSERT RELATION` also has its `in` and `out`, ids the relation declares.
`.onDuplicate((input) => ({ field: value }))` sets fields of the record a
row collides with, from the row the statement tried to insert (`$input.…`),
bindings and constants; each field must be one the merge schema has. The
stored record's own fields are not offered: SurrealDB 4.0 reads them there
and 3.3 reads them as NONE. The conformance suite pins every cell of the
table and both differences.

#### A value that may already be taken

SurrealDB has its own statements for a row whose id or unique value may
already be in the table, and they are the first choice when that is
expected:

- `upsert(table, id)` changes the record when it is there and creates it
  when it is not; `upsert(table).where(…)` does the same for the record a
  condition finds, so a condition on a unique field changes the record
  that holds the value.
- `.onDuplicate(…)` (`INSERT … ON DUPLICATE KEY UPDATE`) updates the record
  a row collides with, by id or by a unique index.
- `INSERT IGNORE` skips such a row and inserts the others. No builder
  writes it; use authored text (`part`, `make`).

A write that expects the value to be free fails instead, and the client
says why with a typed error. Each of `create`, `insert`, `insertRelation`,
`update`, `upsert` and `relate` declares the unique indexes of its table
(`uniqueIndexes`, from the descriptor's `indexes`), so a duplicate on one is
a `StatementIndexConflict` whose `index` is typed as the union of their
names; a table with no unique index adds none to the statement's errors,
and `deleteFrom` declares none. A `CREATE` or `INSERT` of an id that exists
is a `StatementRecordExists`:

```ts
const added = run(createPerson, { person }).pipe(
  Effect.catchTag('StatementIndexConflict', (error) =>
    Effect.fail(new EmailTaken({ index: error.index })), // error.index: 'uniq_email'
  ),
);
```

The duplicate is read from the engine's message, which is pinned on
SurrealDB 3.3.4 and 4.0.1 only (`test/conformance.test.ts`, "constraint
errors"); [surrealdb/surrealdb#7652](https://github.com/surrealdb/surrealdb/issues/7652) asks SurrealDB to report it as a structured
`AlreadyExists` error instead. On any other version a duplicate may arrive as a
`StatementRequestError`, and `Effect.catchTag('StatementIndexConflict', …)`
then catches nothing. Prefer `upsert`, `INSERT IGNORE` and
`.onDuplicate(…)` wherever they fit, and when you upgrade the engine run
`checkConstraintErrors` from `effect-surrealkit/testing` against it: it
reports whether each error is still classified. A record that exists is
read from a structured error, not a message.

On SurrealDB 3.3 and 4.0 (the same conformance rows, on both):
`INSERT IGNORE` raises neither error. `ON DUPLICATE KEY UPDATE`
still raises `StatementIndexConflict` for a taken unique value given under
another explicit id, and for a value its own assignments make a duplicate,
so `.onDuplicate(…)` keeps the declaration. `RELATE` onto an existing edge
id is a different error on each version and stays a `StatementRequestError`.

#### Paging through a table

Page by id range, not by `START`. Each page starts after the last id the
one before returned, built from `idRange` and `limit`:

```ts
const rows = param('rows', SurrealSchema.Int);
const firstPage = select(ReadingTable, idRange(ReadingTable, {}))
  .limit(rows)
  .build();
// SELECT * FROM type::record(s"reading", ..) LIMIT $rows;
const nextPage = select(ReadingTable, idRange(ReadingTable, {
  after: [param('city', SurrealSchema.String), param('at', SurrealSchema.DateTime)],
}))
  .limit(rows)
  .build();
// SELECT * FROM type::record(s"reading", [$city, $at]>..) LIMIT $rows;
```

Pass the key parts of the last row's id as the bindings of the next page,
and stop at a page that comes back empty. A range returns its records in
the order of their ids (SurrealDB's `SELECT` documentation, "Record
ranges", read 2026-11-23), so it needs no `ORDER BY`.

`.orderBy((row) => row.id).start(skip)` gives the same rows on a table
nobody writes to, at a cost and with a fault. The cost is that the engine
reads past every skipped row: on SurrealDB 3.3.4 and 4.0.1 it plans as a
`TableScan` with the offset pushed into it. A range reads by key: the
builder's `type::record` range plans as `Values` then `Limit` on 4.0.1 and
as `DynamicScan` on 3.3.4, and a literal range (`table:key..`) as
`RecordIdScan` then `Limit` on both. On 100,000 records, a page of three
rows after id 99,990 took 57 ms by `START` and 0.6 ms by range on the
embedded 4.0.1 engine, and 6.1 ms and 1.9 ms on a 3.3.4 server. The fault
is that a row removed from a page already read shifts every later offset,
so the next `START` page skips a row. The range page starts after an
id, so it does not.

Do not add `ORDER BY id` to a range page: on 4.0.1 it sorts the whole rest
of the range before `LIMIT`. The conformance suite
(`test/conformance.test.ts`, "paging") pins the plans of these statements
on both versions, the order of a range, the sort, and the skipped row.

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
| `mean` | `math::mean` | float whatever the field, NaN for no value |
| `stddev`, `variance` | `math::stddev`, `math::variance` | float; over a field that may hold a decimal, `Decimal \| number` (one record spreads by the int 0); NaN for no value |
| `median` | `math::median` | the middle value or a float of the two middle ones: float, or `Decimal \| number` over a field that may hold a decimal; NONE for no value |
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
it commits; no lock is held. SurrealDB 3.3 and 4.0 accept it only on one
record id given as a parameter (a binding or a local), a literal record id
or a `recordId(table, key)` (`type::record(…)`), and a concurrent write
conflicts with the read through each of the three
(`test/block-engine.test.ts` races one against each). They refuse a field
path, a subquery, a table and a range, so `LET` a field such as
`$.invite.team` first.

`test/block-engine.test.ts` races a write against a block that reads a
wallet `FOR UPDATE` and then records its balance elsewhere. Without the
lock the block commits with the balance it read, which has since changed.
With it, the block in a `transaction` fails with a `StatementConflict` on
SurrealDB 3.3 and 4.0 and writes nothing. Run alone, it fails the same way
on 3.3, while 4.0 runs the statement again itself and the block reads the
new balance. With `{ retry: schedule }` both forms succeed on both, having
read the balance after the write.

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

The [built-in functions](#built-in-functions) are expressions like any
other, so they fit a guard (`.throwIf(($) => lt(math.abs($.wallet.balance),
1), …)`), a local, the result, and the conditions and projections of the
statements a block binds, a grouped select's condition included;
`test/block-engine.test.ts` runs such a block on both engines. A local
also holds what an `upsert` or an `insert` returns (the record, or the
list of inserted rows), so a later step can read the upserted record's id
and the inserted rows; a write that fails, such as an insert on a taken
unique key, undoes the whole block.

What blocks leave out: loops (`FOR`), which would need a body whose value is
discarded and a way to type what it changes; arithmetic operators (`+`,
`-`, `*`, `/`), which have no typed expressions yet (use a built-in such as
`math.fixed`, `call` a `DEFINE FUNCTION`, or `unsafeExpr`); and the
empty-result check of a `create` bound to a local, since only a statement's
last result is checked. A block costs the compiler
about what nine statements do (`test/budget.test.ts`).

### Search

Full-text matches, nearest-neighbour searches and fused rankings name their
table and field, so the compiler checks the index the engine needs against
the descriptor's `indexes`. What SurrealDB parses only as a literal (the
match reference, the neighbour count, the search effort, the distance) is a
validated literal; the search terms and the query vector are bindings.

```ts
import {
  evaluate, highlight, knn, knnDistance, match, param, rrf, score, select,
} from 'effect-surrealkit/statement';

const words = match(DocTable, 'body', param('terms', SurrealSchema.String), { ref: 1 });
const byWords = select(DocTable)
  .where(() => words)
  .project((row) => ({ id: row.id, score: score(words), marked: highlight(words, '<b>', '</b>') }));
byWords.build('by words').sql;
// SELECT id AS id, search::score(1) AS score, search::highlight(s"<b>", s"</b>", 1) AS marked
//   FROM ⟨doc⟩ WHERE body @1@ $terms;

const near = knn(DocTable, 'embedding', 'query', { k: 5, ef: 40 });
const byMeaning = select(DocTable)
  .where(() => near)
  .project((row) => ({ id: row.id, distance: knnDistance(near) }));
// SELECT id AS id, vector::distance::knn() AS distance FROM ⟨doc⟩ WHERE embedding <|5,40|> $query

const fused = evaluate(rrf([byMeaning.toExpr(), byWords.toExpr()], { limit: 10 }));
// RETURN search::rrf([(SELECT …), (SELECT …)], 10, 60);
// ReadonlyArray<{ id: DocId; distance?: number; score?: number; marked?: string; rrf_score: number }>
```

| Function | SurrealQL |
| --- | --- |
| `match(table, field, terms, { ref?, operator? })` | `field @ref@ terms`, `@ref,AND@`, `@OR@`, `@@`; a compile error unless a `FULLTEXT` index covers `field`; `ref` is a literal 0 to 255 |
| `score(m)`, `highlight(m, open, close)`, `offsets(m)` | `search::score(ref)` (a float), `search::highlight(open, close, ref)` (marked text; a list of it for a list field; the index must keep `HIGHLIGHTS`), `search::offsets(ref)` (`{ [element]: [{ s, e }] }` in characters, or NONE) |
| `knn(table, field, vector, { k, ef })` | `field <|k,ef|> $vector` through the field's `HNSW` or `DISKANN` index (a compile error without one) |
| `knn(table, field, vector, { k, distance })` | `field <|k,DISTANCE|> $vector` by brute force: `EUCLIDEAN`, `COSINE`, `MANHATTAN`, `CHEBYSHEV`, `HAMMING`, `INNER_PRODUCT`, `COSINE_NORMALIZED` or `['MINKOWSKI', p]`, each computed the same by SurrealDB 3.3 and 4.0; `JACCARD` and `PEARSON` are refused (see below) |
| `knnDistance(hit)` | `vector::distance::knn()`, the distance of each hit |
| `rrf(lists, { limit, k? })`, `linear(lists, { weights, limit, normalisation })` | `search::rrf(…)`, `search::linear(…)` over two or more subqueries whose rows have an `id` |
| `evaluate(expr)` | `RETURN expr;` as a statement, for a fused ranking |

The query vector is a binding name (its value an array of floats) or a
binding of your own; its length is checked against the index dimension
when it is encoded, and by the compiler when its codec is a tuple. `k` and
`ef` are positive integer literals, `ref` an integer literal from 0 to 255:
a binding, a fraction or a value out of range is a compile error.

A search ranks by its score or distance through the alias it projects:
`.orderByAlias('score', 'DESC')` renders `ORDER BY score DESC`, which
SurrealDB 3.3 and 4.0 both sort by the projected value (3.3 parses no
function call after `ORDER BY`). The name is checked when it is given and
again when the statement is built, against what the statement then returns:
`.orderByAlias('score')` before a `project` that drops `score` is refused,
because SurrealDB would sort by a stored field of that name or by NONE, and
`SELECT VALUE` holds no names, so `.value(…).orderByAlias(…)` is a compile
error. A hybrid search joins a match and a
nearest-neighbour search with `and`, projects both, and orders by either;
`test/search-engine.test.ts` runs both orders and the hybrid on both
engines.

A fused row has the `id` of every list, every other field of any list as an
optional key (SurrealDB merges rows by `id`, so a field is absent when the
row was not in that list), and `rrf_score` or `linear_score`. A list whose
rows have no `id` is a compile error: SurrealDB leaves such rows out.

A brute-force search takes only the distances SurrealDB 3.3 and 4.0
compute alike; `test/search-engine.test.ts` checks each against its formula
on both (`COSINE_NORMALIZED` is one minus the dot product, so it assumes
unit vectors, and `INNER_PRODUCT` the negated dot product). `JACCARD` and
`PEARSON` are a compile error and refused when built: 3.3 answers the
Pearson correlation itself as the distance, so its nearest rows are the
least correlated, and a set measure for `JACCARD` that is 0 for most
vectors of floats, where 4.0 answers one minus each similarity. The same
search finds other rows on each, and a statement does not know which
engine runs it; the conformance suite pins both.

What is refused when the statement is built, because SurrealDB would answer
without an error: a `score`, `highlight` or `offsets` of a reference no match
of the same `SELECT` defines (SurrealDB 3.3 and 4.0 return another match's
score), one reference on two fields, and `knnDistance` with no search (an
error on both). A second nearest-neighbour search in one `SELECT` (4.0
refuses it, 3.3 runs it) and one under `OR` or `NOT` (both refuse it) are
refused when the expression is built. A subquery's references are its own.

`test/conformance.test.ts` pins what this relies on, on both engines: the
match reference, `k` and `ef` parse only as literals; a match without a
`FULLTEXT` index fails; `<|k, ef|>` without a vector index fails on 4.0.1 and
returns no rows on 3.3; SurrealDB 3.3 scores a term in at least half the
rows 0, 4.0 above 0; two matches sharing a reference highlight both on 4.0
and the last on 3.3; a row admitted through `OR` scores 0, is unmarked and
has no offsets; and the plan of each form. `test/search-engine.test.ts` runs
the builders on the corpus `doc` table and proves with `expectIndex` that the
match reads `ft_body` and `<|k, ef|>` one of its vector indexes, and with
`expectNoIndex` that the brute-force form reads neither.

A highlight is text with the marks in it, not a value of the field's own
type: it need not be one of the field's literals, nor within its length, so
it decodes as a string whatever the field's schema says. For a field that
may be NONE or NULL it is a string or NONE (SurrealDB answers NONE for
both). For a list field it is the list of the marked elements: SurrealDB
answers a list of one element as that element alone and a list of none, or
a field that is NONE, as NONE, and each decodes as the list it stands for.
Offsets are keyed by the index of each element that holds a match (`"0"`
for text) and count characters, not bytes. `test/highlight-engine.test.ts`
runs every one of these on both engines.

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
themselves. `make` and `sequence(…).define` also take `uniqueIndexes`, the
unique indexes the text can duplicate a value of, by name (the analyzer
cannot tell them from the text), so a duplicate on one is a typed
`StatementIndexConflict` (see [A value that may already be
taken](#a-value-that-may-already-be-taken)); a statement declared not to
write is refused one. Only `part` and `sequence` import the
analyzer, so code that uses only the builders does not bundle it.

### Statements from `.surql` files

`effect-surrealkit statements` writes the parts for you. `--input` holds one
directory per scope, each with `.surql` files; every scope becomes
`<out>/<scope>/statements.ts`, and `<out>/index.ts` re-exports each scope as a
namespace:

```sh
effect-surrealkit statements --input queries --out src/generated/statements
```

`queries/accounts/bindOwner.surql`:

```surql
-- Binds the owner a caller names, and the caller.
LET $owner = $boundOwner;
LET $principal = $auth.id;
```

`queries/accounts/requireOwner.surql`:

```surql
IF $owner != $principal { THROW 'Not the owner' };
```

```ts
// src/generated/statements/accounts/statements.ts (excerpt)
/**
 * Binds the owner a caller names, and the caller.
 *
 * ```surql
 * LET $owner = $boundOwner;
 * LET $principal = $auth.id;
 * ```
 *
 * `accounts/bindOwner.surql`: a part.
 */
export const bindOwner = $statement.part({
	name: "accounts.bindOwner",
	source: "accounts/bindOwner.surql",
	sql: `-- Binds the owner a caller names, and the caller.
LET $owner = $boundOwner;
LET $principal = $auth.id;`,
	free: ["boundOwner"],
	defines: ["owner", "principal"],
	slots: 2,
	throws: [],
	throwsAt: {},
	tables: { read: [], written: [] },
	functions: [],
} as const);
```

The doc comment is what an editor's hover shows where the part is used:
the comments that open the file as prose (`--`, `//` and `#` without their
marker, a `/* … */` without its delimiters and `*` margin; a comment after the
first token is not read), then the SurrealQL after them in a fenced block, then
the file name. `analyze(sql).comment` returns the same prose. Templates and
fragments get the same comment. In it a `*/` is written `*\/` and an `@` after
whitespace (SurrealQL's `@@` and `@1@`) `\@`, so neither ends the comment or
starts a tag, and the fence is longer than any run of backticks in the text;
`sql` is never changed.

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
hole (default `__` and `__`); `--schema <file>` takes a SurrealKit schema
document v2 (the JSON the emitter reads) and fails generation, with the file
and the line of the first use, on any table or `fn::` function a file names
that the document does not declare (edge tables included) and that the same
file does not create with `DEFINE TABLE` or `DEFINE FUNCTION`. Any error stops
generation with its file, line and column; warnings are printed. `--verify`
also sends each complete text to an in-memory engine before writing (see
[Checking texts on an engine](#checking-texts-on-an-engine---verify)).

#### What bindings are used as

`analyze(sql)` also reports `uses`: for each free binding (keyed without
`$`), what the text uses it as, sorted and without duplicates. Three
patterns are recognised, each on tokens outside strings and comments:

```ts
type BindingUse =
  | { kind: 'argument'; function: string; index: number; line: number }
  | { kind: 'key'; table: string; line: number }
  | {
      kind: 'field';
      table: string;
      field: string;
      role?: 'compare' | 'assign';
      line: number;
    };
```

- `argument`: an argument of a custom function that is exactly one
  parameter. `RETURN fn::scaled($raw);` uses `$raw` as argument 0 of
  `fn::scaled`.
- `key`: `type::record('table', $key)` (or `type::thing`) with a plain
  string literal and exactly one parameter. `SELECT * FROM
  type::record('site', $siteKey);` uses `$siteKey` as the key of `site`.
- `field`: inside one query (a top-level statement or a parenthesised
  subquery) whose targets are exactly one static table and not open, a
  comparison (`=`, `==`, `!=`, `<`, `<=`, `>`, `>=`) in its `WHERE` between a
  bare field name and a parameter, or an assignment `field = $name` in the
  `SET` of an `UPDATE`, `UPSERT` or `CREATE`. `SELECT * FROM gauge WHERE
  station = $station;` uses `$station` as `gauge.station`. The analyzer sets
  `role` to `compare` for the `WHERE` comparison and `assign` for the `SET`
  assignment, which messages say; a declared use may leave it out.

`part` exposes the analysed `uses` (and checks declared ones against the
text); `sequence(...).define` merges them into the statement's `uses` for
the names that remain bindings.

With `--schema` (or a scope's `declared` names from
`declaredNames(document)`, whose `detail` holds each table's fields with
their kind text and whether they reject NONE, whether it accepts
undeclared fields (a schemaless table or a view, whose fields come from its
`SELECT`), its id key's kind,
and each function's argument kinds; a field or argument the document
records as optional has the kind text `option<K>`), every use must resolve when the
modules are generated: a field the table neither declares nor accepts
(`compares \`utcDy\` of \`snapshot\`, which declares no such field`) or
an argument beyond the function's arguments is an error with its file and
line. `id` resolves to the table's own record kind. Tables and functions
the schema does not declare are reported once, by the names check.

A binding whose every use resolves to the same simple kind (`string`,
`bool`, `int`, `float`, `number`, `decimal`, `datetime`, `duration`,
`uuid`, or `record<table>` of one table; `option<K>` counts as `K`) is
written into the part as `binds: { station: "option<string>" }`. At compile time
`sequence(...).define` then refuses a codec whose stored type cannot hold
that kind, by the same rule that decides whether two operands compare: a
branded or refined codec over the right representation is accepted, and
anything else is an error such as ``binding `station` is compared with
`gauge.station` (option<string>), which its codec cannot hold``. Bindings without a
`binds` entry are checked as before.

The entry is `option<K>` when every use of the binding accepts NONE:
comparing a field with NONE is always valid, and assigning to a field or
passing an argument accepts it when that field or argument is `option<…>`.
A key never does. For `option<K>`, a codec fits when its stored type, with
`undefined` set aside, compares with `K`, so `Schema.UndefinedOr` of a
literal, branded or refined string fits `option<string>`. For a plain `K`
the check is unchanged.

A binding assigned (`SET field = $name` in a `CREATE`, `UPDATE` or
`UPSERT`) to a field that rejects NONE gets a `rejectsNone` entry naming
that field: `rejectsNone: { station: { table: "gauge", field: "station",
kind: "string" } }`. A field rejects NONE when it is a top-level field
whose kind excludes NONE (not `option<…>` or `K | none`, not `any` or
without `TYPE`; `regex`, `function`, `file` and `table` kinds are not
counted) and it has no `DEFAULT`, `VALUE` or `COMPUTED` clause. On those
the engine rejects NONE in every statement the analyzer records an
assignment in, creating a record or updating one, on SurrealDB 3.3 and 4.0
alike, also when the table is schemaless or the field is `READONLY` or has
an `ASSERT`. A field with a plain `DEFAULT` is not counted, because the
default replaces NONE when a record is created, and nor are a view, a
`DROP` table, a relation's `in` and `out`, `id`, and a text that defines,
alters or removes a table or field itself.

This is a compile-time refusal: `sequence(...).define` refuses a codec
that can hold NONE (`undefined` is part of its stored type, as with
`Schema.UndefinedOr(…)`) for a binding with a `rejectsNone` entry, with an
error such as ``binding `station` is assigned to `gauge.station` (string),
which rejects NONE, and its codec can hold NONE``. Code that compiled
before can fail here once its modules are generated again with
`--schema`. Fix it by giving the binding a codec without NONE (a narrower
one, such as a literal, still fits), or, when NONE is meant, by making the
field accept it: declare it `option<…>`, or give it a `DEFAULT`. Only an
assignment counts: a binding compared with the same field (`WHERE $station
= NONE OR station = $station`) is not refused, and `Schema.Unknown`, which
holds anything, is not taken to hold NONE.

The limits are plain: only these three patterns are read, by a lexer, not
a parser, and nothing is recorded where it is not certain (a parameter in a
longer expression, a path, two tables or an open target, a comparison
inside a block). A binding with no recorded use is never checked against
the schema.

#### Projected fields

On a `SCHEMAFULL` table SurrealDB 3.3 and 4.0 both answer `SELECT nmae
FROM person` with `{}` and no error, so a misspelt field quietly comes back
missing. `analyze(sql)` also reports `projected`: what each `SELECT`
projects, as `{ table, field, line }` (`field` is `*` for `*`), sorted by
line, table and field, without duplicates. A `SELECT` is read when it is a
top-level statement (possibly after `LET $name =` or `RETURN`) or a
parenthesised subquery, which is a query of its own (in a projection too,
against its own table), and when all of this holds on tokens outside
strings and comments:

- it has exactly one `FROM` outside brackets;
- it has no `OMIT`;
- after `FROM` (and an optional `ONLY`) comes exactly one target: a table
  name, a record id `table:key` whose key is one token or one bracket
  group, or `type::record('table', …)`, `type::thing('table', …)` or
  `type::table('table')` with a plain string literal; then the end of the
  query or one of its clauses (`WHERE`, `SPLIT`, `GROUP`, `ORDER`, `LIMIT`,
  `START`, `FETCH`, `TIMEOUT`, `EXPLAIN`, `WITH`, `VERSION`, `TEMPFILES`);
- it has no `PARALLEL` after `FROM`, a parse error on both versions;
- a `FOR UPDATE` is read only in the form both versions accept: every
  target a record id (`table:key`, `type::record` or `type::thing`), no
  `GROUP`, `SPLIT` or `VERSION`, and nothing after it but `TEMPFILES` and
  `EXPLAIN [FULL]`. The lock changes neither the target nor the fields
  returned. On a table, a range or an array the engine refuses it
  ("requires record ids as targets"), and the analyzer records nothing.

A pinned engine test runs each of these clauses on both versions and
checks that it returns the same fields as the query without it (`EXPLAIN`
returns a plan, which names the same projection); `VERSION`, which needs a
versioned store, was checked the same way on one. The same `FOR UPDATE`
rule decides the tables a text reads and the binding uses of its `WHERE`.

Of such a `SELECT`, each projected item of exactly one of these forms is
recorded, against that table:

- `*`;
- one bare field name: a word or a quoted name, except the words SurrealQL
  reads as values, `NONE`, `NULL`, `TRUE` and `FALSE` in any case and `NaN`
  and `Infinity` spelt so (a pinned test tries every keyword of both
  versions' parsers on both engines);
- one bare field name, `AS`, and one name: the field is recorded, never the
  alias;
- `SELECT VALUE` followed by one bare field name and then `FROM`.

Every other item records nothing: a function call, a path (`address.city`,
`tags[0]`), an expression, an alias of anything but one bare field, a graph
step and a subquery, and `VALUE` before anything but one bare field. Two
targets, an open target (`$owner`, `type::table($t)`, a subquery, a range
or a graph step after the table), a hole after the target and a statement
in a block that is not parenthesised record nothing.

With `--schema` (or a scope's `declared` detail), a recorded field that a
`SCHEMAFULL` table does not declare fails generation with its file and line:
``shop/q.surql:2: selects `staton` of `gauge`, which declares no such
field``. A quoted name is written as SurrealQL quotes it (`` `lab\`el` ``).
`*`, `id`, a declared field and a field with declared nested fields (`pos`
when `pos.x` is declared) resolve. Nothing is checked on a schemaless table
or a view (both accept any field), on a table the schema does not declare
(the names check reports it once), in a text that defines, alters or
removes a table or field itself, or in a template where a hole starts a
statement (at the start of the text, after `;` or after `{`), because a
fill there can define a field. The generated modules do not change:
`projected` is not written into a part.

One limit, the same the binding uses above have: a row written while its
table was schemaless keeps the fields it had when the table became
`SCHEMAFULL`, and on 3.3 and 4.0 alike `SELECT extra FROM loose` returns
them although no definition declares `extra`. The check refuses such a
text. Declare the field (`DEFINE FIELD extra ON loose TYPE option<…>`), or
clear it from the old rows (`UPDATE loose UNSET extra`; on 4.0 also
`ALTER TABLE loose MIGRATE EXISTING ROWS AS clear_loose REMOVE UNDECLARED
FIELDS`). Until then an `UPDATE` of such a row fails on both versions with
``Found field 'extra', but no such field exists``. As for the
binding uses, there is no way to exempt one text: a scope generated without
schema detail is not checked at all.

#### Rendering in process

The command is a thin wrapper over `renderStatementModules` from
`effect-surrealkit/analyze`: it reads the tree, refuses anything that is not
a `.surql` file, and writes, checks or watches; analysis, naming checks and
rendering are all the function's. Call it directly to generate the modules
without the CLI or a filesystem:

```ts
import { Effect } from 'effect';
import { renderStatementModules } from 'effect-surrealkit/analyze';

const program = renderStatementModules({
	scopes: [
		{ name: 'accounts', files: [{ name: 'bindOwner', sql: 'LET $owner = $boundOwner;' }] },
	],
	importExtension: '.ts', // default .js
	// holes: { open: '{{', close: '}}' },
});
// Succeeds with { files: [{ path: 'accounts/statements.ts', content }, { path: 'index.ts', content }],
// warnings: [{ scope, file, line, column, code, message, text }] }, the same
// bytes the command writes; fails with a StatementModulesError whose
// `problems` hold every rejected file, invalid, reserved or case-colliding
// name, invalid scope and, for a scope with `declared`, undeclared name.
const { files, warnings } = await Effect.runPromise(program);
```

`renderStatementModulesSync` is the same, throwing the error. A scope may
carry `declared: { tables, functions }`: every table or custom function a
file of that scope names that is not in it, nor created by the same text
with `DEFINE TABLE` or `DEFINE FUNCTION`, is a problem of kind `undeclared`
(`shop/q.surql:2: reads table \`shipmnt\`, which the schema does not
declare`). `declaredNames(document)` from `effect-surrealkit/emitter` returns
those names from a decoded schema document (`decodeDocument`), edge tables
included; `--schema` applies it to every scope. Names behind an open target
are not checked: a text with `openTargets` can use tables the check never
sees.

#### Checking texts on an engine: `--verify`

The analyzer is a lexer, so a text it can count but SurrealDB cannot parse
(`SELECT * FORM person;`) generates a module and fails when it first runs.
`--verify` sends each complete text to SurrealDB's own parser before
anything is written:

```sh
effect-surrealkit statements --input queries --out src/generated/statements \
  --verify --verify-schema schema/tables.surql --verify-schema schema/functions.surql
```

It needs the optional peer `@surrealdb/node` (`npm install --save-dev
@surrealdb/node @surrealdb/node-native`); without it the command fails with
that install command. It starts an in-memory engine in the process, runs the
`--verify-schema` files in the order given, then sends each complete text
(a `part` with no holes) once, with no bindings, in scope and file order. A
fragment or a template cannot be sent as it stands and is skipped. Three
outcomes are problems, and any one stops generation with exit 1 and writes
nothing:

- a parse error, with the file, line and column the engine reports
  (`broken/typo.surql:1:10: Parse error: Unexpected token \`an identifier\`,
  expected FROM`);
- a number of results other than the analyzer's `slots`;
- a text that does not finish within the timeout (10 seconds, or
  `timeoutSeconds`): `slow/wait.surql: the text did not finish within 10s on
  an empty database with no bindings, so its result count was not verified`.
  This is the one outcome whichever way the engine reports the timeout (an
  error for each statement it did not finish, or a refusal of the whole
  text). Such a text is never reported as verified and never as a parse
  error.

Any other error a text meets without bindings (a `NONE` record id, an unknown
record, a `THROW`) is not a problem: the engine still returns one result per
top-level statement, and that is what the check counts. The schema is
optional: the parse and the count do not depend on it, so a schema changes
which errors the texts meet, never what the check reports. Loading it also
proves the schema files apply, and a schema file that fails to parse or apply
is a problem with its name. On success the command prints the texts verified
and the engine version, which is the installed `@surrealdb/node`. A text that
only another SurrealDB version refuses is checked against this one. With
`--check` the command verifies, then compares, and writes nothing in either
case; with `--watch` every regeneration is verified.

A verified text is executed. It runs on a database that is discarded when
the check ends, so its writes, `DEFINE`s and `REMOVE`s go nowhere, but texts
run one after another on the same database, so one text's writes are visible
to the next. The engine is started with network targets, `http::` functions
and embedded scripts denied: an `http::post` fails with `Function 'http::post'
is not allowed to be executed` and sends nothing, still counting as one result.
File buckets are refused by the engine's default (no folder is allowed). Each
text and each schema file may run for 10 seconds; a `sleep` or a long loop
ends there and is a timeout problem, as above. Nothing else a text can write
in SurrealQL reaches outside the engine.

`verifyStatements` from `effect-surrealkit/testing` is the same check, for a
build that renders in process; it takes the scopes as `renderStatementModules`
does. `effect-surrealkit/analyze` does not import it or the engine, and stays
host-neutral.

```ts
import { verifyStatements } from 'effect-surrealkit/testing';

const verified = verifyStatements({
	scopes, // as given to renderStatementModules
	schema: [{ name: 'schema/tables.surql', sql: tablesText }], // optional, in order
	// holes, timeoutSeconds (default 10)
});
// Succeeds with { engine: '4.0.1', sent: ['accounts/bindOwner.surql', …], notSent: [fragments and templates] };
// fails with a StatementVerificationError whose `problems` hold { kind, file, line?, column?, text, message },
// kind being 'parse', 'refused', 'count', 'timeout', 'schema' or 'analyzer',
// or with a TestingError when @surrealdb/node is missing or the engine does not start.
```

#### Importing `.surql` files: the content mapper

The `statements` command and the content mapper are two supported ways to get
the same modules. Instead of writing `<out>/<scope>/statements.ts`, a project
can import each `.surql` file. TypeScript 7.1 asks the package's content
mapper for the file's module, and the run-time loaders give Node, Bun and Vite
the same module:

```ts
import { bindOwner } from './queries/accounts/bindOwner.surql';
import { readProfile } from './queries/accounts/readProfile.surql';

export const profile = sequence(bindOwner, readProfile).define({
	params: { boundOwner: SurrealSchema.RecordIdAny }, // a missing name is a compile error
	result: SurrealSchema.String,
});
```

A `.surql` import yields exactly the `<scope>/statements.ts` that
`renderStatementModules` renders for a scope holding only that file: the
command's module for that scope, run on the same input, holds the same
constant byte for byte. The mapper's `input` option is the command's
`--input`, and a file must be `<input>/<scope>/<name>.surql`, so
`queries/accounts/bindOwner.surql` exports one constant, `bindOwner`, with
`name: "accounts.bindOwner"`, its doc comment and the one import it needs.
There is no default export and no index module. Only the layouts the command
accepts are accepted, and named as it names them. A file outside the input,
nested deeper, under a dot directory, or in an input the command refuses
(a file beside the scopes, a non-`.surql` file in a scope, an empty scope, a
name that is not an identifier, two names that differ only in case, a scope
name in two inputs when `input` lists several) is a problem of the file. A
file with a problem, or that the analyzer or the schema check refuses,
exports nothing.

Requirements:

| Where | What | Needs |
| --- | --- | --- |
| Type check | `tsc --noEmit --runExternalCode` with a `contentMappers` entry | TypeScript 7.1: `typescript@7.1.0-beta` is the version this package is tested on. `--runExternalCode` is accepted only on the command line. Editors run the mapper only in a trusted workspace with content mappers enabled. |
| Node | `node --import effect-surrealkit/register app.ts` | Node 22.21.1 or later, the package's floor: `module.registerHooks` and `module.stripTypeScriptTypes` exist, a `.ts` entry point needs no `--experimental-strip-types`, and `tsc --runExternalCode` starts the mapper. Node 23.0 to 23.4 is within `engines` but lacks both functions, and the hook refuses it with that message; 23.5 and later work. Measured on 22.15.0 to 22.18.0: before 22.18.0 a `.ts` entry point needs `--experimental-strip-types`, and before 22.21.1 `tsc` cannot start the mapper, because `process.execve` there passes no environment ([nodejs/node#60029](https://github.com/nodejs/node/pull/60029)). Node prints `ExperimentalWarning: stripTypeScriptTypes is an experimental feature and might change at any time` once per process (measured on Node 26.7); `--disable-warning=ExperimentalWarning` hides it |
| Bun | `bun --preload effect-surrealkit/register app.ts`, or `surqlBunPlugin()` from `effect-surrealkit/mapper` in `Bun.build` | Bun |
| Vite | `surrealkitPlugin({ surql: true })` from `vite-plugin-surrealkit` | Vite 8 |

Not supported: TypeScript before 7.1, and Bun's own type checker, `bun
check`, which ignores `contentMappers` (every `.surql` import is TS2307) and
refuses `--runExternalCode`, measured on Bun 1.4.4 and 1.4.5. A project
checked by `bun check` keeps the `statements` command.

```jsonc
// tsconfig.json
{
	"compilerOptions": { /* … */ },
	"contentMappers": [
		{
			"package": "effect-surrealkit",
			"extensions": [".surql"],
			// Optional; paths are relative to this tsconfig.json.
			"options": {
				"input": "queries", // as --input; or a list of directories
				"schema": "schema/document.json", // every directory, as --schema
				"schemas": { "queries/earth": "earth/schema.json" }, // one directory
				"holes": { "open": "__", "close": "__" } // as --hole-open and --hole-close
			}
		}
	],
	"include": ["src", "queries"]
}
```

A directory named in `schemas` takes that schema document v2, any other takes
`schema`, and with neither a file is not checked against a schema, as the
command without `--schema`. With a schema the mapper checks what `--schema`
checks, and the parts carry the same `binds` and `rejectsNone`. A schema
document that changes renders its files again (the mapper reports the
documents to TypeScript as watched files). A new `.surql` file added under an
input is not picked up by an incremental `tsc` run until the next full run.

The type check and the loaders read one literal configuration, or refuse.
The loaders read the entry from the nearest `tsconfig.json` above each file
(the Vite plugin and `surqlBunPlugin` take a `tsconfig` path to read
another), from that file itself, never from one it `extends`, and refuse a
`tsconfig.json` that has `extends` and no entry of its own. The mapper
compares the options TypeScript passes with the entry written in the checked
`tsconfig.json`. An entry inherited through `extends`, options that differ
from the written ones, or a project with no `tsconfig.json` refuse every
file, each with that reason, rather than check with options the run time
would not use. Write the entry in the `tsconfig.json` that is checked.

The mapper runs the analyzer and the renderer only: no engine, no cache
directory and no worker, so it needs neither `@surrealdb/node` nor a
database. A problem the command reports with a file, line and column is a
type error at that line and column of the `.surql` file
(`queries/broken/unterminated.surql(2,8): error effect-surrealkit1:
unterminated string (unterminated)`), reported beside every other type error
of the program, which it does not hide. The mapper reports each problem as
an expect directive, a form TypeScript 7.1.0-beta honours but does not
document. If a later TypeScript stops honouring it, the reason disappears and
what remains is an importing file's TS2305 (`Module … has no exported
member`), because a refused file exports nothing; the fixtures'
`rejects/expected.txt` is what catches that in this package's CI. The protocol has no warning, so the
analyzer's warnings, which the command prints without failing, are not
reported. `--verify` has no mapper counterpart: keep the engine check where
the build runs, with the command or `verifyStatements`. The run-time loaders
throw a `SurqlModuleError` naming each problem, as the command refuses to
write the file.

Each host strips the module's types with its own TypeScript transform: Node
with `module.stripTypeScriptTypes`, Bun with its `ts` loader, and the Vite
plugin with Vite's `transformWithOxc`, in dev and in builds alike.

`effect-surrealkit/mapper` exports what the hosts use: `renderSurqlModule`
(the module of one file, pure), `loadSurqlModule` (read a file and its
project's options), `surqlLayout` (the command's layout rules),
`surqlBunPlugin`, and `openSurqlProject`, `surqlProjectFor`, `mapperEntryIn`
and `configurationDrift` (the options, the schema documents and the one
configuration).

#### Pinning built statements

Pinning keeps the SurrealQL text of statements built in TypeScript in
checked-in `.surql` files, so a reviewer reads the built statement as SQL,
a package upgrade that changes the emitted text shows up in a diff, and a
recorded hash covers a text that is in the repository. Both functions are
pure and synchronous; the caller reads and writes the files.
`renderPinnedStatements(statements)` returns `[{ path, content }]`, sorted:
`billing.openInvoices` becomes `billing/openInvoices.surql` holding its
`sql` and one newline. It throws a `PinnedStatementsError` listing every
unnamed, duplicate or colliding name and every segment that is empty or not
letters, digits, `_` and `-`. `pinnedStatementProblems(statements, files)`
compares the files on disk and returns `[{ path, name?, kind }]` with kind
`missing`, `changed` or `stale`, ignoring files that are not `.surql`;
empty means current.

```ts
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { pinnedStatementProblems, renderPinnedStatements } from 'effect-surrealkit/analyze';

// build: write every pin
for (const { path, content } of renderPinnedStatements(statements)) {
	mkdirSync(dirname(join('pins', path)), { recursive: true });
	writeFileSync(join('pins', path), content);
}
// check: fail on any missing, changed or stale pin
const files = readdirSync('pins', { recursive: true, encoding: 'utf8' })
	.filter((path) => path.endsWith('.surql'))
	.map((path) => ({ path, content: readFileSync(join('pins', path), 'utf8') }));
const problems = pinnedStatementProblems(statements, files);
if (problems.length > 0) throw new Error(problems.map((p) => `${p.kind} ${p.path}`).join('\n'));
```

The analyzer is `effect-surrealkit/analyze`, pure and host-neutral:
`analyze(sql, { holes? })` returns `{ kind, sql, free, defines, slots,
throws, throwsAt, holes, holeScopes, provisional, transactional, writes,
tables, functions, openTargets, diagnostics }` or fails
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
- **Tables and functions.** `tables.read` and `tables.written` are the
  static tables the text names, each sorted and without duplicates (a
  table both read and written is in both); `functions` are the custom
  functions it calls, as written (`fn::a::b`). Written: the target of
  `CREATE`, `UPDATE`, `UPSERT`, `DELETE [FROM]`, `INSERT [IGNORE]
  [RELATION] INTO` and the edge of `RELATE a->edge->b`, after an optional
  `ONLY`. Read: each target of a `SELECT`'s `FROM` list, graph steps
  written as names (`->edge->table`, `<-`, `<->`, also `->(edge WHERE …)`)
  and any other `type::record('t', …)`, `type::thing('t', …)` or
  `type::table('t')`. A static target is a name (also quoted), a record id
  `t:key` or `type::record`/`type::thing` with a string literal; a
  parenthesised subquery in `FROM` is read where it stands. Any other target
  (`$ids`, a function call, `type::table($t)`), or one the lexer cannot read
  with certainty, sets `openTargets`, so the lists may be incomplete.
  `DEFINE`, `REMOVE`, `ALTER`, `INFO`, `USE`, `SHOW` and `REBUILD`
  contribute nothing. `part` takes `tables`, `functions` and `openTargets`,
  checks them against the text when given and always exposes what the text
  does; a `sequence`'s statement carries the union over its parts (builders
  leave them out).
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
  `holeScopes` gives, for each hole, the names visible at every occurrence
  (`bound`) and the free names a `LET` placed there would define for every
  read (`binds`, only where the hole starts a statement); the command
  writes it as `scopes`, which statement families build on.

What it does not check: it lexes, it does not parse, so it does not know
whether a text is valid SurrealQL, what a statement returns (only how many
results), whether a fragment really is one expression, or what a function
body reads when it is called; a `DEFINE PARAM` is reported as a warning
and later reads of it stay free.

### Statement families

Some reads differ only in which clauses they hold: a filter the caller may
leave out, a nearest-neighbour branch only some calls need. A statement
family is the finite, declared set of those statements, composed from
generated templates. The caller selects one variant per call with a key
whose axes the family declares. Every variant is an ordinary static
`Statement`, with its own text, its own bindings and its own pin. Input
values never change a text. They still travel only as bindings.

`compose(template, fills)` fills every hole of a generated template.
`choose(axis, options)` picks a fill by the value of one axis, and `empty`
leaves a clause out. A fill is a generated text without holes, a part (for
a hole that starts a statement), another `compose(…)` or `choose(…)`, a
constant that binds nothing (`literal(…)`) or `empty`. Raw strings are
refused. `family(axes, root).define({ name, params, result })` takes one
codec for each name any variant reads, and `select(key)` returns the
variant's statement:

```text
queries/catalog/visible.surql   shelf = $shelf.id __PRICE__ __REGION__
queries/catalog/priceCap.surql  AND price <= $maxPrice
queries/catalog/regionIs.surql  AND region = $region
queries/catalog/byWords.surql   SELECT id, title FROM listing WHERE __WHERE__ AND title @1@ $terms ORDER BY id LIMIT 10
queries/catalog/guarded.surql   RETURN IF $tenant = NONE { … } ELSE { LET $shelf = (…); … data: (__BODY__) … };
```

```ts
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { run } from 'effect-surrealkit/client';
import { choose, compose, empty, family } from 'effect-surrealkit/statement';
import { catalog } from './generated/statements/index.js';

const visible = compose(catalog.visible, {
	__PRICE__: choose('price', { true: catalog.priceCap, false: empty }),
	__REGION__: choose('region', { true: catalog.regionIs, false: empty }),
});

export const listings = family(
	{ price: [false, true], region: [false, true] },
	compose(catalog.guarded, {
		__BODY__: compose(catalog.byWords, { __WHERE__: visible }),
	}),
).define({
	name: 'listings',
	params: {
		tenant: SurrealSchema.String,
		terms: SurrealSchema.String,
		maxPrice: SurrealSchema.Int,
		region: SurrealSchema.String,
	},
	result: Schema.Unknown,
});

// A key known at compile time: this variant binds exactly tenant, terms and maxPrice.
run(listings.select({ price: true, region: false }), { tenant: 't', terms: 'lamp', maxPrice: 40 });

// A key known only at run time: the input must carry every required name
// some selectable variant reads; the variant selected sends only its own.
declare const input: { tenant: string; terms: string; maxPrice?: number };
run(
	listings.select({ price: input.maxPrice !== undefined, region: false }),
	{ ...input, maxPrice: input.maxPrice ?? 0 },
);
```

The compiler works out what each variant reads from the literal names the
`statements` command writes. Each text carries its `free` names and, for a
template, the `scopes` of its holes: `bound`, the names visible at every
occurrence of a hole, such as `$shelf` above, and `binds`, the names a
`LET` placed there would define. `$shelf` is a local of the guard, so it
is no binding of any variant. A key whose axes are unions (`price:
boolean`) selects any variant in that sub-product. Its statement is typed
with every name those variants read, and a name is required when its codec
does not accept `undefined`. Typing the selection as a union of variant
statements would be unsound: TypeScript would keep only the names every
variant reads, and a call could omit a value the selected variant needs.
No type builds SQL or enumerates the variants, so the cost of a family
does not grow with the number of variants. `test/budget.test.ts` holds it:
30 families shaped like an eight-variant search cost about 8,000
instantiations each, about what a block costs.

Mistakes are compile errors whose first line names them
(`test/diagnostics.test.ts` pins every case `family/*`):

| Mistake | First line (abridged) |
| --- | --- |
| a hole without a fill | `… '{ __PRICE__: Empty; }' is not assignable to parameter of type '"hole `__REGION__` has no fill"'` |
| a fill for no hole | `Type 'Empty' is not assignable to type '"`__COLOUR__` is not a hole of this template"'` |
| raw SurrealQL as a fill or option | `… '"a hole takes a generated text or part, `compose(…)`, `choose(…)`, `literal(…)` or `empty`, not raw SurrealQL"'` |
| a template as a fill | `… '"this fill is a template with holes (__NEAR__ …): compose it first"'` |
| a constant that binds | `… '"a constant fill binds nothing: put the binding in a `.surql` fragment"'` |
| a fill typed as one of several texts | `… '"this fill is typed as one of several texts, so the compiler cannot tell which a variant holds: choose between them with `choose(…)`"'` |
| a choice on an undeclared axis | `… '"choose('vector', …) names an axis this family does not declare"'` |
| an axis nothing chooses on | `… '"axis `size` is never chosen on, so every value of it gives the same text"'` |
| an axis value without a text | `… '"choose('price', …) has no text for `none`" \| …'` |
| a codec missing, or one too many | `… '"binding `$maxPrice` is read by a variant of this family and has no codec"'`, `… '"binding `$colour` is not read by any variant of this family"'` |
| `select` with a property that is no axis | `… '"`colour` is not an axis of this family"'` |
| `select` without an axis, or a bad value | TypeScript's own (`Property 'vector' is missing …`, `Type 'string' is not assignable to type 'boolean'`) |
| `run` without a value the variant reads | `… '"binding `maxPrice` has no value"'`, also for a run-time key that may select the variant |
| `run` with a value the variant does not read, misspelt, or of the wrong type | `TS2353 … 'region' does not exist …`, `TS2561 … Did you mean to write 'maxPrice'?`, `Type 'string' is not assignable to type 'number'` |

The same rules are checked at run time, so a hand-written or out-of-date
text cannot mislead the compiler:

| When | Check |
| --- | --- |
| `compose`, `choose` | every hole filled and none extra; each fill a node; no raw string, template or constant that binds; a choice has options |
| `define` (module load) | every text's declared `holes`, `free`, `defines`, `throws` and `scopes` equal what the analyzer derives from its SQL (`… does not declare what its text does: scopes.__BODY__.bound: declared [], the text has [shelf]`); the axes are well formed, every chosen axis is declared and every declared axis is chosen on; the codecs are exactly the names the variants read (`the bindings of family `short` do not match the names its variants read; missing: $maxPrice`) |
| first `select` of a key, `variants()`, or `define` with `build: 'eager'` | the variant's final text is analysed. It must have no holes, be a part rather than a fragment, read and throw exactly what the compiler's rule says, and have no error diagnostics. Its statement then binds exactly the analysed free names (`family `x`, variant price=true: the composition reads [$a], its text reads [$a, $b]`) |
| `select` | every axis has one of its declared values, and there is no other axis (`family `x`: yes is not a value of the axis `price` (false, true)`) |
| `run` | unchanged: only the variant's own `params` are encoded and sent |

A variant is built once, on first use. `variants()` builds every one and
returns `{ key, name, sql, statement }` for each. Its `name` is the
family's name plus one `axis-value` segment per axis, so the list feeds
`renderPinnedStatements` and `pinnedStatementProblems` as it is: run it in
CI and every variant is checked and reviewed as SurrealQL
(`pins/listings/price-true/region-false.surql`). Every variant's
statement is named after the family, so metrics count the family under
one label. Its `variant` (`price=true, region=false`) is recorded on the
span of a `run` as `effect_surrealkit.statement.variant`. Binding values
never are.

What a family does not do:

- No text depends on an input value; the choice is by declared keys only.
- No absent filter is neutralised (`$x = NONE OR …`). The clause is left
  out of the text.
- No per-variant codecs: one codec per name per family, so a name only
  some variants require takes the one codec.
- No dependent axes (an axis that exists only under some value of
  another): use two families, or accept variants nobody selects.
- No custom hole delimiters yet: `Text` does not record its delimiters,
  and composition assumes `__NAME__`.
- No definitions propagate through a composite at a statement hole. Only a
  text's or part's own `defines` count, and `define` refuses a family that
  relies on more instead of sending a wrong statement.

### Running statements

Use `run` for one typed statement, `transaction` for an atomic batch,
and `pipeline` for a separate result per statement in one request.
`withTransaction` makes several requests on an open transaction.
`client.bulk` writes a list binding in chunks, `stream` emits decoded
rows, and `client.liveScoped` and `client.live` subscribe to typed
changes. `timed` returns request measurements alongside the result.

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

A statement decodes one result, at its `valueSlot`, and every other result
of its text must be NONE. `valueSlot` is the last result unless the text
ends with transaction control: a script written `BEGIN; guards…; <value>;
COMMIT;` decodes the result before `COMMIT`, since SurrealDB answers NONE
for `BEGIN`, `COMMIT` and `CANCEL`. `sequence` derives the slot from its
parts, and `make` takes it as `valueSlot` (default: the last).

`layer(db)` takes an SDK connection or session, and statements run with that
session's authentication. `make(db)` returns the same client as a plain
value, for an application that names its own services, one per session.

For replay-sensitive WebSocket writes and for live queries that must report a
lost connection, connect with `reconnect: false` on the root SDK connection.
Forked sessions inherit that setting. With automatic reconnect enabled, SDK
2.2.2 can resend a buffered request whose acknowledgement was lost, even
with conflict retries disabled. An unmanaged live listener can also keep
waiting after its original socket is gone. The client counts its own sends;
the SDK can make additional physical sends inside one attempt.

```ts
await db.connect(endpoint, { reconnect: false });
const client = make(db);
```

| Error | When | Was anything written? |
| --- | --- | --- |
| `StatementEncodeError` | a binding failed its codec, or its encoded value is not one the SDK sends without loss (`path` says where inside it) | no, nothing was sent |
| `StatementScopeError` | the client could not safely rename a shared variable, the SDK would discard an atomic binding, or a statement contains transaction control inside a transaction | no, nothing was sent |
| `StatementThrown<Message>` | a `THROW` the statement declares; in a transaction, `statement` names the one that declares it (`one of …` when several do); `source` and `lines` say where, when the part records them | not by a transaction; a single statement's earlier statements commit on their own |
| `StatementUnexpectedThrow` | a `THROW` whose message the statement does not declare (a dynamic message), with the thrown text as `reason` and the SDK's error as `cause` | as for `StatementThrown` |
| `StatementConflict` | a transaction conflict that `retry` did not resolve | the conflicting transaction rolled back. Earlier independent statements may have committed |
| `StatementIndexConflict<Index>` | a write would duplicate a value of a unique index the statement declares (`uniqueIndexes`; a write builder declares its table's), named by `index`, which is read from the engine's message ([surrealdb/surrealdb#7652](https://github.com/surrealdb/surrealdb/issues/7652) asks for a structured error); in a transaction, `statement` names the one that declares it (`one of …` when several do). Prefer `upsert`, `INSERT IGNORE` or `.onDuplicate(…)` where a taken value is expected | not by that statement, nor by a transaction; a single statement's earlier statements commit on their own |
| `StatementRecordExists` | a `CREATE` or `INSERT` of a record id that already exists | as for `StatementIndexConflict` |
| `StatementRequestError` | any other failure of the request: transport, authentication, a SurrealQL error (a duplicate on an index the statement does not declare included), or the SDK refusing to send (a `RETURN` followed by another statement in a transaction), with the SDK's error as `cause` | unknown for a transport failure; otherwise no |
| `StatementEmptyResult` | a `create` or `relate` that returns rows (`nonEmpty`) returned NONE or an empty list: SurrealDB reports a write the session may not make as an empty result, without an error, so the session may lack permission | not by that statement |
| `StatementDecodeError` | the answer of a statement or transaction known not to write (`writes: false`) is not what it declares: the number of results, a value where NONE belongs, or a last result its codec refuses | no |
| `StatementWriteOutcomeError` | the same, for a request not known to be read-only; `request` names the statement or transaction and `cause` is the `StatementDecodeError` (naming the statement and result), with `committed: true` and a message saying the request succeeded and either its writes are committed (`writes: true`) or it may have written (`writes` unknown) | yes, whatever it wrote is committed: read it back before resending |
| `StatementUnsupported` | the connection or engine refuses the requested feature | the feature did not start |
| `TransactionOutcomeUnknown` | the commit of an interactive transaction, an atomic bulk write, or a multi-statement bulk chunk lacked an acknowledgement | unknown. Read back before replaying |
| `BulkWriteError` | a non-atomic bulk chunk failed. Carries the failed chunk, confirmed preceding counts and the statement error | preceding chunks completed. The cause describes the failed chunk's outcome |
| `LiveQueryClosed` | the engine killed the live query or the connection was lost | closing the subscription does not write data |

Client error messages, default inspection, and JSON output omit raw server
diagnostics, undeclared `THROW` text, keys from preflight binding values,
and the duplicated value and record id of a constraint error (a
`StatementIndexConflict` shows the statement and the index name only).
Read `cause`, `reason`, or `path` directly when you need those details and
can handle them privately. Formatters that traverse those fields, including
Effect's `Cause.pretty`, can display their contents.

SurrealDB reports a record that exists as a structured error (`AlreadyExists`
of kind `Record`), which is what `StatementRecordExists` reads. It reports a
duplicate on a unique index as an `Internal` error that names the index only
in its message, and its protocol documentation says a message may change
between releases. The client therefore reads that message only in the form
the conformance suite pins on SurrealDB 3.3.4 and 4.0.1 (on another version
it may be a `StatementRequestError`; `checkConstraintErrors` tells), and only for an index
name the statement declares ([surrealdb/surrealdb#7652](https://github.com/surrealdb/surrealdb/issues/7652) asks SurrealDB for a structured error
instead); any other message stays a
`StatementRequestError`, as does a duplicate raised inside an event (the
engine prefixes the event's name). The message names the index and not its
table: a write that calls a function writing to another table with a
unique index of the same name reports that index under this name. A
`block` and a `family` declare no unique index, so a duplicate inside one is
a `StatementRequestError`.

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
document or a lone surrogate as U+FFFD. Older supported SDK versions can also
encode a bound negative zero as positive zero. The check never throws; `{
unsafeSkipValueCheck: true }` turns it off.

`{ retry: schedule }` takes an Effect `Schedule` and sends the same,
already encoded request again while the engine reports a
`StatementConflict`; no other error is retried. SurrealDB 4 already retries
a single conflicting statement on the server (a block run alone that loses
a conflict there succeeds, as `test/block-engine.test.ts` shows), so this
matters most for transactions and for SurrealDB 3.

```ts
yield* transaction([bind(bump)], { retry: Schedule.recurs(3) });
```

Standalone `run` also honors the SDK connection's retry policy. With a
custom policy that retries a later error in a multi-statement request, an
earlier write can commit again on each attempt. Use an atomic transaction
when those writes must succeed or roll back together.

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
parse). When two statements bind the same name, or one defines a top-level
`LET` with a name the other binds, each gets its own variable in the sent
text. The names depend only on the statements and their order. See “The
same statement more than once” below for an example.

A statement that writes its own `BEGIN`, `COMMIT` or `CANCEL` cannot be
nested in an atomic batch. The SDK also refuses a top-level `RETURN`
followed by another statement. End a reusable lookup with a bare expression
such as `$person;` to return that value and let the next statement run.

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

#### Several statements in one request: pipelines

`pipeline` sends statements paired with their inputs by `bind` in one
query request. The effect returns a tuple of Effect `Result`s in the
same order, each with its own result type and declared throws. An empty
pipeline returns `[]` without sending a request.

```ts
import { Effect, Schema } from 'effect';
import { bind, pipeline, run } from 'effect-surrealkit/client';
import { make } from 'effect-surrealkit/statement';

const names = make({
  name: 'person names',
  sql: 'SELECT VALUE name FROM person WHERE name != $excluded LIMIT 3;',
  params: { excluded: Schema.String },
  result: Schema.Array(Schema.String),
  writes: false,
});
const countTeams = make({
  name: 'team count',
  sql: 'count((SELECT VALUE id FROM team));',
  params: {},
  result: Schema.Number,
  writes: false,
});

const reports = pipeline([
  bind(names, { excluded: 'Bee' }),
  bind(countTeams),
], { each: 'transaction' });
```

The text sent with the `excluded` binding is:

```surql
BEGIN;
SELECT VALUE name FROM person WHERE name != $excluded LIMIT 3;
COMMIT;
BEGIN;
count((SELECT VALUE id FROM team));
COMMIT;
```

With `each: 'statement'` (the default), each statement's text runs as
written, including any of its own transaction controls. With
`each: 'transaction'`, each member gets its own `BEGIN` and `COMMIT`. A
failure rolls back that member's writes, while the other members can commit. This
mode refuses members that already contain transaction controls. The
members share one request scope, so an unrenamed `LET` is visible to later
members in either mode.

A binding that fails validation, a scope refusal, or a request the SDK
cannot send or answer fails the outer effect. A failed statement or
block has its own `Result.fail`, with the first error that caused the
failure. If every failed slot only reports fallout from a different
failure, the first failed slot is used. A write whose acknowledged
result fails decoding has a `StatementWriteOutcomeError` in its result.
A pipeline has no `retry` option because some writes may already have
committed by the time another member fails.

Use a pipeline for one round trip when you want the answers together.
Use concurrent Effects for independent requests whose answers should
be available as each request finishes:

```ts
const independent = Effect.all([
  run(names, { excluded: 'Bee' }),
  run(countTeams),
], { concurrency: 2, mode: 'result' });
```

`Effect.all` collects the results after both requests finish. To act on an
answer as soon as it arrives, handle it inside its individual effect.

#### The same statement more than once

Bind each occurrence separately in a `transaction` or `pipeline`.

```ts
const find = make({
  sql: 'SELECT * FROM ONLY $id;',
  params: { id: PersonId },
  result: Schema.UndefinedOr(PersonRow),
});
const [first, second] = yield* transaction([
  bind(find, { id: firstId }),
  bind(find, { id: secondId }),
]);
```

The SDK receives one request containing the following statements and two
separate encoded bindings.

```surql
SELECT * FROM ONLY $id__1;
SELECT * FROM ONLY $id__2;
```

Names shared by bindings or top-level `LET`s use their statement's 1-based
position. If the new name is already in use, another `_` is appended until
it is free. This depends on the statements alone, so different inputs to
the same list send identical text. Unshared names keep their spelling. A
scripted client still sees original input names.

For authored queries that will run together, use a final expression such
as `$person;`. A top-level `RETURN $person;` works alone or at the end of
an atomic batch. The SurrealDB SDK refuses a top-level `RETURN` followed
by another statement, and the client preserves that refusal.

A shared engine-set name or text that cannot be safely tokenized for a
rename raises `StatementScopeError` before sending. Plain queries can
bind `$__proto__`. Atomic requests refuse it when it would still be sent
under that name, because the SDK would drop it. A shared `$__proto__`
binding can be sent under its generated name.

A quoted record literal such as `r'thing:[$id]'` can evaluate a variable
inside its record key. If that variable needs renaming, composition raises
`StatementScopeError` before sending. Generated aliases also skip names
referenced inside quoted record literals.

#### Timing

`timed(effect)` returns the effect's value and one `RequestTiming` for each
request it sends, in completion order. Nested collectors also see the
requests made inside their scope. Effects that send nothing return an
empty timing list.

```ts
import { Duration, Schema } from 'effect';
import { bind, type RequestTiming, timed } from 'effect-surrealkit/client';
import { make } from 'effect-surrealkit/statement';

const answer = make({
  name: 'read, answer',
  sql: 'NONE; 42;',
  params: {},
  result: Schema.Number,
  slots: 2,
  writes: false,
});
const numbers = make({
  name: 'read, answer',
  sql: '[1, 2];',
  params: {},
  result: Schema.Array(Schema.Number),
  writes: false,
});
const [results, [timing]] = yield* timed(
  client.pipeline([bind(answer), bind(numbers)]),
);

// One timing observed on embedded SurrealDB 4.0.1.
const example: RequestTiming = {
  kind: 'pipeline',
  name: 'pipeline(read, answer, read, answer)',
  sends: 1,
  encode: Duration.nanos(289_333n),
  wait: Duration.nanos(5_243_334n),
  server: Duration.nanos(1_143_000n),
  decode: Duration.nanos(396_584n),
  total: Duration.nanos(6_301_875n),
  statements: [
    { statement: 'read, answer', server: Duration.nanos(785_583n) },
    { statement: 'read, answer', server: Duration.nanos(357_417n), rows: 2 },
  ],
};
```

- `kind` identifies the client operation. `name` is its span name.
- `sends` counts client transport attempts, including the client's own
  conflict retries. The SDK may retry internally within one attempt.
- `encode` measures binding encoding and preparation of the request.
- `wait` measures the last transport attempt, including any SDK retries it
  performs internally.
- `server` sums the engine durations supplied for the request's slots.
  Pipelines provide these durations, including the wrapping `BEGIN` and
  `COMMIT` slots when requested. The collected `run` and `transaction`
  methods leave them absent.
- `decode` measures the time spent decoding the answer.
- `total` measures the whole operation, including earlier attempts and
  retry delays.
- `statements` lists the original statement names in request order, with
  each statement's own `server` duration when available and `rows` count
  when its decoded result is a list.

The client records these phases with or without `timed`. Request spans
carry `effect_surrealkit.encode_us`, `effect_surrealkit.wait_us`,
`effect_surrealkit.decode_us`, `effect_surrealkit.sends`, and
`effect_surrealkit.server_us` when available. The timers
`effect_surrealkit_statement_encode_duration`,
`effect_surrealkit_statement_decode_duration`, and
`effect_surrealkit_statement_server_duration` use the `statement` tag.
Binding and result values stay out of span attributes and metric tags.

#### Interactive transactions

Use `client.withTransaction((tx) => effect)` to make several requests
and commit them together. The body can call `tx.run`, `tx.transaction`
or `tx.bulk`. The `withTransaction(effect)` accessor supplies the
transaction as `SurrealClient` so existing code that uses `run` or
`transaction` joins it.

```ts
import { run, withTransaction } from 'effect-surrealkit/client';

const move = withTransaction(
  Effect.gen(function* () {
    yield* run(debit, { from, amount });
    return yield* run(credit, { to, amount });
  }),
);
```

A successful body commits. A body that fails, dies or is interrupted
cancels the transaction. Embedded and WebSocket sessions support this
API. HTTP returns `StatementUnsupported` before the body starts.
There are no savepoints. Nested interactive transactions, pipelines,
live queries and streams are refused in the transaction’s client.

The tested engines, embedded SurrealDB 4.0.1 and WebSocket 3.3.4, can
continue and commit an open transaction after a statement fails. Propagate
the failure from the body when its earlier writes should be cancelled.
A top-level `LET` is local to the request that declares it. Keep the
statements that use it in that same request.

Pass `{ retry: schedule }` to rerun the whole body in a fresh transaction
after a conflict. A failed attempt does not commit its database writes.
The body’s other effects run again. If a commit answer is lost, or none
arrives within 30 seconds, the client returns `TransactionOutcomeUnknown`
without cancelling or retrying the commit. The SDK may still complete it.
Read the data back before deciding whether to run the body again.

#### Bulk writes

Pass an iterable or an Effect `Stream` for a statement's list binding.
Every chunk is encoded through that binding's schema. Other bindings are
included unchanged. The default chunk size is 500 and the result holds the
decoded answer for each chunk in input order.

```ts
import { Schema } from 'effect';
import { insert, param } from 'effect-surrealkit/statement';
import { PersonCreate, PersonTable } from './generated/surreal/index.js';

const insertPeople = insert(
  PersonTable,
  param('people', Schema.Array(PersonCreate)),
).returning('none').build('insert people');

const wrote = client.bulk(insertPeople, { people }, { over: 'people', chunk: 250 });
```

By default, embedded and WebSocket sessions begin one interactive
transaction, send each chunk, then commit. An HTTP session sends one
atomic batch containing **every chunk**, so the request size is unbounded
by `chunk`. A stream is collected before sending that batch. The client
peeks one chunk so empty input sends nothing. If begin is unsupported it
reuses that chunk and runs the hooks only on the fallback. Inside an
existing transaction, `tx.bulk` uses its handle.

With `atomic: false`, the client sends each chunk in one request and waits
for its acknowledgement before pulling the next chunk. A chunk containing
several SurrealQL statements gets its own atomic transaction. A conflict
rolls back the entire chunk before a scheduled retry. The SDK's rule for
an early `RETURN` in a stateless transaction also applies to these chunks.

`BulkWriteError` reports the zero-based failed chunk and the confirmed
counts for preceding chunks. A single-statement chunk with a lost response
can have written, as can a chunk whose acknowledged result fails decoding.
An unacknowledged atomic commit raises `TransactionOutcomeUnknown`, including
when that commit covered one multi-statement chunk. Read back before replaying.

`beforeChunk` sees the original rows and can stop the operation with its
own error. `onProgress` runs after acknowledgement, with a total for
arrays and sized collections. Progress inside an open transaction
describes provisional work. With an explicit retry schedule, a confirmed
conflict retries the affected non-atomic chunk or the whole atomic write.
Interactive retries retain the original chunks already pulled from
one-shot sources. Callback effects may run again. A connection's SDK
retry policy does not replay collector-backed bulk requests.

Use `updateEach(table, param('rows', Rows))` to merge a different
`{ id, set }` into each existing record. The codec must use ids for that
table and fields its merge schema accepts. A missing record raises
`StatementThrown<'Record not found'>` unless you pass
`{ missing: 'ignore' }`.

#### Rows as a stream

`stream(statement, input)` returns an Effect `Stream`. Consuming it sends
one request and delivers the decoded rows in order. The result schema must be a plain
`Schema.Array(row)`. A statement built with `only()` returns at most one
row, so use `run` for it. Use `run` for array-wide checks and transformations
as well. Effect Schema preserves the array's static type when a check is added.
The stream verifies the schema at runtime and refuses array-wide checks before dispatch.

```ts
import { Schema, Stream } from 'effect';
import { stream } from 'effect-surrealkit/client';
import { make } from 'effect-surrealkit/statement';

const readings = make({
  name: 'recent readings',
  sql: 'SELECT VALUE n FROM reading WHERE n >= $minimum ORDER BY n;',
  params: { minimum: Schema.Number },
  result: Schema.Array(Schema.Number),
  writes: false,
});

const sum = stream(readings, { minimum: 10 }).pipe(
  Stream.runFold(() => 0, (total, n) => total + n),
);
```

This sends the statement's SurrealQL with the encoded `minimum` binding.
Each consumer pull reads only far enough to decode and emit the next row.
Taking the first few rows or interrupting the effect closes the iterator
and aborts the request. The connection can serve subsequent requests.
`timed` reports one send, the time spent pulling from the iterator, the
number of emitted rows and the server durations received before closure.

Full consumption verifies that every slot besides the value slot returns
NONE. An empty list in the value slot emits zero rows, or fails with `StatementEmptyResult` if the
statement declares `nonEmpty`. A server or decode failure ends the stream
after any rows already emitted. If decoding fails before a writing stream
has been fully acknowledged, `StatementWriteOutcomeError` reports the
unknown write outcome. Read the data back before replaying an interrupted
or unacknowledged write.

The engine and SDK can buffer data before the consumer asks for it. On
Apple Silicon with Node 26.13.1, fresh processes read and consumed 100,000
small rows in four runs per method. Median peak RSS for `run` and `stream`
was about 183 MiB and 256 MiB on embedded 4.0.1 with SurrealKV, and 168 MiB
and 293 MiB for the client on WebSocket 3.3.4. Streaming provides early
row delivery without a memory bound.

#### Live queries

`live(table)` builds a typed `LIVE SELECT` over a table descriptor or a
`readAs` view. `where` and `project` use the same expressions and binding
codecs as `select`.

```ts
import { Effect, Stream } from 'effect';
import { Int } from 'effect-surrealkit';
import { SurrealClient } from 'effect-surrealkit/client';
import { gt, live, param } from 'effect-surrealkit/statement';

const adults = live(PersonTable)
  .where((row) => gt(row.age, param('minimum', Int)))
  .project((row) => ({ name: row.name }))
  .build('adult changes');

const listen = Effect.scoped(Effect.gen(function* () {
  const client = yield* SurrealClient;
  const changes = yield* client.liveScoped(adults, { minimum: 17 });
  return yield* Stream.runForEach(changes, (event) => onChange(event));
}));
```

The registration sends `LIVE SELECT name AS name FROM ⟨person⟩ WHERE age >
$minimum;`. When `liveScoped` succeeds, the listener is ready to receive
changes even before the first pull. Closing the scope kills the subscription.
If the SDK kill fails, the client sends a separate KILL for that query ID on the
same session. If cleanup still cannot be confirmed, scope release logs a warning.
A failed attachment also reports when its registration could not be cleaned up.
Use the client's `live` stream to start listening on the first pull and kill
the subscription when that stream ends.
`timed` records registration and scope cleanup as separate requests.

Each `CREATE`, `UPDATE` or `DELETE` has a separate typed record `id` and a
typed `value`. `DELETE` supplies the last row before deletion, including
the selected projection. The client preserves the engine's delivery order.
On embedded 4.0.1 and WebSocket 3.3.4, an update
that stops matching `WHERE` emits no event. An update that starts matching
again emits `UPDATE`. Live events honor the session's table and field
permissions, and `readAs` supplies the restricted row codec.

An invalid event fails with `StatementDecodeError`. Server-side `KILLED`
and a disconnected session end the stream with `LiveQueryClosed`. HTTP
connections refuse live queries with `StatementUnsupported`. Embedded
SurrealDB 4.0.1 still delivered changes with
`capabilities.live_query_notifications` set to `false` in the Node engine
configuration.

Live queries here start from a table. They provide change events without a
baseline read. `DIFF`, `FETCH`, record-ID and range targets, ordering,
limits, grouping, and `only` are outside this API.

#### Transports

The matrix uses the SDK's native values on embedded SurrealDB, WebSocket
RPC, and HTTP RPC at `/rpc`. Tested kinds include integers beyond
JavaScript's safe range, decimals, NaN and infinities, datetimes and
durations with nanoseconds, UUIDs, bytes, sets, geometry, composite
record IDs, and nested lists and objects.

| Capability | 4.0.1 embedded | 4.0.0 WebSocket | 4.0.0 HTTP | 3.3.4 WebSocket | 3.3.4 HTTP |
| --- | --- | --- | --- | --- | --- |
| JavaScript SDK | 2.2.2 | 2.2.2 | 2.2.2 | 2.2.2 | 2.2.2 |
| `run` | 1 request, client timing only | 1 request, client timing only | 1 request, client timing only | 1 request, client timing only | 1 request, client timing only |
| `transaction` | 1 request, client timing only | 1 request, client timing only | 1 request, client timing only | 1 request, client timing only | 1 request, client timing only |
| `pipeline`, each statement | 1 request, server timing | 1 request, server timing | 1 request, server timing | 1 request, server timing | 1 request, server timing |
| `pipeline`, each transaction | 1 request, server timing | 1 request, server timing | 1 request, server timing | 1 request, server timing | 1 request, server timing |
| `stream` | 1 request, server timing | 1 request, server timing | 1 request, server timing | 1 request, server timing | 1 request, server timing |
| `withTransaction`, one statement | 3 requests, server timing | 3 requests, server timing | Unsupported | 3 requests, server timing | Unsupported |
| `liveScoped`, start and kill | 2 requests, client timing only | 2 requests, client timing only | Unsupported | 2 requests, client timing only | Unsupported |
| `bulk`, two chunks, atomic | 4 requests, server timing | 4 requests, server timing | 1 request, client timing only | 4 requests, server timing | 1 request, client timing only |
| `bulk`, two chunks, non-atomic | 2 requests, client timing only | 2 requests, client timing only | 2 requests, client timing only | 2 requests, client timing only | 2 requests, client timing only |
| Checked `-0` binding | refused before dispatch | refused before dispatch | refused before dispatch | refused before dispatch | refused before dispatch |
| Unchecked `-0` binding | returned -0 | returned -0 | returned -0 | returned -0 | returned -0 |
| Engine float literal `-0.0f` | preserved | preserved | preserved | preserved | preserved |

The counts include begin and commit for an interactive transaction, and
start and kill for a live query. The interactive example runs one
statement. Each bulk example writes two chunks. Timing describes what
the client reports for these requests.

SDK 2.2.2 encodes JavaScript `-0` as a CBOR float and preserved it on all
five transports when sent with `unsafeSkipValueCheck`. With value checking
enabled, a bound `-0` still raises `StatementEncodeError` before dispatch
and names the binding path. This protects callers on older SDK versions such
as 2.1.1, below the supported floor, which encode it as positive zero.

#### Measured costs

Five fresh Node processes and databases per case on macOS, Apple M2
Ultra with 128 GiB RAM. Runs used Node 26.13.1, SurrealDB SDK 2.1.1 and
Effect 4.0.6 with the execution implementation measured at `799ab96`.
WebSocket connected to a local in-memory server.

| Workload | Embedded 4.0.1 | WebSocket 3.3.4 |
| --- | ---: | ---: |
| 5,000 inserts, sequential `run` | 1,275 ms | 1,218 ms |
| 5,000 inserts, `run` with concurrency 64 | 636 ms | 444 ms |
| 5,000 inserts, non-atomic `bulk` | 51 ms | 69 ms |
| 5,000 inserts, atomic `bulk` | 56 ms | 219 ms |
| Eight sequential reads with `run` | 10.0 ms | 10.9 ms |
| Eight reads in one `pipeline` | 8.1 ms | 7.1 ms |
| 20,000 generated `doc` rows with `run` | 137 ms, 209 MiB | 126 ms, 179 MiB |
| 20,000 generated `doc` rows with `stream` | 328 ms, 220 MiB | 338 ms, 194 MiB |
| Strict decoding of pre-fetched `doc` rows | 2.59 μs/row | 2.86 μs/row |
| Two writes in one `transaction` | 1.12 ms | 0.77 ms |
| Two writes using `withTransaction` | 1.85 ms | 1.40 ms |

The table reports medians. Bulk used chunks of 500. The two-write
figures are per transaction from 100 repetitions in each sample.
Peak RSS includes setup and the embedded engine when it shares the
client process. Stream timing includes consuming all 20,000 rows.
The streamed read had higher peak process RSS in this workload.

```sh
npm run build
npm run --silent bench
SURREAL_BIN=/path/to/surreal npm run --silent bench > benchmark.json
```

The binary adds a WebSocket run. The JSON includes all five samples for
each case. The benchmark runs separately from the test suite.

### Record links and graph traversal

Links, graph steps, recursive paths and `FETCH` are typed from the store
registry the emitter writes (`_store.ts`): a link's target and a step's
relation are found there by name, and nothing else describes them.

```ts
import * as SurrealSchema from 'effect-surrealkit';
import { collect, edges, eq, follow, outgoing, param, select } from 'effect-surrealkit/statement';
import { DocTable, MemberOfTable, PersonTable, store } from './generated/surreal/index.js';

const byOwner = select(DocTable)
  .where((row) => eq(follow(store, row.owner).name, param('owner', SurrealSchema.String)))
  .project((row) => ({
    title: row.title,
    teams: outgoing(store, row.owner, 'member_of', {
      where: (edge) => eq(edge.role, param('role', MemberOfTable.fields.role)),
    }),
    roles: follow(store, edges(outgoing(store, row.owner, 'member_of'))).role,
    reach: follow(store, collect(row.owner, (node) => outgoing(store, node, 'knows'))).name,
  }))
  .build();
// SELECT title AS title, owner->(member_of WHERE role = $role)->? AS teams,
//   owner->member_of.role AS roles, owner.{..+collect}(->knows->?).name AS reach
//   FROM ⟨doc⟩ WHERE owner.name = $owner;

const withManager = select(PersonTable).fetch(store, 'manager', 'watchers').build();
// SELECT * FROM ⟨person⟩ FETCH manager, watchers;
```

| Function | SurrealQL | Yields |
| --- | --- | --- |
| `follow(store, link).field` | `manager.name`, `watchers.name` | each field of the target, possibly NONE; a list through a list of links |
| `rows(store, link)` | `manager.*` | the target's row (its table's select schema), or a list of them |
| `destructure(store, link, (target) => ({ … }))` | `manager.{ n: name }` | the projected object, or a list of them |
| `outgoing(store, from, 'member_of', { where?, whereTarget? })` | `id->member_of->?`, `id->(member_of WHERE …)->?`, `id->member_of->(? WHERE …)` | the ids on the relation's `out` side, as a list |
| `incoming(store, from, 'member_of', { where?, whereTarget? })` | `id<-member_of<-?` | the ids on its `in` side |
| `both(store, from, 'knows', { where?, whereTarget? })` | `id<->knows<->?` | both endpoints of each edge, `from` included once per edge |
| `edges(step)` | `id->member_of` | the ids of the edges the step walks, for `follow` and `rows` |
| `recurse(from, { min, max }, (node) => step)` | `id.{1..3}(->knows->?)` | the records the last repetition that reached any record reached |
| `collect(from, step, { min?, max?, inclusive? })` | `id.{..+collect}(->knows->?)` | every distinct record reached (`+inclusive` adds `from`) |
| `shortest(from, to, step, { inclusive? })` | `id.{..+shortest=$to}(->knows->?)` | the records of a shortest path to `to`, or NONE |
| `select(table).fetch(store, ...paths)` | `… FETCH manager, owner.team` | each named link replaced by its target's row |
| `selectFrom(store, step)` | `SELECT * FROM $who->knows->? WHERE …` | the records a step, the edges of one or a recursive path reaches, as rows of their one table, to filter, order, limit, project, fetch and group |

What the types say, and the engine behaviour behind it (pinned in
`test/conformance.test.ts` on SurrealDB 3.3.4 and 4.0.1):

- **A link may not lead anywhere.** SurrealDB answers NONE for a field read
  through a link whose record is missing or the session may not read, and
  nothing stops a `record<t>` field holding such a link, so every field
  `follow` yields may be NONE; through a list of links each element may be.
  `rows` and `destructure` of one link may be NONE for the same reason.
- **Several tables, no table.** A link to several tables (`record<org | team>`)
  exposes the fields every target has, with the codecs of all of them (a
  field only some have reads NONE on the others); a link to any table
  (`record`) exposes none and is a compile error.
- **Steps.** A step is legal only from a record of a table the relation
  accepts on that side; any other start is a compile error naming the table.
  A step yields a list; chained steps (`id->knows->?->knows->?`) flatten into
  one list with duplicates kept. From a list of links, or from the records
  of a recursive path, SurrealDB yields one list per record instead, so a
  step refuses to start there. From an optional link the list itself may be
  NONE.
- **Edges and missing records.** The engine leaves out of a step's list the
  records the session may not read (their edges stay readable). An
  `ENFORCED` relation cannot point at a missing record, so its records are
  rows, never NONE; one that is not enforced can (`RELATE` to a missing
  record succeeds), and its records read as NONE where they are missing.
  Deleting a record deletes its edges. So a record an `ENFORCED` relation
  reaches can read NONE only when the live schema is not the one the
  descriptors were generated from, and decoding it fails with a message
  that names the relation and says so; `schemaDrift` from
  `effect-surrealkit/testing` lists such differences.
- **Filters and destructuring read the edge or the target.** Inside a step's
  `where` and a `destructure` callback, field paths are relative to the edge
  or target, so a field of the outer row would read the edge's or target's
  field of the same name (`ne(edge.out, row.id)` would be `out != id` on the
  edge). Every row object knows which row it was written for, and a path of
  another row is refused when the step, the destructuring, the recursive
  path or the select around it is built, with a message naming the field;
  so is a field of a row in a block or a bare `RETURN`, which have none.
  Bindings, constants and a block's locals read no row and are always legal.
  Read the select's row from a step's `where` with `parent(row.field)`
  (`$parent.field`):

  ```ts
  select(PersonTable).project((row) => ({
    peers: outgoing(store, row.id, 'knows', {
      where: (edge) => ne(follow(store, edge.out).manager, parent(row.id)),
    }),
  }));
  // SELECT id->(knows WHERE out.manager != $parent.id)->? AS peers FROM ⟨person⟩
  ```

  SurrealDB 3.3 and 4.0 agree on `$parent` in a step's `where` directly
  under a select's row: from the row, from a binding, in either step of a
  chain, in `WHERE`, and with the step then followed or destructured. They
  disagree elsewhere, so `parent` is refused there when the statement is
  built: inside a destructuring (3.3 binds the outer row, 4.0 the
  destructured record), under a recursive path (4.0 binds the select's row,
  3.3 matches nothing), under a bare `RETURN` or a block with no select
  (4.0 binds something, 3.3 nothing), in a step inside another step's
  `where`, in a subquery of another select, a write or a grouped select.
  Elsewhere, bind the value with `param`.
- **Recursion.** A recursive path repeats a step that leads back to the table
  it starts from, at depths 1 to 256 (the engine refuses 0 and 257 and
  answers NONE for a reversed range, so the builder refuses all three).
  `{min..max}` keeps the records of the last repetition that reached any,
  and is empty when fewer than `min` repetitions reach a record; `collect`
  keeps each record once; `shortest` is NONE when `to` cannot be reached.
- **FETCH.** A path names a field of what the select returns, the stored row
  or the projection's aliases (`SELECT manager AS boss … FETCH manager`
  fetches nothing), so `fetch` comes after `project`, and `SELECT VALUE`
  takes none: a projection, `value`, `omit`, `split` or grouping after
  `fetch` is a compile error that says so (``call `project` before
  `fetch` …``). A nested path (`owner.manager`)
  fetches its prefix too. A fetched link becomes `field?: Row | undefined`,
  absent where the record is missing or unreadable; a fetched list of links
  becomes a list of `Row | undefined`. Rows decode with the target's
  generated select schema, the row a privileged reader gets; for a
  restricted session a field its permissions hide is absent from the
  fetched record, as from `SELECT *`, so fetch through `readAs(store)`
  (see [permissions](#what-the-statements-do-not-know)).
- **SurrealDB 3 and 4 differ** on a link to a record of a table that does
  not exist: 3.3 reads NONE, 4.0 fails the statement ("The table … does not
  exist"). A link typed from the store always names a table of the store.

- **The records a step reaches.** `whereTarget` keeps the records that
  satisfy a condition on their fields (`->member_of->(? WHERE name =
  $name)`), after the edge filter, in each direction, chained, followed,
  destructured and repeated alike on 3.3 and 4.0; its callback reads the
  fields every table on that side has. `edges` refuses such a step, whose
  edges would include those to the records it leaves out. `selectFrom`
  reads the records as rows of their table (`SELECT * FROM
  $who->knows->?`): the engines read none from a missing or NONE start,
  keep the duplicates of chained steps, and read a step from the outer row
  in a subquery from that row (`(SELECT … FROM id->knows->?)`).

Not supported: a step to one table of several (`->member_of->team`), so
`selectFrom` takes the records of one table only; several relations in one
step (`->(likes, follows)->`); `+path`; ordering by a field read through a
link; `FETCH` under `SELECT VALUE`; and `FETCH` inside a step.

### Putting it together

A block binds a hybrid search ordered by its score, whose projection calls
a built-in function and follows a record link, and guards and returns with
built-in functions (`bestMatch` in `test/blocks.ts`, which
`test/search-engine.test.ts` runs on SurrealDB 3.3.4 and 4.0.1):

```ts
import { array, string } from 'effect-surrealkit/functions';
import {
  and, block, eq, follow, knn, knnDistance, match, param, score, select,
} from 'effect-surrealkit/statement';

const terms = match(DocTable, 'body', param('terms', SurrealSchema.String), { ref: 1 });
const near = knn(DocTable, 'embedding', 'query', { k: 3, ef: 40 });

export const bestMatch = block()
  .let('hits', () =>
    select(DocTable)
      .where(() => and(terms, near))
      .project((row) => ({
        id: row.id,
        title: string.uppercase(row.title),
        owner: follow(store, row.owner).name,
        score: score(terms),
        distance: knnDistance(near),
      }))
      .orderByAlias('score', 'DESC')
      .build())
  .throwIf(($) => eq(array.len($.hits), 0), 'No match')
  .return(($) => ({ best: array.first($.hits), matches: array.len($.hits) }))
  .build('best match', { writes: false });

// Result<typeof bestMatch>: {
//   best: { id: DocId; title: string; owner: string | undefined; score: number; distance: number } | undefined;
//   matches: number;
// }
// Thrown<typeof bestMatch>: 'No match'
// Input: { terms: string; query: ReadonlyArray<number> }
```

```sql
{
	LET $hits = (SELECT id AS id, string::uppercase(title) AS title, owner.name AS owner, search::score(1) AS score, vector::distance::knn() AS distance FROM ⟨doc⟩ WHERE body @1@ $terms AND embedding <|3,40|> $query ORDER BY score DESC);
	IF array::len($hits) = 0 { THROW s"No match" };
	{ best: array::first($hits), matches: array::len($hits) }
};
```

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

  The reader is a property of the store view too. `follow`, `rows`,
  `destructure`, the steps, recursive paths and `fetch` take the store, and
  `readAs(store)` is the store as such a session reads it: every table
  reads through its `visible` row, so a record reached through a link, a
  step, a nested projection or a FETCH decodes with it, hidden fields
  absent. A statement whose own table is read as one reader while a link,
  step or FETCH in it reads through a store view of the other is refused
  when it is built (each subquery answers for its own):

  ```ts
  const session = readAs(store);
  select(readAs(DocTable))
    .project((row) => ({
      owner: row.owner,
      teams: destructure(session, outgoing(session, row.owner, 'member_of'), (team) => ({
        name: team.name,
      })),
    }))
    .fetch(session, 'owner')
    .build();
  // SELECT owner AS owner, owner->member_of->?.{ name: name } AS teams FROM ⟨doc⟩ FETCH owner;
  ```

  Under such a session the list of a step's edges and the list of the
  records it reaches can differ in length: the engine leaves out a record
  the session may not read and keeps its edge (SurrealDB 3.3 and 4.0
  alike), so the two lists do not pair by position. Pair them from the
  edge instead: project each edge and reach its record from the edge's
  endpoint, which reads NONE where the record is hidden
  (`destructure(session, edges(step), (edge) => ({ role: edge.role, team:
  follow(session, edge.out).name }))`). `test/reader-engine.test.ts`
  proves a link, a step, a nested projection, a FETCH and this pairing on
  both engines as a record user.

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
- **Change feeds.** A bound `SINCE $since` in `SHOW CHANGES` fails on
  SurrealDB 3.3.4 and 4.0.1 with a parse error beginning
  ``Unexpected token `a parameter`, expected a version stamp``.
  `LIMIT $limit` is also refused. A dynamic change-feed cursor would
  require putting an input value into the statement text.
- **Coverage.** Views have no typed builder yet. Use `unsafeExpr` or
  authored SurrealQL. `ON DUPLICATE KEY UPDATE` takes plain
  assignments only (no `+=`), and an aggregate takes a field, not an
  expression over fields. For graph traversal, see [what is not covered
  yet](#record-links-and-graph-traversal).

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
| `schemaDrift(store)` | The differences between what a store registry declares and what the `TestDatabase` defines, as a list (empty for a match): a declared table that is missing or of another kind, a relation whose `ENFORCED` flag or endpoint tables differ, and a declared index that is missing, of another kind or over other fields. It reads `INFO FOR DB STRUCTURE` and `INFO FOR TABLE … STRUCTURE`, which SurrealDB 3.3 and 4.0 answer alike. |
| `expectIndex(statement, input, index)`, `expectNoIndex(…)` | Succeed when the engine's plan of a `SELECT` does (does not) read `index`; otherwise an `IndexExpectationError` naming the indexes it does read. `explain(statement, input)` returns the plan and its indexes. |
| `fake(script)`, `fakeLayer(script)` | A client without an engine. It answers each statement by name with a value, a scripted reply, or a function of its encoded bindings. `calls` records requests and lifecycle operations. |
| `result(value)`, `thrown(message)`, `conflict()`, `failure(message)` | Reply constructors for ordinary result slots and failures. |
| `checkConstraintErrors` | An effect over `SurrealClient` that defines the table `effect_surrealkit_constraint_check` with a unique index, writes a duplicate value and an id that exists, removes the table, and returns `{ indexConflict, recordExists, got }`: whether each failed as `StatementIndexConflict` and `StatementRecordExists`, and the tag each failed with. Run it against a disposable database on a new engine version before relying on the two errors. |
| `indexConflict(index)`, `recordExists()` | A duplicate on the unique index `index`, and a record id that exists, as SurrealDB reports them: `StatementIndexConflict` for a statement that declares `index` (otherwise `StatementRequestError`), and `StatementRecordExists`. |
| `events(list)`, `unsupported(feature)` | Script a live subscription's events or a feature refusal, such as `{ begin: unsupported('interactive transaction') }`. |
| `verifyStatements({ scopes, schema?, holes?, timeoutSeconds? })` | Sends each complete authored text once, with no bindings, to a discarded in-memory engine after the schema files, and fails with a `StatementVerificationError` on a parse error, a result count other than the analyzer's `slots`, or a text that does not finish within the timeout; what `statements --verify` runs. See [Checking texts on an engine](#checking-texts-on-an-engine---verify). |
| `conflictingSession(session, { marker, times })` | Wraps an SDK session, injecting a transaction conflict into the first `times` sent requests with matching text. Returns the wrapped `session` and the `matched.count` readback. |

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
request. Recorded calls use each statement's original binding names.

`conflictingSession` leaves the injected request unsent. When a request
has several response slots, the first matching slot has the conflict
and the others report that they did not execute. Later matching requests
pass through to the engine and continue incrementing `matched.count`.

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
| `float` | `Float` (native IEEE-754, including NaN, infinities and signed zero on reads; checked bindings refuse negative zero) / `Schema.Finite` (JSON) | `number` |
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

Every schema carries `toCode` metadata. In Effect 4.1.5,
`Codegen.Module.addSchemas` and `finalize` print schemas built from them back
to source after conversion with `SchemaRepresentation.toRepresentation`.

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

A user-visible change updates the agent skill
([`skills/effect-surrealkit`](../../skills/effect-surrealkit/SKILL.md) at the
repository root) in the same change. `test/skill.test.ts` fails when the skill
names an export the package does not have, a flag the command does not list,
or a README section, test or file that does not exist.

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
fixed limit. `test/execution-budget.test.ts` adds separate compiler budgets
for 30 eight-member pipelines and 30 bulk calls. `test/requests.test.ts`
counts transport calls for each execution method.

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

Which tests a lane skips is pinned. `bun run test:skips` runs the whole suite
on the lane the same variables select and exits 1 when its skipped tests
differ from `test/skips/<lane>.txt` in either direction, printing each name
that was skipped and is not listed, or is listed and was not skipped. The
lanes are `embedded`, `binary-<major>.<minor>` for `SURREAL_BIN` (by the
version the binary reports) and `remote` for `SURREAL_URL`, with `-http`
appended under `SURREAL_TRANSPORT=http`. `embedded` and `binary-3.3` are
committed; a lane without a list is reported as unpinned and not compared.
After a change that is meant to skip or un-skip a test, rewrite the list from
a passing run and commit it with the change:

```sh
bun run test:skips                                                   # embedded
SURREAL_BIN=/usr/local/bin/surreal bun run test:skips                # binary-3.3 with a 3.3 binary
UPDATE_SKIPS=1 SURREAL_BIN=/usr/local/bin/surreal bun run test:skips # rewrites test/skips/binary-3.3.txt
```

The content mapper has a suite of its own, because it needs TypeScript 7.1
and the package's type check stays on TypeScript 5.9. `test/mapper-fixtures`
pins `typescript@7.1.0-beta`, and `bun run test:mapper` (after `bun run
build`, and `npm ci` in that directory) type-checks its `accepts` project with
`tsc --runExternalCode`, checks that `rejects` reports exactly
`rejects/expected.txt`, and runs `accepts/src/run.ts` under the Node hook and
the Bun plugin against the texts the mapper gave the type check. It then
asks the mapper process for every authored fixture, with and without its
schema, and loads each under the Node hook and the Bun plugin as the same
module (`bun` must be on `PATH`). `test/mapper.test.ts`, in the ordinary suite, holds the byte
equality with the command and the protocol.

```sh
bun run build
(cd test/mapper-fixtures && npm ci)
bun run test:mapper
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
