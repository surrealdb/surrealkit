# effect-surrealkit

[Effect](https://effect.website) v4 schemas for SurrealDB values exactly as
the JavaScript SDK (`surrealdb` 2.x) decodes and encodes them, and an emitter
that turns a SurrealKit schema document v2 into typed Effect modules.

```sh
npm i effect-surrealkit effect@4.1.5 surrealdb@2.2.2
```

Every runtime export imports only `effect` and `surrealdb` value classes, so it
runs in browsers, Workers, Bun and Node. The emitter (`effect-surrealkit/emitter`)
is host-neutral too, and so is the SurrealQL analyzer
(`effect-surrealkit/analyze`); only the `effect-surrealkit` bin touches the
filesystem.

- [Generating modules](#generating-modules) from a schema document
- [Analyzing SurrealQL](#analyzing-surrealql): what an authored text reads,
  defines, returns, throws and names
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
| `pipeable` | Emit nested unary field schemas such as `Schema.Array(item).pipe(Schema.optionalKey)`. Enable in JSON or with `--pipeable`. Defaults to false. |
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
| `PersonTable` | The descriptor: `{ name, type, id, select, visible, create, merge, content, fields, presence, relations, permissions, fieldPermissions, indexes, edge? }`. |
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

Regenerating with this version changes the schemas of some fields, adds to
the output and tightens one schema. Every existing export keeps its name.

The schemas change for schemas that use these kinds (see
[Interpretation notes](#interpretation-notes) and the kinds table under
[Two codec worlds](#two-codec-worlds)):

- `any` is `Schema.UndefinedOr(SurrealSchema.SurrealValue)`, and
  `SurrealSchema.SurrealJsonValue` in the JSON world, instead of
  `Schema.Unknown`. An open `object`, the extra keys of a `FLEXIBLE` or
  SCHEMALESS row and the items of an `array<any>` or `set<any>` use
  `SurrealValue` and `SurrealJsonValue` the same way, and the extra keys of
  an open row's Merge schema use `SurrealMergeValue`. Their types are those
  values instead of `unknown`.
- `float` is `SurrealSchema.Float` instead of `Schema.Number`, and
  `Schema.Finite` in the JSON world.
- A field whose `VALUE` ignores its input is excluded from Create, Merge and
  Content, where it was optional, and its presence entry says so.
- `ids.ts` declares a record ID after the IDs its key refers to, so the
  order of its declarations can change.

The output gains:

- every table descriptor gains `fieldPermissions`, `indexes` and `visible`,
  and every table module a `<Table>Visible` export (with its `Type` and
  `Encoded` types), so a table whose PascalCase name is another table's
  plus `Visible` (`person` and `person_visible`) now needs a `names` entry;
- a new module, `_store.ts`, exports `store` and its type `Store`, and
  `index.ts` re-exports `store`.
  A SurrealKit-run emitter records the new file in `.surrealkit-emit.json`
  as it does every generated file.

Code that spells out a descriptor's full type, or compares one with
`toEqual`, sees the new keys. They are optional in `TableDescriptor`, so
hand-written descriptors without them keep working.

One schema is stricter at run time, with the same type: a closed object with
no fields (the Create, Merge and Content of a SCHEMAFULL table whose only
field is its id, or a `{}` literal kind) is now `SurrealSchema.EmptyObject`
instead of `Schema.Struct({})`, so a value other than `{}` fails to decode or
encode instead of reaching SurrealDB (see
[Interpretation notes](#interpretation-notes)). The generated modules need
an `effect-surrealkit` that exports `EmptyObject`.

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

## Analyzing SurrealQL

`effect-surrealkit/analyze` reads an authored SurrealQL text and reports what
it does with names, results, `THROW`s, tables, functions and fields. It is
pure and host-neutral: it imports only `effect`, reads no files and connects
to nothing. It is a lexer, not a parser, and its rule is to record nothing
where it is not certain.

```ts
import { analyzeSync } from 'effect-surrealkit/analyze';

const analysis = analyzeSync(`-- Records a reading, then lists the station's recent ones.
IF $amount < 0 { THROW 'Negative reading' };
CREATE reading SET station = $station, amount = $amount, at = time::now();
SELECT amount, at FROM reading WHERE station = $station AND at > $since;
`);
// analysis.free:      ['amount', 'since', 'station']
// analysis.slots:     3 (one result per terminated top-level statement)
// analysis.throws:    ['Negative reading'], thrown on line 2
// analysis.writes:    true
// analysis.tables:    { read: ['reading'], written: ['reading'] }
// analysis.uses.station: assigned to reading.station (line 3),
//                        compared with reading.station (line 4)
// analysis.uses.since:   compared with reading.at (line 4)
// analysis.projected: reading.amount and reading.at (line 4)
// analysis.comment:   "Records a reading, then lists the station's recent ones."
```

`analyze(sql, { holes? })` returns an `Effect` of the analysis, or fails with
an `AnalyzeError` carrying a code, a line and a column; `analyzeSync` throws
the same error. The analysis is `{ kind, sql, comment, free, defines, slots,
valueSlot, throws, throwsAt, holes, holeScopes, provisional, transactional,
writes, tables, functions, openTargets, uses, projected, fields, diagnostics
}`:

- **Lexing.** `--`, `//`, `#` and `/* */` comments; `'…'` and `"…"` strings
  with SurrealQL's escapes, prefixed (`r` `d` `u` `s` `f` `b`); `` `…` `` and
  `⟨…⟩` identifiers; regex literals; `$name`. An unterminated string,
  identifier or comment, an invalid escape, unbalanced brackets, an empty
  statement or a character it cannot classify is an error, never a guess.
- **Free and defined names.** `free` is every `$name` read before anything in
  scope defines it, including a read before the `LET` of the same name.
  A top-level `LET` is in `defines` and visible to later statements; a `LET`
  inside `{ }`, a `FOR $x IN …` variable, closure parameters (`|$a: int,
  $b|`) and `DEFINE FUNCTION` parameters are local to their body. `$auth`,
  `$access`, `$session` and `$token` are never free and a `LET` of one is an
  error; `$this`, `$self` and `$parent` are never free. `$value`, `$input`,
  `$before`, `$after` and `$event` are ordinary script names (free unless
  defined, with a `contextualName` warning), except inside a `DEFINE` other
  than `FUNCTION` and `PARAM`, where the engine sets them.
- **Results.** Each terminated top-level statement is one result (`LET`,
  `IF`, `FOR`, `BEGIN` and `COMMIT` included): `slots`. `valueSlot` is the
  position of the last one that is not `BEGIN`, `COMMIT` or `CANCEL`. A text
  with its own `BEGIN`, `COMMIT` or `CANCEL` is `transactional`. A `RETURN`
  inside `BEGIN … COMMIT` followed by another statement is an error in
  `diagnostics`: SurrealDB 3 skips what follows, 4 runs it.
- **THROW.** `throws` holds every string-literal message, at any depth, and
  `throwsAt` the lines each is thrown on; any other `THROW` is a
  `dynamicThrow` warning.
- **Writes.** `writes` is true when `CREATE`, `INSERT`, `UPDATE`, `UPSERT`,
  `DELETE`, `RELATE`, `DEFINE`, `REMOVE` or `ALTER` starts a statement or a
  subquery anywhere in the text, outside strings and comments. No write found
  does not prove the text read-only: a function it calls may write.
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
  contribute nothing.
- **Comment.** `comment` is the comments that open the text, as prose (`--`,
  `//` and `#` without their marker, a `/* … */` without its delimiters and
  `*` margin, a blank line between paragraphs); a comment after the first
  token is not read.
- **Fragments.** A single expression or block without a final `;` is a
  `fragment` (`kind: 'fragment'`): no results, usable only to fill a hole.
  Several statements with an unterminated last one are an error.
- **Holes and templates.** Outside strings and comments, a name matching
  `[A-Z][A-Z0-9_]*` between two literal delimiters, `__NAME__` by default,
  is a hole; `{ holes: { open, close } }` sets other delimiters, which are
  matched as text, never compiled into a pattern. A text with holes is a
  template; its `free` and `defines` are `provisional`, because a fill can
  read or define names. `fill(template, fills)` returns the filled text,
  failing with a `FillError` on an unknown hole, a missing fill or a fill
  with holes of its own (unless `allowHoles`); each fill must lex on its own
  with balanced brackets. Analyse the result to know what it does.
  `holeScopes` gives, for each hole, the names visible at every occurrence
  (`bound`) and the free names a `LET` placed there would define for every
  read (`binds`, only where the hole starts a statement). `text({ … })`
  declares a fragment or template with its names kept as literal types.

```ts
import { fill } from 'effect-surrealkit/analyze';

const filled = fill('SELECT * FROM person WHERE __FILTER__;', {
  __FILTER__: 'age > $min',
});
// An Effect that succeeds with 'SELECT * FROM person WHERE age > $min;'
// or fails with a FillError.
```

### What bindings are used as

`uses` holds, for each free binding (keyed without `$`), what the text uses
it as, sorted and without duplicates. Three patterns are recognised, each
on tokens outside strings and comments:

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
  subquery) whose targets are exactly one static table and not open, and,
  for a `SELECT`, with no subquery in its `FROM` (the rows are then the
  subquery's results, not rows of its table), a comparison (`=`, `==`,
  `!=`, `<`, `<=`, `>`, `>=`) in its `WHERE` between a bare field name and a
  parameter (`role: 'compare'`), or an assignment `field = $name` in the
  `SET` of an `UPDATE`, `UPSERT` or `CREATE` that is a top-level statement
  or the whole value of one (`(UPDATE …);`, `LET $r = (UPDATE …);`, `RETURN
  (UPDATE …);`) and has no `WHERE` of its own (`role: 'assign'`). `SELECT *
  FROM gauge WHERE station = $station;` uses `$station` as `gauge.station`.

Nothing else is recorded: a parameter in a longer expression, a path, two
tables or an open target, a subquery in `FROM`, a comparison inside a
block. A statement in a block records no binding use because it may run for
some values of a binding and not others (`IF $x != NONE { UPDATE gauge SET
station = $x }`): recording the assignment would claim that NONE can reach
`station`, which the text never lets happen. For the same reason an
assignment records no use anywhere a condition can skip it, which SurrealDB
3.3 and 4.0 both show running without an error when the binding is NONE:

- in a parenthesised query that is not the whole value of a top-level
  statement: after `IF … THEN` or `ELSE` without braces, behind `AND`, `&&`
  or `OR`, in a projection or `WHERE` of another query, in a closure (a
  function argument or an array item skips nothing, but a lexer does not
  tell it from these);
- in a query with a `WHERE` of its own (`UPDATE gauge SET station = $x
  WHERE $x != NONE`). No reading of the `WHERE` can rule out that it skips
  every record when the binding is NONE: it can test a local `LET` from
  it, call a function whose body reads the caller's bindings (both
  versions), or, on 3.3, read a `COMPUTED` field whose expression does.

A comparison in such a subquery is still a use: it accepts NONE whether or
not it runs. `??` and `?:` evaluate their right side when the left is NONE,
and a `RETURN` in a top-level `IF` block does not end the text, so these
skip nothing, yet an assignment behind `??` or `?:` records nothing either.
The field names these statements compare are still reported, in `fields`.

### Projected fields

On a `SCHEMAFULL` table SurrealDB 3.3 and 4.0 both answer `SELECT nmae
FROM person` with `{}` and no error, so a misspelt field quietly comes back
missing. `projected` holds what each `SELECT` projects, as `{ table, field,
line }` (`field` is `*` for `*`), sorted by line, table and field, without
duplicates. A `SELECT` is read when it is a top-level statement (possibly
after `LET $name =` or `RETURN`), a statement of a block (the body of `IF`,
`ELSE`, `FOR`, a function or a bare `{ … }`, read like a top-level
statement), or a parenthesised subquery, which is a query of its own (in a
projection too, against its own table), and when all of this holds on
tokens outside strings and comments:

- it has exactly one `FROM` outside brackets;
- it has no `OMIT`;
- after `FROM` (and an optional `ONLY`) comes exactly one target: a table
  name, a record id `table:key` whose key is one token or one bracket
  group, `type::record('table', …)`, `type::thing('table', …)` or
  `type::table('table')` with a plain string literal, or a local the text
  fixes to one table (see [A local fixed to one
  table](#a-local-fixed-to-one-table)); then the end of the query or one of
  its clauses (`WHERE`, `SPLIT`, `GROUP`, `ORDER`, `LIMIT`, `START`,
  `FETCH`, `TIMEOUT`, `EXPLAIN`, `WITH`, `VERSION`, `TEMPFILES`);
- it has no `PARALLEL` after `FROM`, a parse error on both versions;
- a `FOR UPDATE` is read only in the form both versions accept: every
  target a record id (`table:key`, `type::record` or `type::thing`), no
  `GROUP`, `SPLIT` or `VERSION`, and nothing after it but `TEMPFILES` and
  `EXPLAIN [FULL]`. The lock changes neither the target nor the fields
  returned. On a table, a range or an array the engine refuses it
  ("requires record ids as targets"), and the analyzer records nothing.

An engine test runs each of these clauses on both versions and checks that
it returns the same fields as the query without it (`EXPLAIN` returns a
plan, which names the same projection); `VERSION` needs a versioned store,
which an in-memory engine is not, so the test does not run it. The same `FOR UPDATE` rule decides
the tables a text reads and the binding uses of its `WHERE`.

Of such a `SELECT`, each projected item of exactly one of these forms is
recorded, against that table:

- `*`;
- one bare field name: a word or a quoted name, except the words SurrealQL
  reads as values, `NONE`, `NULL`, `TRUE` and `FALSE` in any case and `NaN`
  and `Infinity` spelt so (an engine test tries every keyword of both
  versions' parsers on both engines);
- one bare field name, `AS`, and one name: the field is recorded, never the
  alias;
- `SELECT VALUE` followed by one bare field name and then `FROM`.

Every other item records nothing: a function call, a path (`address.city`,
`tags[0]`), an expression, an alias of anything but one bare field, a graph
step and a subquery, and `VALUE` before anything but one bare field. Two
targets, an open target (`$owner` unless the text fixes it to one table,
`type::table($t)`, a subquery, a range or a graph step after the table) and
a hole after the target record nothing. A `{ … }` whose entries start with
a key and `:` is an object literal, not a block, and has no statement.

### Compared and assigned fields

SurrealDB 3.3 and 4.0 both answer `SELECT * FROM person WHERE nmae =
'Ada'` with no rows and no error: on a `SCHEMAFULL` table a field it does
not declare is NONE in every row, so the comparison never holds. `fields`
holds `{ table, field, role: 'compare', line }` for each bare field a query
compares in its `WHERE`, whatever the other operand, sorted by line, table,
field and role, without duplicates. A comparison already recorded as a
binding use (`station = $station`) is not repeated there. A comparison is
read where `uses` reads one: in a query whose targets are exactly one
static table and not open (or a local [fixed to one
table](#a-local-fixed-to-one-table)), as a top-level statement, a statement
in a block or a parenthesised subquery (a bare word there is a field of the
subquery's own row on both versions, never the outer one's), at the
query's own level or inside parentheses, before a second `WHERE` or `SET`
clause. A `SELECT` with a subquery in its `FROM` records nothing of its
own: `SELECT * FROM (SELECT station AS s FROM gauge) WHERE s = 'a'`
compares `s` of the subquery's results, which `gauge` need not declare, and
the subquery is read against its own table. The field is recorded when it
is a whole operand of `=`, `==`, `!=`, `<`, `<=`, `>` or `>=`:

- on the left: after `WHERE`, `AND`, `OR`, `(` or `,`, and directly before
  the operator;
- on the right: directly after an operator that follows the end of an
  operand (a name, a parameter, a literal or a closing bracket), and
  before `;`, `)`, `,`, `AND`, `OR`, a clause or `FOR UPDATE`.

A bare field is the word or quoted name `uses` reads: never `NONE`, `NULL`,
`TRUE` or `FALSE` in any case nor `NaN` or `Infinity`, never a word that
ends a clause (`AND`, `WHERE`, `ORDER`, …), and nothing followed by `.`,
`(`, `[`, `:` or an arrow. An engine test compares every keyword of both
versions' parsers on either side and checks that the analyzer records one
only where the engine reads the field of that name. A projection alias is
not visible in `WHERE` on either version, so `SELECT name AS label FROM
person WHERE label = 'Ada'` compares the field `label`. Nothing else is
recorded: a path, a call, an index, a graph step, an operand of arithmetic
or of a cast on the left, any other operator (`?=`, `@@`, `@1@`, `<|2|>`,
`~`, `CONTAINS`, `IN`, …), and names in `ORDER BY`, `GROUP BY`, `SPLIT`,
`OMIT` and `FETCH`.

An assignment is recorded as `role: 'assign'`: the target of `field =
<value>` in the `SET` of an `UPDATE`, `UPSERT` or `CREATE` (after `SET` or a
`,` of the clause), in the same queries, when the value is a literal
SurrealDB never evaluates to NONE and the assignment ends after it (at `,`,
`;`, a closing bracket or a clause): a string of any prefix, a number (`1`,
`1.5`, `-1`, `1d`, `10dec`), `TRUE`, `FALSE`, `NULL`, an array literal, or
a record id `table:key`. Every record such a statement writes then fails on
both versions when the field is not declared (``Found field 'nmae', but no
such field exists for table 'person'``). Any other value is not read,
because both versions accept NONE for a field the table does not declare:
`SET nmae = NONE`, `SET nmae = $v` with `$v` NONE, `SET nmae = 1 AND NONE`
and `SET nmae = [1][5]` all succeed, and on a row kept from before its
table became `SCHEMAFULL` they clear the old field. For the same reason
`UNSET` is never read: `UPDATE loose UNSET extra` is how such a field is
cleared. A binding assigned in `SET` where `uses` takes the assignment for
a binding use is not repeated in `fields`; anywhere else `SET field =
$name` records nothing.

One limit applies to projected, compared and assigned fields alike: a row
written while its table was schemaless keeps the fields it had when the
table became `SCHEMAFULL`, and on 3.3 and 4.0 alike `SELECT extra FROM
loose` returns them, and a comparison with `extra` matches them, although no
definition declares `extra`.

### A local fixed to one table

`LET $x = type::record('t', …);` makes `$x` a record id of `t` whatever the
key, on both versions, so a later `FROM $x`, `FROM ONLY $x`, `UPDATE $x`,
`UPSERT $x` or `DELETE $x` targets `t`, and its projected, compared and
assigned fields are read against `t` as for a static target. A local
counts only when nothing can make it hold anything else:

- its one `LET` in the whole text is a top-level statement exactly `LET $x
  = type::record('t', …);` (or `type::thing`) with a plain string literal
  for the table, and no `FOR` of the same name exists: a `LET` in a block
  is the block's own, and a second `LET` replaces the first;
- the read comes after that statement;
- it is not a name SurrealDB sets (`$this`, `$parent`, `$value`, `$before`,
  `$after`, `$auth`, …);
- the text has no hole (a fill could `LET` it), no closure (whose
  parameters can shadow it) and no `DEFINE` (a function body sees the
  caller's `LET`s on 3.3 and not on 4.0).

A binding compared with or assigned to a field of such a target is not a
binding use: `IF $revision = NONE { DELETE $marker } ELSE { UPSERT $marker
SET revision = $revision }` assigns `$revision` only when it is not NONE.
The tables a text reports (`tables`, `openTargets`) do not change: such a
target is still open there. A local whose table comes from a parameter, a
codec or a subquery is not read.

### What it does not check

It lexes, it does not parse, so it does not know whether a text is valid
SurrealQL, what a statement returns (only how many results), whether a
fragment really is one expression, or what a function body reads when it
is called; a `DEFINE PARAM` is reported as a warning and later reads of it
stay free. It does not know the schema: it reports the tables, fields and
functions a text names, and whether a `SCHEMAFULL` table declares them is
for a caller with a schema document to decide.

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
`AlreadyExists` error instead, and [surrealdb/surrealdb#7726](https://github.com/surrealdb/surrealdb/issues/7726) asks for a details kind of its own
that names the index. On any other version a duplicate may arrive as a
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

The engine, statement, field-presence, analyzer and conformance suites need
a real engine. A plain `bun run test` runs them on the in-process, in-memory
engine of `@surrealdb/node` (SurrealDB 4.0.1; a dev dependency). Set one of these to run
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
version the binary reports) and `remote-<major>.<minor>` for `SURREAL_URL` (by
the version the server answers on `GET /version`), with `-http` appended
under `SURREAL_TRANSPORT=http`. Seven lanes are pinned: `embedded`,
`binary-3.3`, `binary-3.3-http`, `remote-3.3`, `remote-3.3-http`,
`remote-4.0` and `remote-4.0-http`. A lane is named by major and minor
version, but its list is pinned against one exact release: `embedded` against
the `@surrealdb/node` engine (4.0.1), the `binary-3.3` and `remote-3.3` lists
against SurrealDB 3.3.4, and the `remote-4.0` lists against 4.0.0, the server
the CI workflow starts. The transport matrix test compares with a golden named
by the exact version (`test/golden/transports/<version>-<transport>.json`) and
skips where there is none, so on another patch release of the same minor
(a 4.0.1 server, say) it is skipped and `test:skips` reports it as skipped
and not listed. Every run prints the pinned lanes; a
lane without a list (`binary-4.0`, say) is reported as unpinned and not
compared, and a list is written only from a passing run. After a change that
is meant to skip or un-skip a test, rewrite the list from a passing run and
commit it with the change:

```sh
bun run test:skips                                                   # embedded
SURREAL_BIN=/usr/local/bin/surreal bun run test:skips                # binary-3.3 with a 3.3 binary
SURREAL_BIN=/usr/local/bin/surreal SURREAL_TRANSPORT=http bun run test:skips # binary-3.3-http
SURREAL_URL=ws://127.0.0.1:8000/rpc bun run test:skips               # remote-4.0 against a 4.0 server
SURREAL_URL=http://127.0.0.1:8000/rpc SURREAL_TRANSPORT=http bun run test:skips # remote-4.0-http
UPDATE_SKIPS=1 SURREAL_BIN=/usr/local/bin/surreal bun run test:skips # rewrites test/skips/binary-3.3.txt
```

A test that races a write against a statement sends the write with
`Engine.aside` (`test/engine.ts`), on a connection of its own, and fails when
the write lands after the statement has ended. Over HTTP the SDK sends
through `fetch`, and on Node 26.7.0 (undici 8.9.0) a request to an origin can
wait until another request to it has been answered, so a write sent through
the client under test may not be concurrent at all; Node 22 and 24 send it at
once.

The content mapper has a suite of its own, because it needs TypeScript 7.1
and the package's type check stays on TypeScript 5.9. `test/mapper-fixtures`
pins `typescript@7.1.0-beta`, and `bun run test:mapper` (after `bun run
build`, and `npm ci` in that directory) type-checks its `accepts` project with
`tsc --runExternalCode`, checks that `rejects` reports exactly
`rejects/expected.txt`, that `nested` and `nested/sub` (one file under two
entries that render it differently) each report `nested/expected.txt` and
that both loaders refuse that file, and runs `accepts/src/run.ts` under the Node hook and
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
does not state: what each statement contributes to a request's results, the
stateless and interactive transactions, unbound and protected parameters,
missing records and tables, clauses and record ids from bindings, the index
a lookup reads, `RETURN` modes on every write, grouping and aggregates,
`SPLIT` and `OMIT`, what a record user sees where permissions deny a table, a
write, a field or a function, arithmetic, `INFO … STRUCTURE`, blocks,
full-text and nearest-neighbour search, and record links, graph steps,
`FETCH` and recursive paths. It names the mode and engine version it ran on,
and where SurrealDB 3 and 4 differ it asserts each.

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
