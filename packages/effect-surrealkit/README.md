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
| `PersonVisible` | The row as a session reads it when field permissions may hide fields: every field (at any depth) whose `select` permission is not FULL is optional. `Person` itself when no permission can hide a field. |
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
carries `toCode` metadata for this (`test/to-code.test.ts` prints a struct of
all of them). It works mechanically but was not adopted because:

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

The property suites round-trip every codec through the SDK's CBOR codec in
process (`Arbitrary.schema` + `Arbitrary.checkEffect`). Golden output lives in
`test/golden/` (regenerate with `UPDATE_GOLDEN=1 bun run test`): the contract's
`example.v2.json`, the same with the `bigint`/`effect-datetime`/`patch`
options, the whole corpus, the integer-keyed corpus tables in the `bigint` profile (the
whole corpus in that profile is checked line by line against the `corpus`
golden instead of being pinned twice), the field-presence evidence tables and
the `members` fixture (`test/fixtures/members`: field permissions and unique
indexes, compiled with `surrealkit compile` in that directory). The corpus
inputs are real `surrealkit compile` output, pinned byte for byte by
SurrealKit's own test suite under `typegen_corpus/compiled/`, and every input
document is validated against the published JSON Schema. The golden
comparison ignores the emitter version in `manifest.ts` (and SurrealKit's pin
ignores the SurrealKit and engine versions), so a release does not mean
regenerating them.
`test/types` compiles the golden modules with positive and `@ts-expect-error`
fixtures under `strict` and `exactOptionalPropertyTypes`.

The engine, field-presence, analyzer and conformance suites need a real engine. A
plain `bun run test` runs them on the in-process, in-memory engine of
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
version the binary reports) and `remote` for `SURREAL_URL`. `embedded` and
`binary-3.3` are committed; a lane without a list is reported as unpinned and
not compared. After a change that is meant to skip or un-skip a test, rewrite
the list from a passing run and commit it with the change:

```sh
bun run test:skips                                                   # embedded
SURREAL_BIN=/usr/local/bin/surreal bun run test:skips                # binary-3.3 with a 3.3 binary
UPDATE_SKIPS=1 SURREAL_BIN=/usr/local/bin/surreal bun run test:skips # rewrites test/skips/binary-3.3.txt
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
