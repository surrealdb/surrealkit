# Schema document v2

Status: **draft contract** for the Effect-native typegen feature. The document is opt-in:
`schema.json` stays at version 1 unless a project sets `[typegen] schema_version = 2` (or
uses the new `surrealkit compile` command). Version 1 output is unchanged by this contract;
it changes only where the v1 parser was wrong, as listed in `MIGRATING.md` entry 14.

Schema document v2 is the interchange format between SurrealKit, which understands
SurrealQL, and code generators, which do not. A generator reads the document and
never connects to a database. This page is the normative description; the machine
check is [`crates/surrealkit/schema/schema-document-v2.schema.json`](../../crates/surrealkit/schema/schema-document-v2.schema.json)
(JSON Schema draft 2020-12), exported by the crate as
`surrealkit::typegen::SCHEMA_DOCUMENT_V2_JSON_SCHEMA`. Prose wins where the two disagree,
and a disagreement is a bug in the JSON Schema. SurrealKit's test suite validates the
example document, every document it generates from the corpus snapshots (through both the
STRUCTURE adapters and the plain-`INFO` fallback) and a compiled corpus against it.

Generators consume the document as [external emitters](external-emitters.md).

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## Contents

1. [Where the facts come from](#1-where-the-facts-come-from)
2. [Document shape](#2-document-shape)
3. [Provenance](#3-provenance)
4. [Kinds](#4-kinds)
5. [Tables](#5-tables)
6. [Fields](#6-fields)
7. [Field-presence matrix](#7-field-presence-matrix)
8. [Assertion recognition](#8-assertion-recognition)
9. [Indexes and events](#9-indexes-and-events)
10. [Functions](#10-functions)
11. [Other definitions](#11-other-definitions)
12. [Ordering](#12-ordering)
13. [semanticDigest](#13-semanticdigest)
14. [Diagnostics](#14-diagnostics)
15. [Version adapters: 3.2 and 3.3](#15-version-adapters-32-and-33)
16. [Engine hazards](#16-engine-hazards)
17. [Golden corpus](#17-golden-corpus)
18. [Design rationale](#18-design-rationale)

## 1. Where the facts come from

SurrealKit builds the document by applying schema files to a database and reading the
schema back with `INFO FOR DB STRUCTURE` and `INFO FOR TABLE <t> STRUCTURE`. It does not
parse authored `.surql` files itself, so the document describes what the engine actually
stored, after the engine's own normalisation.

- `surrealkit compile` applies the selected modules to an embedded `mem://` engine
  (the `kv-mem` engine SurrealKit already ships) and introspects that. No network, no
  `SURREALDB_*` connection settings.
- `surrealkit typegen` against a live database introspects that database.

`compile` MUST apply statements the way `sync` deploys them, not as raw SurrealQL:
SurrealKit treats a module as desired state and applies every non-sequence `DEFINE` as
`OVERWRITE` (sequences as `IF NOT EXISTS`; see `prepare_schema_sql`). The two readings
differ only when the selected modules define the same entity more than once — for
example `DEFINE FIELD OVERWRITE f …` followed by `DEFINE FIELD IF NOT EXISTS f …`, which
raw SurrealQL ignores and SurrealKit applies. Such a module is ambiguous and gets warning
`definition-duplicate`; the document then describes what `sync` would deploy, so a
compile and an introspection of a synced database agree.

Measured against the golden corpus (section 17): applying the golden corpus to SurrealKit's
embedded 3.3.0 engine and to a `surrealdb/surrealdb:v3.3.0` server produces
`INFO … STRUCTURE` and plain `INFO` output that is identical value for value, internal
table ids included. Repeated runs against the same engine version are identical too.
A hermetic compile and a live introspection on the same engine version therefore yield
the same semantic content.

`INFO … STRUCTURE` is documented by SurrealDB as internal and subject to change. It is
read only through a version-qualified adapter (section 15) that maps each supported
engine minor onto this contract. An engine minor without an adapter MUST fail with the
error diagnostic `structure-unsupported-engine`; a STRUCTURE key the adapter does not
know MUST produce the error diagnostic `structure-unknown-key` rather than being
dropped. Supported today: SurrealDB **3.2.x** and **3.3.x**.

Plain `INFO` (the `DEFINE …` strings) MAY be captured as evidence in `define`
properties. It is never the source of a semantic value when STRUCTURE provides one.

For an engine minor without an adapter, SurrealKit still produces a document, read from
plain `INFO` by a fallback reader that splits each engine-rendered `DEFINE` into its
clauses, with `provenance.adapter` set to `info` and the `structure-unsupported-engine`
error recorded: a strict consumer refuses it, a lenient one gets the best available
description. The fallback reader and the STRUCTURE adapters feed the same assembly, and
SurrealKit's test suite holds them to the same `semanticDigest` on the corpus snapshots of
every supported minor (`info_and_structure_agree_on_3_2_4`, `…_3_3_0`).

## 2. Document shape

```jsonc
{
  "version": 2,
  "semanticDigest": "sha256:…",   // section 13
  "provenance": { … },             // section 3; excluded from the digest
  "tables": [ … ],                 // section 5
  "functions": [ … ],              // section 10
  "params": [ … ], "accesses": [ … ], "analyzers": [ … ], "sequences": [ … ],
  "buckets": [ … ], "apis": [ … ], "modules": [ … ], "configs": [ … ], "users": [ … ],
  "diagnostics": [ … ]             // section 14; excluded from the digest
}
```

Rules that apply everywhere:

- Every top-level array is present, even when empty.
- Properties marked required in the JSON Schema are always present. Optional properties
  are omitted when they do not apply; they are never `null`.
- There is no generation timestamp and no physical namespace or database in the
  semantic content. Identical inputs, configuration and tool versions MUST produce
  byte-identical documents on every platform.
- No object has user-controlled keys. Every collection is an array with a defined order
  (section 12), so canonical JSON only ever sorts fixed ASCII property names.
- Numbers are integers only (lengths, counts, dimensions). Values that are numeric in
  SurrealQL but may exceed the IEEE-754 safe range or are not integers (sequence
  positions, literal values, assertion bounds, durations) are strings.
- Secrets never appear: no access keys, JWT issuer keys, user password hashes or SCRAM
  material, even redacted (section 11).

## 3. Provenance

`provenance` records where a document came from. It is excluded from `semanticDigest`.

| Property | Meaning |
| --- | --- |
| `generator` | `{ name: "surrealkit", version, commit? }` |
| `engine` | `{ version: "3.3.0", build?, mode: "embedded" \| "remote" }`. `version` is the `MAJOR.MINOR.PATCH` part; `build` the full string the engine reports (e.g. `surrealdb-3.2.4+20260803.93ab219`). |
| `adapter` | `structure-3.2` or `structure-3.3` (section 15), or `info` for the plain-`INFO` fallback (section 1) |
| `source` | Compile: `{ kind: "compile", modules: [...], files: [{ path, sha256 }] }`. Introspection: `{ kind: "introspect", namespace, database }`. |
| `capabilities` | `{ experimental: [...], network: false }` — the capabilities the compile engine ran with |

`files[].path` is relative to the project root, uses `/` separators, and is sorted.
`files[].sha256` is the lowercase hex SHA-256 of the file's bytes after removing a
leading UTF-8 byte-order mark and converting CRLF to LF, so a Windows checkout with
`core.autocrlf` produces the same document as Linux.

## 4. Kinds

A kind is a JSON object tagged on `kind`. The adapter parses the kind strings the engine
returns (`"none | array<record<team>>"`) into this tree.

| SurrealQL | v2 kind |
| --- | --- |
| `any`, `null`, `bool`, `int`, `float`, `decimal`, `number`, `string`, `datetime`, `duration`, `uuid`, `bytes`, `regex`, `range`, `function`, `object` | `{ "kind": "<name>" }` |
| `none` (alone) | `{ "kind": "none" }` |
| `record`, `record<a \| b>` | `{ "kind": "record", "tables": ["a", "b"] }` — empty = any table |
| `table`, `table<a \| b>` | `{ "kind": "table", "tables": [...] }` |
| `file`, `file<avatars \| docs>` | `{ "kind": "file", "buckets": [...] }` |
| `geometry`, `geometry<point \| polygon>` | `{ "kind": "geometry", "types": [...] }` — empty = any of the 7 kinds |
| `array`, `array<T>`, `array<T, N>` | `{ "kind": "array", "item": T, "length"?: N }` — bare `array` has item `any` |
| `set`, `set<T>`, `set<T, N>` | `{ "kind": "set", "item": T, "length"?: N }` |
| `[A, B, C]` | `{ "kind": "tuple", "items": [A, B, C] }` |
| `{ a: int, b: option<string> }` | `{ "kind": "objectLiteral", "fields": [{ "name": "a", "kind": …, "optional": false }, …] }` |
| `'draft'`, `-5`, `1.5f`, `10dec`, `true`, `1d` | `{ "kind": "literal", "type": "string"\|"int"\|"float"\|"decimal"\|"bool"\|"duration", "value": … }` |
| `none \| T` in a nested position | `{ "kind": "option", "item": T }` |
| `A \| B` | `{ "kind": "union", "variants": [A, B] }` |
| anything unparseable | `{ "kind": "unknown", "source": "<text>" }` plus error `kind-unrecognized` |

Normalisation rules (each MUST be applied):

1. **Exact lengths.** `array<T, N>` and `set<T, N>` mean *exactly* N elements on 3.2.4
   and 3.3.0; fewer and more are both rejected (evidence: `semantics/collections.surql`).
   The property is therefore `length`, not `max`.
2. **NONE becomes optionality.** The engine stores `option<T>` as `none | T`. At the top of
   a field kind, of a literal-object field and of a function argument, a `none` variant is
   removed and recorded as `optional: true`. Everywhere else (tuple items, array and set
   items) the result is `{ kind: "option", item }`. A union never contains `none`,
   `option` or a nested `union`. `none` alone stays `{ kind: "none" }` with
   `optional: true`.
3. **Unions are canonical.** Variants are flattened, deduplicated and sorted by their
   canonical JSON (section 13). A union has at least two variants; one survivor replaces
   the union. Authored order carries no meaning and does not survive.
4. **Name lists are sorted and deduplicated**: `record.tables`, `table.tables`,
   `file.buckets`, `geometry.types`. `literal-object` fields are sorted by name (the engine
   already sorts them: `{ n: int, region: string }`).
5. **Literals.** `value` is a JSON boolean for `bool`, otherwise a string: strings
   unquoted and unescaped; numbers and durations in their SurrealQL spelling without the
   type suffix (`1.5f` → `"1.5"` with `type: "float"`, `10dec` → `"10"` with
   `type: "decimal"`, `1d` → `"1d"` with `type: "duration"`). The engine renders double-
   quoted string literals with single quotes and inserts stray double spaces before
   float and decimal literals (`-5 | 0 |  1.5f |  10dec`); the parser MUST tolerate both.
6. **Element fields.** For every `array` or `set` field the engine synthesises an element
   field `<path>.*` whose kind is the element kind (`k_array_or_set.*: int | string`).
   These appear in `fields` like any other field (section 6).

## 5. Tables

```jsonc
{
  "name": "member_of",
  "type": { "kind": "relation", "in": ["person"], "out": ["org", "team"], "enforced": true, "lightweight": false },
  "schemafull": true,
  "drop": false,
  "view": { "select": "SELECT count() AS total, team FROM person GROUP BY team" }, // views only
  "changefeed": { "expiry": "1d", "includeOriginal": true },                       // when declared
  "permissions": { "select": true, "create": "$auth.id = id", "update": "$auth.id = id", "delete": false },
  "idKey": { "kind": "objectLiteral", "fields": [ … ] },                           // when `id` is declared
  "comment": "…",
  "fields": [ … ], "indexes": [ … ], "events": [ … ],
  "define": "DEFINE TABLE …"                                                       // evidence, optional
}
```

- Tables whose name starts with `__` are SurrealKit's own metadata (`__entity`, `__rollout`,
  `__seed`, which `setup.surql` creates in every synced database) and are not part of the
  document, the rule the TypeScript interface emitter has always applied. Without it, a
  compile and an introspection of a database synced from the same files would disagree.
- `type.kind` is `normal`, `any` or `relation`. A schemaless table without a declared
  `TYPE` is reported by the engine as `any`; a schemafull one as `normal`; views as `any`.
- Relations: `in`/`out` sorted; empty = unconstrained (`DEFINE TABLE likes TYPE RELATION`).
  `lightweight` is `false` on 3.2 (the keyword does not exist there). 3.3.0 reports every
  lightweight relation as `enforced: true`.
- `permissions` uses `true` (FULL), `false` (NONE) or a `WHERE` expression string. A
  table defined without `PERMISSIONS` has every permission `false`; a field without
  `PERMISSIONS` has every permission `true`. Table permissions include `delete`; field
  permissions do not.
- `view.select` is the engine's `view` text without its leading `AS `. A view's row shape
  comes from the SELECT, so views carry no write contracts and their select shape is not
  derived (info diagnostic `table-view-shape`).
- `changefeed.expiry` is the engine-normalised duration (`7d` is returned as `1w`).
- The engine's internal table `id` counter is never copied.
- `idKey` is the kind of `DEFINE FIELD id`. The `id` field itself does not appear in
  `fields`. If it carries any clause besides `TYPE` and default permissions, the clauses
  are ignored with warning `field-id-clauses-ignored`. Without `TYPE`, `idKey` is `any`
  (info `field-kind-undeclared`). `record<…>` is not a valid key kind;
  the engine rejects it (`rejected/id-record-kind.surql`).

## 6. Fields

```jsonc
{
  "path": ["phones", "*", "kind"],
  "kind": { … },                                          // absent = no TYPE clause
  "optional": false, "flexible": false, "readonly": false, "inline": false,
  "default": { "mode": "create" | "always", "expr": "'member'" },
  "value": { "expr": "string::lowercase($value)", "readsInput": true },
  "computed": { "expr": "string::concat(name, '!')" },
  "assert": { "expr": "…", "recognized": [ … ] },
  "reference": { "onDelete": "reject" | "ignore" | "cascade" | "unset" } | { "onDelete": "then", "expr": "…" },
  "permissions": { "select": true, "create": true, "update": true },
  "comment": "@acme:policy {…}",
  "define": "DEFINE FIELD …"                              // evidence, optional
}
```

- **Paths.** The engine's field name is split on `.` outside backtick quoting, quotes are
  removed, and `[*]` / `*` become the segment `"*"`. `` `odd.name` `` is the single segment
  `"odd.name"`; `phones[*].number` is `["phones", "*", "number"]`.
- **Implicit parents.** The engine accepts `DEFINE FIELD prefs.locale` without
  `DEFINE FIELD prefs`. The document does not invent the parent (info diagnostic
  `field-parent-implicit`), but the engine materialises it: every write stores an empty
  object for an undeclared parent, and NONE resets it to `{}` (evidence: the `ip` table in
  `semantics/field_presence.surql`, identical on 3.2.4 and 3.3.0). Consumers MUST treat a
  missing proper prefix as an object that behaves like `TYPE object DEFAULT ALWAYS {}`:
  required in select; in create and content required when any child is required and
  optional otherwise; in merge optional (NONE rejected) when any child is required and
  resettable otherwise.
- **Parents without TYPE.** A field declared without `TYPE` that has declared children
  (`DEFINE FIELD obj ON t; DEFINE FIELD obj.a ON t TYPE int`) follows the implicit-parent
  rules above (evidence: the `up` table in `semantics/field_presence.surql`): when a child
  is required the field must be an object closed over its children and is required in
  select, create and content and optional (NONE rejected) in merge. When no child is
  required it may hold any value, an object being closed over its children.
- **No TYPE.** `kind` is absent and the field holds any value (info
  `field-kind-undeclared`). A `COMPUTED` field without `TYPE` gets warning
  `field-computed-kind-undeclared`.
- **`default`** comes from `default` + `default_always`. `mode: "create"` applies only when
  a record is created; `mode: "always"` also re-applies whenever the field becomes NONE.
- **`value.readsInput`** is `true` when the expression references `$value` or `$input`
  outside string literals. It is derived (excluded from the digest).
- **`reference.onDelete`**: `REFERENCE` with no `ON DELETE` is reported as `ignore`. An
  action the contract does not know is the error `field-reference-unrecognized`.
  `ON DELETE THEN <expr>` keeps the engine's expression text after `THEN `, verbatim,
  including its parentheses.
- **`flexible`** keeps undeclared nested keys (evidence below). `inline` is the 3.3
  `INLINE` relation-field flag; always `false` on 3.2.
- **`comment`** is carried verbatim, including escaped quotes. SurrealKit never interprets
  it; namespaced payloads such as `@acme:policy {…}` belong to their owners.
- Relation tables report `in` and `out` as ordinary fields with `record<…>` kinds.

## 7. Field-presence matrix

This is the semantic core of the contract. Generators MUST derive one schema per
operation from it. Every row is backed by statements in
[`semantics/field_presence.surql`](../../crates/surrealkit/tests/fixtures/typegen_corpus/semantics/field_presence.surql),
observed identically on SurrealDB 3.2.4 and 3.3.0; the cells marked † are where the
obvious reading is wrong (section 18).

Presence vocabulary:

- **required** — the key must be present with a value of the field's kind.
- **optional key** (select) — absent means the stored value is NONE.
- **optional** — the key may be omitted. Create: the engine supplies the value. Merge:
  the stored value is unchanged. Content: see the cell.
- **removable** — omission leaves the value unchanged; `undefined` (NONE) removes it.
- **resettable** — omission leaves the value unchanged; `undefined` (NONE) re-applies the
  default.
- **excluded** — the key must not be sent; generated types make it a compile-time error.

| Field declaration | Select | Create | Merge | Content |
| --- | --- | --- | --- | --- |
| `TYPE T` | required | required | optional (NONE rejected) | required |
| `TYPE option<T>` | optional key | optional | removable | optional (omission removes) |
| `TYPE T \| null` | required, nullable | required, nullable (NONE ≠ null) | optional, nullable | required, nullable |
| `TYPE T DEFAULT e` | required | optional | optional (NONE rejected; default **not** re-applied)† | **required** (default not re-applied)† |
| `TYPE option<T> DEFAULT e` | optional key | optional (explicit NONE also gets the default) | removable | optional (omission removes) |
| `TYPE T DEFAULT ALWAYS e` | required | optional | resettable | optional (default re-applied) |
| `TYPE option<T> DEFAULT ALWAYS e` | optional key | optional (explicit NONE also gets the default) | resettable (NONE re-applies the default)† | optional (default re-applied) |
| `READONLY` | as declared | as declared (a value may be supplied) | **excluded** | **excluded** (omission keeps the stored value; a different value is rejected) |
| `COMPUTED e` | as declared (`any` without TYPE) | **excluded** (a supplied value is silently ignored) | **excluded** | **excluded** |
| `VALUE e` reading `$value` | as declared | as declared (required unless optional/default) | optional (omission keeps the stored value) | as declared (required unless optional/default always) |
| `VALUE e` ignoring `$value` | as declared | optional (a supplied value is silently replaced) | optional | optional (recomputed) |
| `id` | the table's id schema | via the operation's id argument | excluded | excluded (a different id is rejected) |
| relation `in` / `out` | endpoint id unions | via `relate(from, to)` | excluded (silently ignored) | excluded (omission keeps them) |
| `FLEXIBLE` object | open record over the declared children | same | same | same |

Derivation, in order (first match wins within each operation):

```text
writeExcluded(f) := f.computed || f.path == ["id"] || (relation && f.path in [["in"],["out"]])
                    || table.view || (relation && type.lightweight)

Create(f):  writeExcluded → excluded
            f.optional || f.default || (f.value && !f.value.readsInput) → optional
            otherwise → required
Merge(f):   writeExcluded || f.readonly → excluded
            f.default.mode == "always" → resettable
            f.optional → removable
            otherwise → optional
Content(f): writeExcluded || f.readonly → excluded
            f.optional → optional (omission removes)
            f.default.mode == "always" || (f.value && !f.value.readsInput) → optional
            otherwise → required
Select(f):  f.optional → optional key, otherwise required
```

Further rules:

- **Closed rows.** A schemafull table rejects undeclared top-level keys and undeclared keys
  inside a non-FLEXIBLE object (`Found field 'obj.extra', but no such field exists`), so
  generated write schemas for schemafull tables are exact. Schemaless tables and FLEXIBLE
  objects are open. A non-FLEXIBLE object without declared children therefore holds only
  `{}`, and FLEXIBLE does not carry over to a declared child object (each field's own
  `flexible` decides).
- **Nesting.** Rules apply to child fields relative to their parent. `MERGE` merges nested
  objects, so Merge applies recursively; Create and Content apply to the children of a
  present parent. Element fields (`*`) describe whole elements; elements are never merged.
- **`VALUE` with a fallback.** A `VALUE` that reads `$value` but supplies its own fallback
  (`string::lowercase($value ?? 'X')` accepts omission) is reported with warning
  `field-value-input-conditional` and gets the conservative contract (required); a
  project can override it explicitly. The detector flags `??`, `?:` and `IF` in an
  expression that reads the input.
- **Merge is never `Partial<Create>`.** Readonly, computed, id and endpoint fields are
  excluded, and NONE is only accepted where the matrix says removable or resettable.
- **Select schemas describe full stored rows** as a privileged reader sees them.
  Projections, `FETCH`, graph traversals and permission-filtered reads need their own
  result schemas.

## 8. Assertion recognition

`assert.recognized` is present only when the whole expression is understood. Recognition
is all or nothing: one unrecognised conjunct leaves `recognized` absent and emits info
`assert-unrecognized`. It is derived and excluded from the digest.

Grammar (over the engine-rendered expression):

```text
assert   := [ "$value = NONE OR " ] conj        // NONE guard on optional fields; dropped
conj     := atom ( " AND " atom )*
atom     := "$value INSIDE [" literal ("," literal)* "]"      → literals
          | "$value IN [" … "]"                                  → literals (3.x renders IN as INSIDE)
          | "$value" op number | number op "$value"             → range
          | "string::len($value)" op int | "array::len($value)" op int   → length
          | "string::is_email($value)"                           → email
op       := ">" | ">=" | "<" | "<="
```

Refinements:

```jsonc
{ "type": "literals", "values": [ { "kind": "literal", "type": "string", "value": "admin" }, … ] }
{ "type": "range", "valueKind": "int" | "float" | "decimal" | "duration",
  "min"?: { "value": "0", "inclusive": true }, "max"?: { "value": "150", "inclusive": true } }
{ "type": "length", "target": "string" | "array", "min"?: 1, "max"?: 80 }   // inclusive
{ "type": "email" }
```

- Several range or length atoms on the same target intersect. `valueKind` comes from the
  literal spelling: `0f` float, `0dec` decimal, `1m` duration, plain integer int. Mixed
  kinds are unrecognised.
- `length` bounds are inclusive integers (`< 4` becomes `max: 3`). `string::len` counts
  Unicode scalar values (`string::len('😀') = 1`, `string::len('e\u{301}') = 2`), so a
  generator targeting UTF-16 strings MUST count code points, not code units.
- `literals` values are sorted by canonical JSON.
- Not recognised, by design: regex matches, comparisons with other fields, function calls
  other than the three above, `OR` beyond the leading NONE guard, `CONTAINS`, equality.

## 9. Indexes and events

Index:

```jsonc
{
  "name": "hnsw_embedding",
  "columns": [["embedding"]],
  "type": "standard" | "unique" | "fulltext" | "hnsw" | "diskann" | "count",
  "clause": "HNSW DIMENSION 4 DIST COSINE TYPE F32 EFC 150 M 12 M0 24 LM 0.40242960438184466f",
  "fulltext"?: { "analyzer": "simple", "highlights": true },
  "vector"?: { "dimension": 4, "distance": "COSINE", "elementType": "F32" },
  "where"?: "status = 'open'",   // COUNT … WHERE
  "comment"?: "…"
}
```

`clause` is the engine's rendering verbatim, including the tuning defaults it fills in
(`M0`, `LM`, `DEGREE`, `L_BUILD`, `ALPHA`). `CONCURRENTLY` is a build-time option and is
not reported. A unique index is not a client-side validator.

Event:

```jsonc
{ "name": "on_status", "when": "$before.status != $after.status",
  "then": ["{ CREATE audit_log SET … }"], "async": true, "retry": 3, "maxDepth": 2, "comment": "…" }
```

An event without `WHEN` reports `when: "true"`. `then` keeps the declared order. Events
are descriptive metadata, not typed application contracts.

## 10. Functions

```jsonc
{
  "name": "fn::greet",
  "args": [
    { "name": "name", "kind": { "kind": "string" }, "optional": false, "omittable": false },
    { "name": "greeting", "kind": { "kind": "string" }, "optional": true, "omittable": true }
  ],
  "returns": { "kind": "string" },       // absent when undeclared
  "block": "{ RETURN … }",
  "permissions": true,
  "comment": "…"
}
```

- STRUCTURE reports names without the `fn::` prefix; the adapter restores it.
- `optional` means the argument kind admits NONE. `omittable` is true only for the
  maximal trailing run of optional arguments: those are the only ones a caller may leave
  out. The engine accepts an optional argument before a required one
  (`fn::optional_first($a: option<int>, $b: int)`); such an argument is optional but not
  omittable, and gets warning `function-optional-arg-not-trailing`.
- An undeclared return gets info `function-return-undeclared`; generators type the result
  as unknown. A declared return does not mean the call cannot fail.

## 11. Other definitions

| Collection | Entry | Notes |
| --- | --- | --- |
| `params` | `{ name, value, permissions, comment? }` | `name` without `$`. The engine evaluates `VALUE` **once, at definition time**, and stores the result (`math::max([1, 2, 3])` reads back as `3`). A non-deterministic expression such as `time::now()` puts a timestamp into the document and makes the digest unstable; authored params SHOULD be literals, and `surrealkit compile` reports such a param as the error `param-value-nondeterministic`. |
| `accesses` | `{ name, method, signup?, signin?, authenticate?, refresh?, verify?, subject?, durations, comment? }` | `method` is `record`, `jwt` or `bearer`. `signup`, `signin` and `refresh` (always present, `false` without `WITH REFRESH`) belong to `record`. `authenticate` is the engine-rendered `AUTHENTICATE` expression, for any access method that declares one; omission means no expression was captured. `verify` is `{ alg }` or `{ url }`, for `record` and `jwt` only: a bearer access signs with an engine-internal key that plain `INFO` does not show either. `subject` (`record`/`user`) for bearer. Keys and issuer material are dropped, never copied as `[REDACTED]`. Durations are the engine's normalised strings (`30d` → `4w2d`); null durations are omitted. On SurrealDB 3.3 `CONTEXT` and a JWT `AUDIENCE` are not modelled: both readers report the warning `definition-unsupported` for each. Accesses and users defined `ON NAMESPACE` or `ON ROOT` are outside the database and not part of the document; `compile` reports each one it finds in its engine as `definition-unsupported`. |
| `analyzers` | `{ name, function?, tokenizers, filters, comment? }` | `function` gets its `fn::` prefix back. Filter and tokenizer order is kept (it is a pipeline). |
| `sequences` | `{ name, batch, start, timeout? }` | Strings, as the engine reports them. Allocation state (3.3's `resume`) is never copied. |
| `buckets` | `{ name, backend, readonly, permissions, comment? }` | Requires the experimental `files` capability. Generators MUST NOT embed `backend` in browser output. |
| `apis` | `{ path, actions: [{ methods, middleware, permissions, action }], middleware, permissions, comment? }` | Middleware is `[{ name, args }]`, sorted by name; if an action or the API declares more than one middleware, info `api-middleware-order` records that execution order is not preserved through STRUCTURE. |
| `modules` | `{ name, executable: { type, bucket?, key? }, permissions, comment? }` | `name` gets its `mod::` prefix back. 3.3's `unsigned` flag is not carried: modules are unsigned on every supported version. |
| `configs` | `{ type: "graphql", tables, functions }` or `{ type: "api", middleware, permissions }` | |
| `users` | `{ name, roles, durations, comment? }` | Password hashes and SCRAM material are never copied. |

`models` is always empty on the supported builds (`DEFINE MODEL` is rejected without the
ML feature). Any entry in a category the contract does not model produces warning
`definition-unsupported` with the raw `DEFINE` text as `source`.

## 12. Ordering

| Collection | Order |
| --- | --- |
| tables, functions, params, accesses, analyzers, sequences, buckets, modules, users, indexes, events | `name`, byte order |
| apis | `path`; configs: `type` |
| fields | `path`, compared segment by segment in byte order, a prefix before its extensions |
| relation `in`/`out`, `record.tables`, `table.tables`, `file.buckets`, `geometry.types`, api `methods`, user `roles`, `provenance.source.modules`, `capabilities.experimental` | sorted, deduplicated |
| union variants, `literals` values, api `actions`, `diagnostics` | canonical JSON (section 13), byte order |
| `provenance.source.files` | `path`, byte order |
| function `args`, tuple `items`, event `then`, analyzer `tokenizers`/`filters` | declared order (meaningful) |
| literal-object `fields` | `name`, byte order |

"Byte order" is the order of the UTF-8 encodings.

## 13. semanticDigest

`semanticDigest = "sha256:" + hex(SHA-256(UTF-8(canonical(projection(document)))))`.

`projection` removes, and nothing else:

- the top-level `semanticDigest`, `provenance` and `diagnostics`;
- `define` on every table and field;
- `value.readsInput` and `assert.recognized` on every field.

Everything left is semantic: identical schemas compiled from different checkouts, machines
or operating systems produce the same digest, while any change to a kind, clause,
expression, permission or comment changes it. Generator improvements to recognition or
diagnostics do not move it.

An access's optional `authenticate` expression is included in this projection:
`AUTHENTICATE (RETURN $auth)` and `AUTHENTICATE (RETURN NONE)` have different
digests. This property is an additive v2 extension. Readers accept older v2
documents without it and do not insert a default, preserving those documents'
bytes and digests. Such a document does not establish authentication-policy
parity; regenerate it from its authored schema or catalog before using its
digest to compare that policy. Digest comparison does not replace exact source
or access-catalog comparison for clauses the document does not carry.

Values that never enter the document at all, so they cannot disturb the digest: timestamps,
physical namespace and database names, internal table ids, sequence allocation state,
secrets, and engine-filled defaults that STRUCTURE does not report.

`canonical` is the JSON Canonicalization Scheme (RFC 8785). Because the document contains
only fixed ASCII property names, integers, strings, booleans, arrays and objects, this
reduces to: object keys sorted, no insignificant whitespace, integers in plain decimal,
strings escaped exactly as ECMAScript `JSON.stringify` does (`\"`, `\\`, `\b \f \n \r \t`,
other controls as lowercase `\u00xx`, everything else literal). A non-integer number or
`-0` in the document is a bug and MUST fail digest computation.

The digest is defined per engine minor. Expression text is the engine's rendering, and
the version adapters (section 15) only normalise the differences listed there, so two
documents from different engine minors are comparable only after a separate review.

The example document's digest
(`sha256:9bb3a0a15eb772d737b5dd170653655b97e4833995b53d272207288b37ee0f37`) is computed
by two independent implementations, SurrealKit's (Rust) and `effect-surrealkit`'s
(TypeScript), which agree; both test suites pin it.

## 14. Diagnostics

```jsonc
{ "severity": "error" | "warning" | "info", "code": "kind-unrecognized",
  "table"?: "person", "field"?: ["address", "zip"], "function"?: "fn::greet",
  "source": "<exact text the diagnostic is about>", "span"?: { "start": 0, "end": 4 },
  "reason": "human-readable explanation" }
```

`span` is a UTF-8 byte range within `source`. Strict consumers fail on `error`.
Generation fails on any `error` unless the project acknowledges that specific code, with a
reason, in its configuration. Codes are stable identifiers; reasons are not.

| Code | Severity | Raised when |
| --- | --- | --- |
| `structure-unsupported-engine` | error | The engine minor has no adapter. |
| `structure-unknown-key` | error | STRUCTURE returned a key the adapter does not map. |
| `kind-unrecognized` | error | A kind string could not be parsed; the kind becomes `unknown`. |
| `param-value-nondeterministic` | error | `surrealkit compile`: a `DEFINE PARAM` stored a different value when defined again (`time::now()`, `rand::*`), so the document would change on every run. |
| `compile-engine-mismatch` | error | `surrealkit compile`: `[typegen] engine` names an engine minor other than the embedded one, so the document describes the embedded engine's behaviour. |
| `field-reference-unrecognized` | error | A `REFERENCE` `ON DELETE` action is none of the known ones; `onDelete` is recorded as `ignore`. |
| `object-without-fields` | error | On a SCHEMAFULL table, an `object` field (also under `option`/`\| null`, or the element of an `array`/`set`) that is not FLEXIBLE and has no declared fields. SurrealDB 3.2 and 3.3 refuse every undeclared key of it, so it holds only `{}` (evidence: `semantics/objects.surql`). A FLEXIBLE array opens its elements. |
| `field-name-run-together` | warning | A top-level field name is an object field's name run straight into a lowercase word (`envelopealgorithm` beside `envelope`): perhaps a child path missing its dot. Heuristic. |
| `definition-unsupported` | warning | INFO returned an entry in a category this contract does not model (e.g. models). |
| `field-id-clauses-ignored` | warning | `DEFINE FIELD id` carries clauses other than `TYPE`. |
| `field-computed-kind-undeclared` | warning | `COMPUTED` without `TYPE`. |
| `field-value-input-conditional` | warning | `VALUE` reads `$value`/`$input` and has a fallback; conservative contract applied. |
| `function-optional-arg-not-trailing` | warning | An optional argument precedes a required one. |
| `definition-duplicate` | warning | The selected modules define the same entity more than once (section 1). |
| `field-kind-undeclared` | info | A non-computed field has no `TYPE`. |
| `field-parent-implicit` | info | A nested field's parent path is not declared. |
| `assert-unrecognized` | info | `ASSERT` is outside the recognised subset. |
| `function-return-undeclared` | info | A function declares no return kind. |
| `table-view-shape` | info | A view: its row shape is not derived. |
| `table-lightweight-relation` | info | A lightweight relation: no content schemas, `relate` only. |
| `api-middleware-order` | info | More than one middleware; order not preserved. |

New codes may be added in minor releases. A consumer MUST treat an unknown code by its
severity.

## 15. Version adapters: 3.2 and 3.3

Both adapters read the same STRUCTURE shape; differences found by applying the corpus to
3.2.4 and 3.3.0 (`introspection/3.2.4` vs `introspection/3.3.0`):

| Area | 3.2.4 | 3.3.0 | Adapter rule |
| --- | --- | --- | --- |
| Event `then` items and access `signin`/`signup`/`authenticate` that are statements | `CREATE audit_log SET …` | `(CREATE audit_log SET …)` | `structure-3.3` removes one enclosing pair of parentheses when the inner text starts with a statement keyword (`CREATE`, `UPDATE`, `UPSERT`, `DELETE`, `SELECT`, `INSERT`, `RELATE`). Canonical form is unparenthesised. Blocks `{ … }` and other expressions are untouched. |
| `LIGHTWEIGHT` relations | parse error | supported; reported `enforced: true` | `lightweight: false` on 3.2. |
| `INLINE` relation fields | parse error | `inline: true` | `inline: false` on 3.2. |
| `DEFINE MODULE` | `AS f"…"` | `FROM f"…" UNSIGNED` (the 3.2 form is a parse error) | Same entry; `unsigned` not carried. |
| Sequence `resume` | absent | allocation position | never copied |
| User `scram` | absent | `[REDACTED]` | never copied |
| Access `CONTEXT` and JWT `AUDIENCE` | parse error | `context`, `kind.jwt.audience` | not carried; `definition-unsupported` warning from both readers (`semantics/access.v3.3.surql`) |
| Array literal into a `set` field | rejected (`Expected set but found [5, 4, 5]`), so `DEFAULT []` on a set field makes every CREATE fail | coerced, deduplicated, sorted | none (data semantics; generators encode sets explicitly) |
| `option<set<T, N>>` with duplicates | clean error | **server abort** (section 16) | none |
| Internal table `id` counters | allocation order | allocation order | never copied |

Identical on both versions, and therefore not adapter concerns: field and table STRUCTURE
keys, `option<T>` rendered as `none | T`, `IN` rendered as `INSIDE`, `[*]` rendered as
`.*`, `{}` rendered as `{  }`, `0.0` as `0f`, `sequence::nextval` rendered as
`` `sequence`::nextval ``, duration normalisation, element-field synthesis, event `what`
(not `table`), field `table`, function names without `fn::`, analyzer function names
without `fn::`, module names without `mod::`, assertion parentheses dropped where
precedence allows (`$value = NONE OR ($value >= 0 AND …)` → `$value = NONE OR $value >= 0 AND …`).

## 16. Engine hazards

Found while building the corpus. They are recorded as fixtures under `rejected/` that
are never applied by the corpus.

- **3.3.0 server abort on bounded optional sets**
  (`rejected/crash-option-bounded-set.v3.3.surql`). Writing an array with duplicates to an
  `option<set<T, N>>` field, where the deduplicated length differs from N, panics the
  worker (`If can_coerce_to_kind returns true then coerce_to_kind must not error:
  InvalidLength { len: 2, into: "set<int,3>" }` at `coerce.rs:738`) and the server exits
  (status 139). The non-option field and an explicit cast fail cleanly. Codecs MUST
  encode set fields as deduplicated SDK sets of the exact length before sending them.
- **3.3.0 catalog poisoning by `array<…> | set`**
  (`rejected/poison-array-set-union.v3.3.surql`). The synthesised element field gets kind
  `int | any`, which 3.3.0 stores but cannot re-parse: every later statement in that
  database fails with `Unexpected token ANY, expected a kind name`. 3.2.4 is unaffected.
  `compile` runs in a throwaway engine, so the failure surfaces as a compile error rather
  than a broken shared database.
- **Param values are evaluated at definition time** (section 11).
- **`DEFINE FIELD … FLEXIBLE` needs its table to exist.** Other `DEFINE FIELD` statements
  create a missing table implicitly; a FLEXIBLE one fails with `The table 'person' does not
  exist` on 3.2.4 and 3.3.0. Module order matters for FLEXIBLE fields, so the corpus keeps
  them beside their `DEFINE TABLE`.

## 17. Golden corpus

`crates/surrealkit/tests/fixtures/typegen_corpus/`:

| Path | Contents |
| --- | --- |
| `schema/01_tables.surql` … `10_api_config.surql` | Every value kind and every `DEFINE` family typegen needs, applied in file order. Each file is accepted on its own by 3.2.4 and 3.3.0 with default capabilities. |
| `schema-gated/20_experimental_files.surql` | Buckets and bucket-bound file fields; needs `--allow-experimental=files`. |
| `schema-gated/30_relations.v3.3.surql` | `LIGHTWEIGHT` and `INLINE`; 3.3 only. |
| `schema-gated/31_module.v3.2.surql`, `31_module.v3.3.surql` | A Surrealism module in each version's spelling; needs `--allow-experimental=surrealism`. |
| `corpus.json` | The module list with each entry's engine and capability requirements. |
| `rejected/` | Statements the engines reject, and the two hazards above. |
| `semantics/` | Statement-by-statement behaviour evidence with `-- expect:` annotations (`expect[3.2]:` / `expect[3.3]:` for version-specific outcomes). |
| `introspection/<version>/` | Raw `INFO FOR DB`, `INFO FOR DB STRUCTURE`, and per-table `INFO FOR TABLE` / `STRUCTURE` output, plus `apply-report.json` (all statements accepted with experimental capabilities) and `default-capabilities.apply-report.json` (which statements need them). |
| `example.v2.json` | A hand-written v2 document for a subset (tables `doc`, `id_object`, `member_of`, `person`, `team` and one or more of every other collection), validated against the JSON Schema and cross-checked value by value against `introspection/3.3.0` and `introspection/3.2.4`. |

Apply order: 3.2.x — `schema/01`…`10`, `schema-gated/20`, `31_module.v3.2`; 3.3.x —
`schema/01`…`10`, `schema-gated/20`, `30`, `31_module.v3.3`. Each statement is sent on
its own to a fresh `corpus`/`corpus` database as root.

Gated modules live outside `schema/` on purpose: SurrealKit's own suite
(`schema_state::tests::splitting_and_preparing_agree_with_surrealdb`) applies every file
under a `schema/` directory in the repository, alone, to a default-capability engine on
every CI lane (embedded 3.3.0 and servers 3.2.4 and 3.3.0), and checks that splitting and
`OVERWRITE` preparation agree with SurrealDB. Every `schema/` file therefore has to be
self-contained, portable and free of duplicate definitions; the corpus satisfies that
suite unchanged. The repository-wide splitter-parity test
(`entity_keys_and_hashes_match_the_old_pipeline`) also compares the current splitter with
the retired one on every `.surql` file, which is why the corpus has no identifier
containing whitespace: the retired splitter, kept only as that test's oracle, cuts
`` `with space` `` at the space.

Snapshots were captured from `surreal` 3.2.4 (`surrealdb-3.2.4+20260803.93ab219`) and the
`surrealdb/surrealdb:v3.3.0` image, both `memory` engines started with
`--allow-experimental=files,surrealism`, and repeated with default capabilities for the
`default-capabilities` reports. The embedded check applied the same files through
SurrealKit's own `surrealdb` 3.3.0 dependency with `kv-mem`.

Coverage of `INFO … STRUCTURE` against the corpus: every field property the contract uses
(`kind`, `flexible`, `readonly`, `inline`, `default`, `default_always`, `value`,
`computed`, `assert`, `reference`, `permissions`, `comment`) appears in the 3.3.0
snapshot, as do the table properties `schemafull`, `kind` (normal, any, relation with and
without endpoints, lightweight), `view`, `changefeed`, `drop`, `permissions` (all three
forms), `comment`, and every database category except `models`.

## 18. Design rationale

Where an obvious modelling choice and the engine's behaviour disagreed, the engine
decided. The choices below are deliberate:

| Choice | Instead of | Why |
| --- | --- | --- |
| A plain `DEFAULT expr` field is **required** in Content | optional | A plain DEFAULT is not re-applied on `UPDATE … CONTENT` or on MERGE to NONE (`Expected string but found NONE`). Only `DEFAULT ALWAYS` is. |
| A plain `DEFAULT expr` field is optional in Merge, with NONE rejected | optional, NONE allowed | Same evidence. |
| An `option<T> DEFAULT ALWAYS` field is resettable in Merge | removable, like any `option<T>` | NONE re-applies the default instead of removing the value (the `oa` table in `semantics/field_presence.surql`). |
| Array and set bounds are both `length`, exact | array `len` vs set `max` | `array<int, 3>` and `set<int, 3>` reject 2 and 4 elements on both versions. |
| Table `type: { kind: … }` | table `kind: normal \| any \| relation{…}` | Avoids two different meanings of `kind` in one document. |
| `id` lifted to `table.idKey` | `id` kept among the fields | One place for the key kind; the field carries nothing else. |
| `value` also carries the derived `readsInput` | `default`, `value`, `computed` as bare `{ expr }` | The field-presence matrix needs it; excluded from the digest. |
| Diagnostic `source` is the diagnosed text, `span` a byte range in it | an unspecified `source` | Consumers can point at the exact text. |
| Sequence values, literal values and bounds as strings | JSON numbers | Exactness beyond 2^53 and for floats/decimals; keeps canonical JSON integer-only. |
| `users` included, without credentials | users left out | They are authored `DEFINE USER` statements. |
| `models` not modelled | a `models` collection | Rejected by supported engine builds; reported as `definition-unsupported` if seen. |
| Function args carry `optional` and `omittable` | args as `{ name, kind }` | The engine does not enforce the trailing-optional rule. |
