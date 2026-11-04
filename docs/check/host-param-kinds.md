# Host parameter kinds (`K9301`): a design note

Not implemented. `surrealkit check` does not emit `K9301`; the code is reserved so that the
lint, when it lands, does not renumber anything. This note says what it would compare and what
it needs.

## What it would compare

A call such as

```ts
db.query("SELECT * FROM person WHERE age > $age", { age: "18" });
```

passes a value for each parameter of an untyped query. The lint would compare the TypeScript type
of the value (`string`) with the kind the engine infers for the parameter (`$age` is a `number`,
because it is compared with `person.age`), and report a mismatch at the argument. The inferred
kind is `TypesDocument.queries[].params[].kind` in the document the 4.1 checker returns. It is
always a hint, never a declaration: the engine reads it off how the query uses the parameter, so
the finding is a warning, not an error.

## Why it lands with 4.1

The inference is the engine's. The 4.0 build applies the schema and checks nothing about a
query, so there is no kind to compare against, and re-deriving one in SurrealKit would be a
second, weaker type checker that disagrees with the first. The lint arrives with the backend
that calls the engine's own checker (`Surreal::check`, SurrealDB 4.1).

## Why it is narrow

`generate` emits a typed function per query, and `tsc` already enforces the parameter types at
every call of one. A mismatch the lint could find is therefore only at a call site that bypasses
those functions: `db.query(text, { … })` with a string literal or an obviously typed value. It
adds nothing where the generated functions are used, so it would be off for a project that uses
them everywhere, and cheap to leave out.

## What it needs from the extractor

[`host_queries`](../../crates/surrealkit/src/host_queries/mod.rs) records the query text and its
substitutions. For this lint it would also record, for each query call, the second argument when
it is an object literal:

- the keys, as written (`age`, or a shorthand property `{ age }`), and the byte range of each;
- the node kind of each value (`string`, `number`, `true`/`false`, `template_string`,
  `identifier`, `object`, and so on), so a literal can be typed without running `tsc`;
- nothing for a spread, a computed key or an argument that is not an object literal: those call
  sites are skipped, not guessed at.

A value whose type is only known to `tsc` (an identifier, a call) is skipped as well. Only the
literal and obviously typed values are compared, which is also what keeps the lint quiet.

## The code it would use

`K9301`, in SurrealKit's `9xxx` family, a warning by default. As `9xxx` is the one wildcard over
the family, `K9301 = "allow"` tunes it on its own, and a suppression is
`// surreal: allow(K9301) reason="…"` on the call, as for `K9201`.
