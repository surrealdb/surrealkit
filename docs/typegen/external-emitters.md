# External emitters

Status: **draft protocol 1** for the Effect-native typegen feature.

An external emitter is a code generator SurrealKit runs over
[schema document v2](schema-document-v2.md). The emitter reads the document and returns
the files it generates; SurrealKit writes them. Emitters never connect to a database and
never touch the filesystem themselves, so every output SurrealKit produces — its own JSON
and TypeScript, and every emitter's files — gets the same guarantees: atomic writes,
unchanged files left alone, `--check` without writing, and strict failure.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## Configuration

```toml
[typegen.emit.effect]
command = "effect-surrealkit"          # split on whitespace
out = "src/generated/surreal"

[typegen.emit.docs]
command = ["bun", "x", "my-emitter", "--title", "Data model"]   # exact argv
out = "docs/generated"
```

- `<name>` uses only letters, digits, `-` and `_`.
- `command` is a string split on whitespace, or an array giving the program and each
  argument exactly. It is run from the current directory, like `[typegen] format`, and
  started directly rather than through a shell: on Windows an npm shim has to be named
  with its extension (`effect-surrealkit.cmd`), or run through `node` with an argv.
- `out` is a directory, relative to the current directory. **SurrealKit owns it**: no two
  emitters may share or nest their `out` directories, and `[typegen] json` and `typescript`
  may not be inside one (the configuration is refused).

Emitters run in `surrealkit typegen`, `surrealkit typegen --from <schema.json>` (v2
documents only), `surrealkit compile`, and `surrealkit sync` (including every
`sync --watch` iteration) after a sync that changed the schema, or when an emitter has
never published. They always receive schema document v2, whatever `[typegen]
schema_version` says the JSON output is.

## Invocation

SurrealKit starts `command` with:

- **stdin**: the schema document v2, as UTF-8 JSON — exactly the bytes SurrealKit writes as
  the pretty-printed JSON output, ending in a newline. stdin is closed after the document.
- **environment**: the parent's environment plus
  - `SURREALKIT_EMIT_PROTOCOL` — `1`;
  - `SURREALKIT_EMIT_NAME` — the emitter's `<name>`;
  - `SURREALKIT_EMIT_OUT` — its `out`, as configured. For information only: the emitter
    MUST NOT write there itself.
  - `SURREALKIT_EMIT_ACKNOWLEDGE` — the project's `[typegen] acknowledge` as a JSON object
    of diagnostic code to reason (`{}` when there is none). An emitter that refuses
    documents with error diagnostics SHOULD accept the codes listed here, so a project
    acknowledges a code once, for SurrealKit and every emitter.
- **stderr**: inherited. Progress and warnings go there.

The emitter MUST exit with status 0 and print exactly one JSON object on stdout:

```json
{
  "protocol": 1,
  "files": [
    { "path": "person.ts", "contents": "import { Schema } from \"effect\";\n…" },
    { "path": "tables/team.ts", "contents": "…" }
  ],
  "diagnostics": [
    { "severity": "warning", "code": "value-unresolved", "table": "person", "field": ["slug"],
      "source": "string::slug(name)", "reason": "…" }
  ]
}
```

- `protocol` MUST be `1`.
- `files` is the complete output for this document. Each `path` is relative to `out`,
  `/`-separated, without empty, `.` or `..` segments, without `\` or `:`, unique, and not
  `.surrealkit-emit.json`. `contents` is the file's UTF-8 text. An empty `files` array is
  valid and means the emitter generates nothing.
- `diagnostics` is optional and has the shape of schema document diagnostics. SurrealKit
  logs them; one of `error` severity fails a strict run unless its code is in
  `[typegen] acknowledge`.
- No other property is allowed.

The output MUST depend only on the document (and the emitter's own version and
arguments): the same document must give the same files, byte for byte, or `--check`
cannot work.

## What SurrealKit does with the reply

Everything is rendered before anything is written: SurrealKit runs every emitter, and only
when all of them (and its own outputs) succeeded does it publish. Then, for each emitter:

1. It refuses to **overwrite** a file in `out` that it did not write (one not listed in the
   manifest), in every mode. With `strict`, it also refuses to publish into an `out` that
   holds such a file at all; otherwise that is a warning and the file is kept.
2. Each file is written to a temporary sibling and renamed over the target. A file whose
   bytes are unchanged is not rewritten, so watchers do not rebuild.
3. Files the previous run produced and this one does not are deleted, with directories
   that become empty.
4. The manifest `out/.surrealkit-emit.json` is written last:
   `{ "emitter": "<name>", "protocol": 1, "files": ["person.ts", "tables/team.ts"] }`.
   Commit it with the generated files.

`--check` writes nothing. It fails, listing each, when a file or the manifest is missing or
different, and when `out` holds a file that is not part of the output (`extra:`), which is
how a hand-edited or hand-added file in a generated directory is caught. It also fails when
an emitter could not run (`failed:`), strict or not: files nobody produced cannot be
checked.

## Failure

An emitter fails when it cannot be started, exits non-zero, prints anything but a valid
reply, returns an invalid path, or (strict) reports an error diagnostic whose code is not
acknowledged. In a strict run (`--strict`, `[typegen] strict = true`, and
`surrealkit compile` by default) the command fails naming the emitter, and nothing is
written. `surrealkit compile --lenient` tolerates diagnostics only: an emitter that cannot
run, or replies with something that is not a valid reply, still fails it. Otherwise
(`typegen` and `sync` without strict) the failure is logged, that emitter's files are left
exactly as they were, and the other outputs are published; a `--check` fails.

## Writing an emitter

The smallest emitter is a script that reads stdin and prints a reply:

```sh
#!/bin/sh
digest=$(grep -o '"semanticDigest": "[^"]*"' | head -1 | cut -d'"' -f4)
printf '{"protocol":1,"files":[{"path":"digest.txt","contents":"%s\\n"}]}' "$digest"
```

A real emitter decodes the document strictly (unknown properties are errors, as SurrealKit's
own reader does), validates it against
[`schema-document-v2.schema.json`](../../crates/surrealkit/schema/schema-document-v2.schema.json)
if it wants a second line of defence, derives one schema per operation from the
field-presence matrix (contract section 7), and fails on a document version it does not
know.
