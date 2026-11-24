# Generating statement modules

Every statement here cites the package README (`packages/effect-surrealkit/README.md`) or a test under `packages/effect-surrealkit/test/`.

## The `statements` command

`effect-surrealkit statements --input <dir> --out <dir>` reads one directory per scope under `--input`, each holding `.surql` files, and writes `<out>/<scope>/statements.ts` with one constant per file and `<out>/index.ts` re-exporting every scope as a namespace. A file name must be a JavaScript identifier and not a reserved word; names that differ only in case, and anything in `--input` other than scope directories of `.surql` files, are errors. (README, "Statements from .surql files")

| Flag | What it does |
| --- | --- |
| `--check` | Writes nothing; exits 1 when anything is missing, stale or edited. |
| `--watch` | Regenerates on change; cannot be combined with `--check`. |
| `--import-extension .ts` | Changes the extension of the index's imports (default `.js`). |
| `--hole-open <text>`, `--hole-close <text>` | The delimiters of a template hole (default `__` and `__`). |
| `--schema <file>` | A SurrealKit schema document v2: turns on the checks below. |
| `--verify` | Sends each complete text to a discarded in-memory engine before writing. |
| `--verify-schema <file>` | SurrealQL loaded before the texts, in the order given; needs `--verify`. |
| `--verify-timeout <seconds>` | How long each text or schema file may run, a whole number (default 10); needs `--verify`. |

(README, "Statements from .surql files"; "Checking texts on an engine: --verify")

## What `--schema` checks

Without the schema document none of these run, and the scope is generated unchecked. (README, "Projected fields")

- Every table or `fn::` function a file names must be declared, or created by the same file with `DEFINE TABLE` or `DEFINE FUNCTION`. (README, "Statements from .surql files")
- A bare field a `SELECT` projects from one static `SCHEMAFULL` table must be declared: SurrealDB 3.3 and 4.0 answer `SELECT nmae FROM person` with `{}` and no error. Schemaless tables and views are not checked. A row written while its table was schemaless keeps its undeclared fields, so declare the field or clear it from the old rows. (README, "Projected fields"; `test/projections-engine.test.ts`)
- A bare field a query compares in its `WHERE` on one static `SCHEMAFULL` table must be declared, whatever the other operand, in a block too: both versions answer `WHERE nmae = 'Ada'` with no rows and no error. So must the target of `SET field = <literal>` when the literal is never NONE. To clear a field a row kept from before its table became `SCHEMAFULL`, use `UNSET` or `= NONE`, which the check does not read. (README, "Compared and assigned fields"; `test/fields-engine.test.ts`)
- A binding compared with or assigned to a field, used as a record key, or passed to a custom function must resolve. A binding whose uses resolve to one simple kind gets a `binds` entry, and one assigned to a field that rejects NONE gets a `rejectsNone` entry; `sequence(...).define` then refuses a codec that cannot hold that kind, or one that can hold NONE. Code that compiled before can fail once the modules are generated again with `--schema`. (README, "What bindings are used as")

## `--verify`

It needs the optional peer `@surrealdb/node` (`npm install --save-dev @surrealdb/node @surrealdb/node-native`). Each complete text (a part with no holes) is sent once with no bindings; fragments and templates are skipped. A parse error (with file, line and column), a result count other than the analyzer's `slots`, and a text that does not finish within the timeout are problems: generation exits 1 and writes nothing. Any other error a text meets without bindings is not a problem. The texts are executed on a database that is discarded, one after another, with network targets, `http::` functions and scripting denied. `verifyStatements` from `effect-surrealkit/testing` is the same check for a build that renders in process with `renderStatementModules`. (README, "Checking texts on an engine: --verify"; "Rendering in process"; `test/verify.test.ts`)

## The content mapper

The command and the mapper are two supported ways to the same modules. A `.surql` import yields the constant the command writes for that file, byte for byte. (README, "Importing .surql files: the content mapper"; `test/mapper.test.ts`)

- Type check: `tsc --noEmit --runExternalCode` from TypeScript 7.1 (`typescript@7.1.0-beta` is the tested version); TypeScript accepts that flag only on the command line. Bun's `bun check` ignores `contentMappers`: a project checked by it keeps the command.
- Run time: `node --import effect-surrealkit/register` (Node 22.21.1 or later; refused on 23.0 to 23.4), `bun --preload effect-surrealkit/register` or `surqlBunPlugin()` from `effect-surrealkit/mapper`, and `surrealkitPlugin({ surql: true })` from `vite-plugin-surrealkit`. Bun reads a `bunfig.toml` preload only from its working directory, so a process started elsewhere does not handle `.surql` imports: start it with `bun --cwd <project>` or pass the preload as a path. The loaders keep no cache, about 3 ms a file at start-up on Node.
- A type-aware linter that does not run content mappers (typescript-eslint on TypeScript 5.9) sees each `.surql` import as unresolved and reports its `no-unsafe-*` rules at each use.
- One configuration: write the `contentMappers` entry (`package`, `extensions: [".surql"]`, `options`) in the `tsconfig.json` that is checked, never through `extends`. Its `input` option is the command's `--input`, and a file must be `<input>/<scope>/<name>.surql`. `schema` and `schemas` are the command's `--schema`. The type check and the loaders refuse every file when the configuration differs or is inherited. A nested `tsconfig.json` with its own entry over the same directory gives a file two entries: when they would render it differently, both type checks and every loader refuse that file by name (`effect-surrealkit8`); give the file one entry or the same options in both.
- No engine runs in the mapper, so keep `--verify` or `verifyStatements` where the build runs.
- A new `.surql` file added under an input is not picked up by an incremental `tsc` run until the next full run.

(README, "Importing .surql files: the content mapper")
