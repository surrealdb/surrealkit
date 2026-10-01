# SurrealKit

[![Crates.io](https://img.shields.io/crates/v/surrealkit.svg)](https://crates.io/crates/surrealkit) [![Documentation](https://docs.rs/surrealkit/badge.svg)](https://docs.rs/surrealkit)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

SurrealKit is a schema management and migration tool for SurrealDB. It lets you define your schema as `.surql` files and keeps your database in sync with them.

It provides two approaches to schema management:

- **Sync**: a fast, declarative push for development. Your schema files are the source of truth - add a definition and it gets created, change it and it gets updated, remove it and it gets deleted.
- **Rollouts**: controlled, phased migrations for shared and production databases. Changes are planned into reviewed manifests, applied in stages, and can be rolled back.

SurrealKit also includes a seeding system and a declarative testing framework for validating schemas, permissions, and API endpoints.

## Installation

| Method                                                           | Command                                                             | Notes                                                     |
| ---------------------------------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------- |
| [`cargo binstall`](https://github.com/cargo-bins/cargo-binstall) | `cargo binstall surrealkit`                                         | Fastest, downloads a prebuilt binary. Recommended.        |
| Cargo (from source)                                              | `cargo install surrealkit`                                          | Compiles locally. Works anywhere Rust does.               |
| Prebuilt tarball                                                 | [GitHub Releases](https://github.com/surrealdb/surrealkit/releases) | Manual download. Each archive ships a matching `.sha256`. |
| Docker                                                           | `docker pull ghcr.io/surrealdb/surrealkit:latest`                   | Multi-arch image on GHCR. Distroless base.                |

Prebuilt binaries are published for:

- **Linux**: `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`, `aarch64-unknown-linux-musl`
- **macOS**: `aarch64-apple-darwin` (Apple Silicon), `x86_64-apple-darwin` (Intel)
- **Windows**: `x86_64-pc-windows-msvc`

### Docker

Multi-arch (`linux/amd64`, `linux/arm64`) images are published to GitHub Container Registry on every release. The image is based on `gcr.io/distroless/cc-debian12:nonroot` - minimal (~25 MB), no shell, runs as uid 65532.

```sh
docker pull ghcr.io/surrealdb/surrealkit:latest
docker run --rm -v "$(pwd)/database:/database:ro" ghcr.io/surrealdb/surrealkit:latest \
    --host http://host.docker.internal:8000 --ns my_ns --db my_db sync
```

Available tags: `X.Y.Z` (exact), `X.Y` (minor line), `latest`.

Use in Docker Compose for E2E testing alongside SurrealDB:

```yaml
services:
  surrealdb:
    image: surrealdb/surrealdb:latest
    command: start --user root --pass root memory
    healthcheck:
      test: ["CMD", "/surreal", "is-ready"]
      interval: 1s
      timeout: 5s
      retries: 30

  surrealkit:
    image: ghcr.io/surrealdb/surrealkit:latest
    depends_on:
      surrealdb:
        condition: service_healthy
    volumes:
      - ./database:/database:ro
    command:
      - --host=http://surrealdb:8000
      - --ns=my_ns
      - --db=my_db
      - --user=root
      - --pass=root
      - sync
```

`surrealkit` exits on completion, so Compose moves on, ideal for "apply schema then run tests" pipelines.

## Library

SurrealKit can also be used as a Rust library. See [`crates/surrealkit/README.md`](crates/surrealkit/README.md) for the full library API reference.

## Usage

Initialise a new project:

```sh
surrealkit init
```

This creates a `database/` directory with the project scaffolding and lets you pick optional features to include. See [Templates](#templates) for details.

Connection details can be provided via CLI arguments, environment variables, or a `.env` file. The resolution order is: CLI args > system env vars > `.env` file > defaults.

### CLI Arguments

```bash
surrealkit --host http://localhost:8000 --ns my_ns --db my_db --user root --pass root sync
```

| Flag           | Description                                                            | Default                 |
| -------------- | ---------------------------------------------------------------------- | ----------------------- |
| `--host`       | Database host URL                                                      | `http://localhost:8000` |
| `--ns`         | Database namespace                                                     | `db`                    |
| `--db`         | Database name                                                          | `test`                  |
| `--user`       | Database user                                                          | `root`                  |
| `--pass`       | Database password                                                      | `root`                  |
| `--auth-level` | Authentication level: `root`, `namespace` / `ns`, `database` / `db`, or `none` | `root`                  |

### Embedded databases

SurrealKit can manage an in-process SurrealDB by pointing `--host` at an embedded endpoint. Authentication is skipped automatically (a fresh embedded datastore has no users):

```bash
surrealkit --host surrealkv://./data --ns my_ns --db my_db sync
surrealkit --host surrealkv://./data --ns my_ns --db my_db seed
```

Embedded schemes: `surrealkv://`, `rocksdb://`, `speedb://`, `mem://`, `file://`, `tikv://`. Pass `--auth-level none` to force the no-signin path on any endpoint.

The **prebuilt CLI bundles only the in-memory engine** to stay light. For a CLI that can open on-disk engines, build with the matching feature:

```bash
cargo install surrealkit --features kv-surrealkv   # or kv-rocksdb, or `embedded` for all
```

Embedded engines are single-process, so run the CLI while your application is stopped (it holds an exclusive lock on the datastore). For schema management inside your application at startup, use the [library](crates/surrealkit/README.md) instead, which needs no engine feature on SurrealKit itself.

### Environment Variables

- `SURREALDB_HOST`
- `SURREALDB_NAME`
- `SURREALDB_NAMESPACE`
- `SURREALDB_USER`
- `SURREALDB_PASSWORD`
- `SURREALDB_AUTH_LEVEL`, accepted values: `root`, `namespace` / `ns`, `database` / `db`, `none`

> The `DATABASE_*` aliases were removed in 1.0. If one is set without its
> `SURREALDB_*` replacement, SurrealKit fails with an explanatory error rather
> than ignoring it — ignoring it would silently fall back to the defaults and
> connect to the wrong database.
- `SURREALDB_FOLDER` — root folder for schema, rollouts, snapshots, seed, and tests (default: `./database`)
- `SURREALDB_CONNECT_TIMEOUT_SECS` — deadline for connect and sign-in (default: `30`; `0` waits indefinitely)
- `SURREALDB_QUERY_TIMEOUT_SECS` — deadline for a single rollout step's SQL (unset waits indefinitely)

These can be set as system environment variables or in a `.env` file.

SurrealKit creates and manages its internal sync and rollout metadata tables on your configured database.

## Templates

`surrealkit init` scaffolds a project from a template and lets you choose which optional features to include:

```sh
surrealkit init
```

In a terminal this shows a checklist of the template's features. Pick the ones you want and SurrealKit writes their schema, seed, and test files into `database/`. It always creates the base layout first: `schema/`, `rollouts/`, `snapshots/`, `seed/`, `tests/`, `setup.surql`, and `surrealkit.toml`.

### Choosing features without a prompt

When there is no terminal (such as CI) or you pass any of these flags, init runs without prompting:

| Flag             | Behaviour                                                            |
| ---------------- | ------------------------------------------------------------------- |
| `--feature <id>` | Enable a feature by id. Repeatable, and pulls in what it requires.  |
| `-y`, `--yes`    | Take the template's default features.                               |
| `--minimal`      | Scaffold the base project only, with no features.                   |
| `--force`        | Overwrite files that already exist. The default is to skip them.    |

```sh
surrealkit init --feature organizations --feature teams
surrealkit init -y
surrealkit init --minimal
```

A feature can depend on other features. Selecting one adds what it requires, and init prints what it added.

### Using your own template

Point `--from` at a local path or a git repository instead of the bundled template, or pick a bundled template by name with `--template`:

```sh
surrealkit init --from ./path/to/template
surrealkit init --from https://github.com/your-org/your-template.git
surrealkit init --from https://github.com/your-org/your-template.git#v1.0.0
surrealkit init --template default
```

Git sources are cloned with `git clone --depth 1`, so `git` must be on your PATH. Pin a branch, tag, or commit with `#rev`, and target a subdirectory with `#rev:subdir`.

### Template layout

A template is a directory with a `template.toml` manifest plus the files each feature contributes:

```toml
schema_version = 1
name = "default"
display_name = "My starter"
description = "Shown above the feature checklist"

[[features]]
id = "organizations"
name = "Organizations"
description = "Shown next to the feature in the checklist"
default = false
schema   = ["schema/organization/organization.surql"]
seed     = ["seed/organization_permissions.surql"]
suites   = ["tests/suites/organization.toml"]
fixtures = ["tests/fixtures/organization_seed.surql"]

[[features]]
id = "teams"
name = "Teams"
requires = ["organizations"]
schema = ["schema/team/team.surql"]
```

Each feature lists the files it adds, grouped by where they land:

- `schema` files are copied into `database/schema/`
- `seed` files into `database/seed/`
- `suites` files into `database/tests/suites/`
- `fixtures` files into `database/tests/fixtures/`

Set `default = true` to pre-check a feature in the prompt and include it with `-y`. Use `requires` to declare dependencies on other features.

### Bundled template

The bundled template provides an organization and access-control model with four opt-in features:

- **Organizations**: organizations, roles that bundle permissions, a per-app permission catalog, employees, and invitations.
- **Teams**: teams within an organization, with per-member roles.
- **Organization units**: a department and region hierarchy with unit-scoped permissions.
- **Subsidiaries and delegation**: parent and child organizations with cross-org delegated permissions.

Teams, units, and subsidiaries each require the organizations feature.

## Schema Modules and Targets

By default a project has one unnamed schema (`database/schema`) applied to one
database. That is still the default and needs no configuration.

For larger projects, declare **schema modules** — independently tracked sets of
schema files — and **targets** — named databases to apply them to.

```toml
# surrealkit.toml
[schema.core]

[schema.billing]
depends_on = ["core"]

[target.acme]
ns = "acme"
db = "prod"
primary = true

[target.globex]
ns = "globex"
db = "prod"
pass_env = "GLOBEX_DB_PASSWORD"   # never inline a password
```

```
database/
  schema/                    # the default module
  modules/
    core/schema/
    billing/schema/
```

```bash
surrealkit sync                              # every declared module, primary target
surrealkit sync --schema billing             # billing (and core, its dependency)
surrealkit sync --schema billing --no-deps   # billing alone
surrealkit sync --target acme                # every module, one target
surrealkit sync --all                        # the full matrix
surrealkit sync --all --keep-going           # don't stop at the first failure
```

### Why modules matter

Each module owns its own metadata, so **a module only ever prunes its own
database objects**. Without modules, syncing one schema against a database that
another schema had populated would remove the other's tables.

Named modules live under `modules/<name>/` rather than inside `database/schema`,
because the default module walks its schema directory recursively — nesting would
make it collect the named module's files and claim ownership of them.

Configure a custom location with `[schema.<name>] path` if you need one.

Before opening any database connection, filesystem sync resolves every selected
module and refuses if one has no `.surql` sources. This prevents a wrong working
directory or `--folder` value from becoming setup or prune activity. Use
`--allow-empty-prune` only when an empty source set is intentional; a selection
where no module applies to any target remains an error rather than a successful
no-op.

### Dependencies

`depends_on` orders application, so a module is never applied before what it
depends on. Selecting a module pulls its dependencies in, like `cargo build -p`;
`--no-deps` opts out. Cycles are rejected before anything touches a database.

### Targets and credentials

A target inherits everything it does not set from the ambient configuration
(`--host`/`--ns`/`--db` and the `SURREALDB_*` variables), so it usually only needs
`ns` and `db`.

Passwords are read from the environment via `pass_env`. A literal `pass` or
`password` key in `surrealkit.toml` is rejected, and an unset `pass_env` fails
before any connection is opened rather than part-way through a fan-out.

A target may restrict which modules apply to it:

```toml
[target.warehouse]
ns = "internal"
db = "analytics"
schemas = ["core", "analytics"]
```

It may also override the connection deadlines, which is useful when one target
is across a slower link than the rest:

```toml
[target.warehouse]
connect_timeout_secs = 60   # default 30; 0 waits indefinitely
query_timeout_secs = 900    # default unset, meaning no deadline on step SQL
```

Full key list for `[target.<name>]`: `host`, `ns`, `db`, `user`, `pass_env`,
`auth_level`, `schemas`, `primary`, `connect_timeout_secs`,
`query_timeout_secs`.

**A value set here wins over the equivalent CLI flag and environment variable.**
That holds for every key, not just the timeouts: naming a target selects a
described connection, so `--host` does not override a target's `host`, and
`--connect-timeout-secs 5 --target warehouse` uses the target's
`connect_timeout_secs` if it sets one. Omit the key to inherit, where the order
is CLI flag, then environment variable, then `.env`, then the default.

### Fan-out semantics

Targets are applied one at a time. Modules within a target stop at the first
failure, since they are dependency-ordered and the rest would build on a broken
base; targets also stop at the first failure unless you pass `--keep-going`.

There is no cross-database transaction, so a failed run can leave some targets
applied and others not. Every operation is idempotent, so re-running after a fix
is safe. `surrealkit sync --all` exits non-zero if any pair failed.

## Type Generation

`surrealkit typegen` introspects the live database and emits a JSON description
of its tables, fields, functions and params — and, when configured, TypeScript
types.

```bash
surrealkit typegen                 # writes database/types/schema.json
surrealkit typegen --stdout        # print instead
surrealkit typegen --out types.json
```

Configure TypeScript output in `surrealkit.toml`:

```toml
[typegen]
# Where generated TypeScript goes. Setting this enables TS generation:
# `surrealkit typegen` and `surrealkit sync` both write it.
typescript = "src/types"

# The file to write in that directory (default: index.ts). Naming it leaves
# index.ts free to be your package's own barrel.
filename = "schema.generated.ts"

# Optional formatter run on the generated file. The path is appended as the
# final argument. Failures are warnings, not errors.
format = "biome check --write"
```

`typescript` can also name the file itself, as in
`typescript = "src/types/database.ts"` (any `.ts`, `.mts` or `.cts` path), in
which case `filename` is not used. `surrealkit typegen --typescript <path>`
overrides the configured path for one run.

With `typescript` set, `surrealkit sync` regenerates types after applying schema
changes, so the generated types never drift from the database.

## Vite Plugin

[`vite-plugin-surrealkit`](packages/vite-plugin-surrealkit) runs `surrealkit sync`
from a Vite dev server or build, so schema changes apply as you edit.

```bash
npm install --save-dev vite-plugin-surrealkit
```

```ts
// vite.config.ts
import { surrealkitPlugin } from "vite-plugin-surrealkit";

export default defineConfig({
    plugins: [
        surrealkitPlugin({
            schemas: ["core", "billing"],   // optional: restrict modules
            reloadOnSync: true,
        }),
    ],
});
```

Requires Vite 8. See the
[plugin README](packages/vite-plugin-surrealkit/README.md) for all options.

## Team Workflow

SurrealKit now separates schema authoring, dev sync, and shared/prod rollouts:

### Sync vs Rollouts

- `surrealkit sync` is the fast desired-state reconciler for local, preview, and other disposable databases.
- `surrealkit sync` applies changed schema files and automatically removes SurrealKit-managed objects that were deleted from `database/schema`.
- `surrealkit rollout ...` is the production/shared-database migration path.
- `surrealkit rollout plan` turns the desired-state diff into a reviewed manifest in `database/rollouts/*.toml`.
- `surrealkit rollout start` applies the non-destructive expansion phase and records resumable state in the database.
- `surrealkit rollout complete` performs the destructive contract phase, including removing legacy objects after application cutover.
- Use `sync` when it is safe for the database to match local files immediately. Use `rollout` when changes need review, staged execution, rollback, or operator-controlled cutover.

1. Edit desired state in `database/schema/*.surql`
2. Reconcile local or disposable DBs with managed auto-prune:

```sh
surrealkit sync
```

3. Watch mode for local development, including file deletions:

```sh
surrealkit sync --watch
```

4. Baseline an existing shared/prod database before the first rollout:

```sh
surrealkit rollout baseline
```

5. Generate a rollout manifest from the current desired-state diff:

```sh
surrealkit rollout plan --name add_customer_indexes
```

`plan` handles additions and removals on its own. A change to an entity that
already exists — tightening an `ASSERT`, adjusting `PERMISSIONS` — needs
`--allow-modified`:

```sh
surrealkit rollout plan --name tighten_age_assert --allow-modified
```

The opt-in is about rollback, not about applying the change: applying it is just
`DEFINE ... OVERWRITE`. Undoing it means restoring the previous definition, which
`start` captures from the live database before the expand phase. That is a clean
undo for `ASSERT`, `PERMISSIONS` and `COMMENT`, and only a partial one for `TYPE`,
`VALUE` or `DEFAULT` changes, since it reverses the schema but not data written
under the new definition. Such a rollout is recorded as
`reversibility: definition_only` and `rollout status` says so.

`plan` freezes a copy of every schema file the rollout changes into a directory
beside the manifest, `database/rollouts/<id>/`, and records each file's sha256 in
the manifest. The rollout applies those copies, not whatever `database/schema`
holds when it runs, so a rollout planned for release 1.0.3 still applies 1.0.3's
SQL when a database catches up to 1.0.5. Frozen files are what was reviewed:
editing one fails the rollout with a hash mismatch; plan a new rollout instead.

`plan` also writes the snapshots, so commit the manifest, its directory and the
snapshots together. Reverting a plan you decided not to run means reverting all
three.

Steps you add to a manifest by hand (a `run_sql` backfill, an `assert_sql`
check) travel with it, and run wherever it runs.

6. Deploy with `rollout up`, let application cutover happen, then complete:

```sh
surrealkit rollout up               # every pending rollout, in order
# deploy the application
surrealkit rollout up --complete    # finish the newest
```

`up` works out where the database is (from its completed rollouts, or for a
database that has only been baselined, from the file hashes `baseline` stored)
and runs every rollout it has not run yet, in order. Each one is started and
completed, except the newest, which is started and left `ready_to_complete` so
the old and new application can overlap. Running `up` again leaves it waiting;
`up --complete` or `rollout complete <id>` finishes it.

Catching up across several releases runs the older rollouts' contract phases
straight away, so anything they remove is gone before the new application
starts. That is the same as running each release's deploy in turn.

Rollouts can still be driven one at a time:

```sh
surrealkit rollout start 20260302153045__add_customer_indexes
surrealkit rollout complete 20260302153045__add_customer_indexes
```

`start` refuses a rollout whose predecessors have not run here, and names them.

`surrealkit rollout up --dry-run` shows what would run. `--from <id>` names the
first rollout a database still needs, when its history cannot say: say, one
whose schema was applied by hand.

A new database should get its schema with `surrealkit sync` and then
`surrealkit rollout baseline`, after which `up` has nothing to do. If the first
rollout in the project was planned from an empty schema folder, `up` can also
build a database from nothing.

7. Roll back an in-flight rollout if needed:

```sh
surrealkit rollout rollback 20260302153045__add_customer_indexes
```

Generated rollout manifests are written to `database/rollouts/*.toml`.
Local snapshots are tracked in:

- `database/snapshots/schema_snapshot.json`
- `database/snapshots/catalog_snapshot.json`

To validate a rollout manifest without mutating the database:

```sh
surrealkit rollout lint 20260302153045__add_customer_indexes
```

Without an id, `lint` checks every manifest and how they chain, without
connecting. That suits a CI check:

```sh
surrealkit rollout lint
```

It fails on:
- two rollouts planned from the same snapshots (two branches each ran `plan`;
  delete the later one and plan it again after merging)
- a gap in the chain
- a frozen file that does not match its manifest
- schema files that no rollout plans yet

Manifests planned before 1.0.0-beta.6 do not carry their SQL, and can only run
against the schema they were planned from. `lint` lists them. To let a
database catch up across one, check out the commit that planned it and run:

```sh
surrealkit rollout freeze 20260302153045__add_customer_indexes
```

If you check out on Windows, keep git from converting line endings in frozen
files, which would change their hashes:

```gitattributes
database/rollouts/** -text
```

`rollout status` lists, after the rollout records, what is still pending in
order, and any rollout the database moved past without running (whose data
steps never ran there).

To inspect rollout state stored in the database:

```sh
surrealkit rollout status
```

If managed destructive prune is enabled against a shared DB, SurrealKit requires explicit override:

```sh
surrealkit sync --allow-shared-prune
```

To allow non-`DEFINE` statements (e.g. `INSERT`, `UPDATE`, `CREATE`) in schema files:

```sh
surrealkit sync --allow-all-statements
```

### How schema files are applied

Sync and rollouts re-apply whole files, so every `DEFINE` is made safe to run
again: SurrealKit adds `OVERWRITE`, and turns `IF NOT EXISTS` into `OVERWRITE`
so a changed definition is applied. Everything else in the statement, comments
and regex literals included, reaches the server exactly as written.

`DEFINE SEQUENCE` is the exception, because a sequence's definition carries its
counter: `OVERWRITE` puts it back to `START` (immediately on SurrealDB 3.3, after
a restart on 3.2), and the ids it hands out next collide with existing records.
A plain `DEFINE SEQUENCE` is applied as `IF NOT EXISTS`, and an explicit
`IF NOT EXISTS` is kept. A changed definition therefore does not reach a database
where the sequence exists, and sync says so. An explicit `OVERWRITE` is applied as
written, with a warning every time. Reposition a sequence deliberately, in a
rollout `run_sql` step or by hand.

A record `DEFINE ACCESS` with no `WITH JWT ... KEY` gets a new random signing key
every time it is overwritten, which signs every user out. SurrealKit warns when it
applies one. Give it a stable key, for example
`WITH JWT ALGORITHM HS512 KEY ${JWT_SECRET}`.

A file SurrealKit cannot read (an unterminated string, an unmatched bracket, a
`DEFINE` that starts inside another statement) stops sync or plan with the file,
line and column, before anything is applied or pruned.

`surrealkit sync` is the local/dev reconciliation path. `surrealkit rollout ...` is the shared/prod migration path.

### Recovering a stuck rollout

Connect and sign-in are bounded by `--connect-timeout-secs` (30s by default), so
an endpoint that accepts the connection and never responds fails with a message
naming it rather than blocking. Step SQL is not bounded by default — a legitimate
index build can take hours — but every step logs when it starts and every 15
seconds while it runs, so a slow step is distinguishable from a stuck one. Bound
it explicitly with `--query-timeout-secs` when you want to.

If `surrealkit rollout complete` (or `rollback`) is killed mid-flight, the
`__rollout` row can be left in an intermediate state — `running_complete`,
`running_rollback`, or `running_start` — even though the schema is already
materialised. Re-running `complete`/`rollback` will not always heal the
metadata because the SQL steps are already applied.

Use `repair` to finish the metadata transition without re-running any SQL:

```sh
surrealkit rollout repair 20260302153045__add_customer_indexes
```

Behaviour by stuck state:

- `running_complete` → flips to `completed`, restores `target_entities`.
- `running_rollback` → flips to `rolled_back`, restores `source_entities`.
- `running_start` → flips to `failed` with a note; re-run `start`
  (idempotent) or `rollback`.

Repair never re-executes per-step SQL — it only reconciles `__rollout` and
`__entity` so subsequent `sync` / `plan` runs see a clean state.

### Seeding

Seeding runs on demand and is **idempotent**. Each file is tracked in the `__seed` table by a content hash and runs only on first boot or when its content changes, so it is safe to run repeatedly (and on every deploy):

```sh
surrealkit seed           # applies new/changed seed files; skips unchanged ones
surrealkit seed --force   # re-run every seed file, ignoring the tracking table
```

To ship seeds inside a binary (no filesystem at runtime), use the library's `embed_seed!` macro. See the [library README](crates/surrealkit/README.md#embedded-seeds-embed_seed--seed).

## Template Variables

Use `${VAR_NAME}` tokens in any `.surql` file (schema, seed, or rollout SQL) and bind values to them at runtime. Useful for credentials, table prefixes, or environment names that differ between dev, staging, and prod.

```sql
-- database/schema/access.surql
REMOVE USER IF EXISTS ${talent_username} ON DATABASE;
DEFINE USER ${talent_username} ON DATABASE PASSWORD "${talent_password}" ROLES EDITOR;

-- database/schema/tables.surql
DEFINE TABLE IF NOT EXISTS ${schema_prefix}_users SCHEMAFULL;
```

### Resolution Priority

Values are resolved in this order (highest wins):

| Source                                      | Example                                             |
| ------------------------------------------- | --------------------------------------------------- |
| `--var KEY=VALUE` CLI flag                  | `surrealkit sync --var schema_prefix=acme`          |
| `SURREALKIT_VAR_<KEY>` environment variable | `SURREALKIT_VAR_SCHEMA_PREFIX=acme surrealkit sync` |
| `[variables]` section in `surrealkit.toml`  | _(see below)_                                       |

Variable names are case-insensitive: `${FOO}`, `${foo}`, and `${Foo}` all match key `FOO`.

### `surrealkit.toml`

Place a `surrealkit.toml` at the project root (created by `surrealkit init`):

```toml
[variables]
schema_prefix = "myapp"
talent_username = "talent_rw"
talent_password = "change_me_in_prod"
environment = "development"
```

### CLI Flag

`--var` works on `sync`, `seed`, `apply`, and `rollout start/complete/rollback`. Repeatable:

```sh
surrealkit sync --var schema_prefix=acme --var talent_username=talent_rw
surrealkit rollout start my_rollout --var schema_prefix=acme
```

### Environment Variables

Any environment variable prefixed with `SURREALKIT_VAR_` is picked up automatically:

```sh
export SURREALKIT_VAR_SCHEMA_PREFIX=acme
export SURREALKIT_VAR_TALENT_USERNAME=talent_rw
surrealkit sync
```

### Escape Sequence

To emit a literal `${...}` (no substitution), double the dollar sign:

```sql
-- $${literal} becomes ${literal} in the output sent to SurrealDB
SET note = 'pass $${MY_VAR} literally';
```

### Where Substitution Runs

Applied: `sync`, `seed`, `apply`, `rollout start`, `rollout complete`, `rollout rollback`.

Not applied: `rollout plan`, `rollout baseline`, `rollout status`, `rollout lint` (no user SQL is executed).

### Undefined Variables

An undefined variable is always a hard error. Surrealkit will not silently skip or leave the token in the SQL:

```
error: template variable 'SCHEMA_PREFIX' is not defined
       (set via --var SCHEMA_PREFIX=VALUE, SURREALKIT_VAR_SCHEMA_PREFIX env var, or surrealkit.toml [variables])
```

### Known Limitations

- **Hash-based re-sync**: `surrealkit sync` tracks schema files by content hash. Changing a variable value does not change the file hash, so sync will not re-apply the file. Touch the file or remove its tracking entry to force re-application.
- **Watch mode**: variables are resolved once at startup. Edits to `surrealkit.toml` during `--watch` require a restart.
- **Catalog snapshots**: entity names containing `${VAR}` tokens appear literally in `catalog_snapshot.json` and are not substituted. This affects drift detection for template-named tables; prefer fixed entity names in production schemas.
- **String literals**: substitution is textual, so `${VAR}` inside a SurrealQL string literal is also replaced.

## Testing Framework

[Testing Example](https://github.com/surrealdb/surrealkit/blob/main/examples/testing/README.md)

```sh
surrealkit test
```

The runner executes declarative TOML suites from `database/tests/suites/*.toml` and supports:

- SQL assertion tests (`sql_expect`)
- Permission rule matrices (`permissions_matrix`)
- Schema metadata assertions (`schema_metadata`)
- Schema behavior assertions (`schema_behavior`)
- HTTP API endpoint assertions (`api_request`)

By default, each suite runs in an isolated ephemeral namespace/database and fails CI on any test failure.
The runner performs filesystem sync first, so the same non-empty source preflight
applies. Use `--no-sync` when the suite's fixtures intentionally own the complete
schema instead.

### Testing against replayed rollouts

A suite can get its database's schema from the rollouts instead of sync, the
way production got it:

```toml
# database/tests/config.toml
[defaults]
schema_from = "both"   # "sync" (the default), "rollouts", or "both"
```

With `rollouts`, each suite's database is built by replaying every rollout in
order from an empty database, which needs the first rollout to have been
planned from an empty schema folder. With `both`, every suite runs once on each,
reported as `name [sync]` and `name [rollouts]`. A suite can set `schema_from`
itself, and `surrealkit test --schema-from <source>` overrides both.

Whenever rollouts are replayed, a parity check runs first. It builds one
database with sync and one from the rollouts, and fails on any definition they
disagree on. That is how a schema change nobody planned a rollout for shows up.
Turn it off with:

```toml
[rollouts]
parity = false
```

See [examples/rollout-testing](examples/rollout-testing).

### CLI Flags

`surrealkit test` supports:

- `--suite <glob>`
- `--case <glob>`
- `--tag <tag>` (repeatable)
- `--fail-fast`
- `--parallel <N>`
- `--json-out <path>`
- `--no-setup`
- `--no-sync`
- `--no-seed`
- `--base-url <url>`
- `--timeout-ms <ms>`
- `--keep-db`
- `--schema-from <sync|rollouts|both>`

### Global Config

Global test settings live in `database/tests/config.toml`.

Example:

```toml
[defaults]
timeout_ms = 10000
base_url = "http://localhost:8000"

[actors.root]
kind = "root"
```

Optional env fallbacks:

- `SURREALKIT_TEST_BASE_URL`
- `SURREALKIT_TEST_TIMEOUT_MS`
- `SURREALDB_HOST` (used as API base URL fallback when test-specific base URL is not set)

### Example Suite

```toml
name = "security_smoke"
tags = ["smoke", "security"]

[[cases]]
name = "guest_cannot_create_order"
kind = "sql_expect"
actor = "guest"
sql = "CREATE order CONTENT { total: 10 };"
allow = false
error_contains = "permission"

[[cases]]
name = "orders_api_returns_200"
kind = "api_request"
actor = "root"
method = "GET"
path = "/api/orders"
expected_status = 200

[[cases.body_assertions]]
path = "0.id"
exists = true
```

An assertion whose `path` (or `header_assertions` `name`) is not present in the result
**fails** with `path '<path>' not found`. This catches typos and queries that matched
zero rows, which would otherwise report a pass without ever running the comparison. To
assert that a field is genuinely absent, state it explicitly with `exists = false` —
that is the only spec that passes on a missing path.

To compare a returned field against the authenticated actor, use `equals_auth` with `$auth` or `$auth.<property>`:

```toml
[[cases]]
name = "user_can_create_calendar"
kind = "sql_expect"
actor = "user_alice"
sql = "CREATE calendar CONTENT { name: 'Alice Personal' };"
allow = true

[[cases.assertions]]
path = "0.owner"
equals_auth = "$auth.id"
```

### Actor Example (Namespace / Database / Record / Token / Headers)

```toml
[actors.reader]
kind = "database"
namespace = "app"
database = "main"
username_env = "TEST_DB_READER_USER"
password_env = "TEST_DB_READER_PASS"

[actors.access_user]
kind = "record"
access = "app_access"
signup_params = { email = "viewer@example.com", password = "viewer-password" }
signin_params = { email = "viewer@example.com", password = "viewer-password" }

[actors.jwt_actor]
kind = "token"
token_env = "TEST_API_JWT"

[actors.custom_client]
kind = "headers"
headers = { "x-tenant-id" = "tenant_a" }
```

For record access actors, `signup_params` is optional and runs before authentication. `signin_params` is used for the actual signin step, and legacy `params` still works as a signin alias for backward compatibility.

### Permission Matrix Example

```toml
[[cases]]
name = "reader_permissions"
kind = "permissions_matrix"
actor = "reader"
table = "order"
record_id = "perm_test"

[[cases.rules]]
name = "reader can see orders"
action = "select"
allow = true

[[cases.rules]]
action = "update"
allow = false
error_contains = "permission"
```

A rule's `name` is what the report shows; without one it reads `action:update (#2)`.

Update and delete rules act on a copy of the record, with a new id, so the
record itself is untouched. Create rules create a new record with its content.

`record_id = "$auth"` targets the record the actor is signed in as:

```toml
[actors.record]
kind = "record"
access = "human_passphrase"
signup_params = { email = "user@example.test", passphrase = "abc" }
signin_params = { email = "user@example.test", passphrase = "abc" }

[[cases]]
name = "user can be created and signed in and read its own record"
kind = "permissions_matrix"
actor = "record"
table = "user"
record_id = "$auth"

[[cases.rules]]
name = "reads its own record"
action = "select"
allow = true

[[cases.rules]]
name = "updates its own record"
action = "update"
allow = true
```

With `$auth`, update and delete rules act on the record itself, because a
permission written as `WHERE id = $auth` can never match a copy. Root puts the
record back afterwards: an update is reverted with the record's original content
(fields computed with `VALUE` are recomputed then), and a deleted record is
created again (anything the delete cascaded to through `REFERENCE ... ON DELETE`
is not). `table` must be the table of the signed-in record. A create rule on
`$auth` is rejected when the suite loads, since signup already created it.

### JSON Reports for CI

Generate machine-readable output:

```sh
surrealkit test --json-out database/tests/report.json
```

The command exits non-zero if any case fails.
