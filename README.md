# SurrealKit

[![Crates.io](https://img.shields.io/crates/v/surrealkit.svg)](https://crates.io/crates/surrealkit) [![Documentation](https://docs.rs/surrealkit/badge.svg)](https://docs.rs/surrealkit)
[![License](https://img.shields.io/badge/license-Unlicense-blue.svg)](https://unlicense.org/)

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
    --host http://host.docker.internal:8000 sync --schema admin
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

This creates a directory `/database` with the necessary scaffolding

```
database/
  schemas/     ← schema SQL files, one subdirectory per named schema
  rollouts/    ← rollout manifests per named schema
  snapshots/   ← plan snapshots per named schema
  seed/        ← seed files per named schema
  tests/
  setup.surql
surrealkit.toml
```

Connection details can be provided via CLI arguments, environment variables, or a `.env` file. Namespace/database targets are selected by schema commands such as `sync --schema admin`. The resolution order for host/auth is: CLI args > system env vars > `.env` file > defaults.

### CLI Arguments

```bash
surrealkit --host http://localhost:8000 --user root --pass root sync --schema admin
```

| Flag           | Description                                                            | Default                 |
| -------------- | ---------------------------------------------------------------------- | ----------------------- |
| `--host`       | Database host URL                                                      | `http://localhost:8000` |
| `--ns`         | Deprecated namespace override for legacy flat mode                     | `db`                    |
| `--db`         | Deprecated database override for legacy flat mode                      | `test`                  |
| `--user`       | Database user                                                          | `root`                  |
| `--pass`       | Database password                                                      | `root`                  |
| `--auth-level` | Authentication level: `root`, `namespace` / `ns`, or `database` / `db` | `root`                  |

### Environment Variables

- `SURREALDB_HOST` (fallback: `DATABASE_HOST`)
- `SURREALDB_USER` (fallback: `DATABASE_USER`)
- `SURREALDB_PASSWORD` (fallback: `DATABASE_PASSWORD`)
- `SURREALDB_AUTH_LEVEL` (fallback: `DATABASE_AUTH_LEVEL`) — accepted values: `root`, `namespace` / `ns`, `database` / `db`
- `SURREALDB_FOLDER` — root folder for schema, rollouts, snapshots, seed, and tests (default: `./database`)

**Deprecated** (legacy flat-mode only — only effective when no `[schema.*]` entries exist in `surrealkit.toml`, or when using deprecated `--ns` / `--db`):

- `SURREALDB_NAMESPACE` (fallback: `DATABASE_NAMESPACE`) — namespace for flat sync/seed (default: `db`)
- `SURREALDB_NAME` (fallback: `DATABASE_NAME`) — database for flat sync/seed (default: `test`)

These can be set as system environment variables or in a `.env` file.

SurrealKit creates and manages its internal sync and rollout metadata tables on your configured database.

### Schemas

Named schemas live in `surrealkit.toml` and select the namespace/database target for schema-aware commands. Schemas can extend a base schema, which composes files from `database/schemas/<name>/` and seeds from `database/seed/<name>/` in inheritance order.

```toml
[schema.base]

[schema.admin]
extends = "base"
ns = "system"
db = "main"

[schema.org]
extends = "base"
ns = "org_${org_id}"
db = "main"
required_variables = ["org_id"]
```

```sh
# Sync/seed all resolved schemas (default when schemas are defined)
surrealkit sync
surrealkit seed

# Target a single schema
surrealkit sync --schema admin
surrealkit sync --schema org --var org_id=acme
surrealkit seed --schema admin
surrealkit setup --schema admin
surrealkit apply --schema admin ./custom.surql
```

Schemas without `ns` and `db`, such as `base`, are **abstract** and only contribute files through child schemas. Schemas with `ns`/`db` but unbound `required_variables` are **template schemas** — they resolve once the required vars are supplied. All other schemas are fully **resolved** and can be used by schema-aware commands (`sync`, `seed`, `setup`, `apply`, `rollout`, `status`, and `test`).

## Team Workflow

SurrealKit now separates schema authoring, dev sync, and shared/prod rollouts:

### Sync vs Rollouts

- `surrealkit sync` is the fast desired-state reconciler for local, preview, and other disposable databases. When schemas are defined in `surrealkit.toml`, it syncs all resolved schemas by default; template schemas with missing `required_variables` are an error unless `--skip-template-schemas` is passed.
- `surrealkit sync --schema <name>` targets a single named schema.
- `surrealkit rollout ...` is the production/shared-database migration path.
- `surrealkit rollout plan` turns the desired-state diff into a reviewed manifest in `database/rollouts/*.toml`.
- `surrealkit rollout start` applies the non-destructive expansion phase and records resumable state in the database.
- `surrealkit rollout complete` performs the destructive contract phase, including removing legacy objects after application cutover.
- Use `sync` when it is safe for the database to match local files immediately. Use `rollout` when changes need review, staged execution, rollback, or operator-controlled cutover.

> **Deprecation notice:** If no schemas are defined in `surrealkit.toml`, schema-aware commands fall back to the legacy flat `database/schema/` and `database/seed/` directories and print a warning. Move files manually to the named-schema layout (see [Moving from flat schema](#moving-from-flat-schema)). The flat directories, `--ns`, and `--db` will be removed in a future release.

1. Edit desired state in `database/schemas/<name>/*.surql`
2. Reconcile local or disposable DBs with managed auto-prune (syncs all schemas):

```sh
surrealkit sync
```

3. Watch mode for local development, including file deletions:

```sh
surrealkit sync --watch
```

4. Baseline an existing shared/prod database before the first rollout:

```sh
surrealkit rollout baseline --schema admin
```

5. Generate a rollout manifest from the current desired-state diff:

```sh
surrealkit rollout plan --schema admin --name add_customer_indexes
```

6. Start the rollout, let application cutover happen, then complete it:

```sh
surrealkit rollout start --schema admin 20260302153045__add_customer_indexes
surrealkit rollout complete --schema admin 20260302153045__add_customer_indexes
```

7. Roll back an in-flight rollout if needed:

```sh
surrealkit rollout rollback --schema admin 20260302153045__add_customer_indexes
```

Rollout manifests and local snapshots are stored per schema:

| Path | Purpose |
|---|---|
| `database/rollouts/<name>/*.toml` | Rollout manifests |
| `database/snapshots/<name>/schema_snapshot.json` | Schema file hashes after last plan |
| `database/snapshots/<name>/catalog_snapshot.json` | Managed-entity catalog after last plan |

To validate a rollout manifest without mutating the database:

```sh
surrealkit rollout lint --schema admin 20260302153045__add_customer_indexes
```

To inspect rollout state stored in the database:

```sh
surrealkit rollout status --schema admin
```

If managed destructive prune is enabled against a shared DB, SurrealKit requires explicit override:

```sh
surrealkit sync --allow-shared-prune
```

To allow non-`DEFINE` statements (e.g. `INSERT`, `UPDATE`, `CREATE`) in schema files:

```sh
surrealkit sync --allow-all-statements
```

`surrealkit sync` is the local/dev reconciliation path. `surrealkit rollout ...` is the shared/prod migration path.

### Recovering a stuck rollout

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

Seeding runs on demand:

```sh
surrealkit seed
```

## Moving from flat schema

If your project was created before named schemas existed, your files live in the flat layout:

```
database/
  schema/          ← SQL files
  seed/            ← seed files
  rollouts/        ← manifest .toml files at the root
  snapshots/       ← *_snapshot.json files at the root
```

The named-schema layout puts everything under a schema name:

```
database/
  schemas/<name>/  ← SQL files
  seed/<name>/     ← seed files
  rollouts/<name>/ ← manifest .toml files
  snapshots/<name>/← snapshot JSON files
```

Move flat files into a named subdirectory:

```text
database/schema/*.surql             -> database/schemas/main/*.surql
database/seed/*.surql               -> database/seed/main/*.surql
database/rollouts/*.toml            -> database/rollouts/main/*.toml
database/snapshots/*_snapshot.json  -> database/snapshots/main/*_snapshot.json
```

Then add the schema to `surrealkit.toml` (replacing `ns`/`db` with your values):

```toml
[schema.main]
ns = "<your-namespace>"
db = "<your-database>"
```

Verify the migration:

```sh
surrealkit sync --schema main
```

## Template Variables

Use `${VAR_NAME}` tokens in any `.surql` file (schema, seed, or rollout SQL) and bind values to them at runtime. Useful for credentials, table prefixes, or environment names that differ between dev, staging, and prod.

```sql
-- database/schema/roles.surql
DROP ROLE IF EXISTS ${talent_username};
DEFINE ROLE ${talent_username} PERMISSIONS FULL;

-- database/schema/tables.surql
DEFINE TABLE ${schema_prefix}_users SCHEMAFULL;
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

[Testing Example](https://github.com/ForetagInc/surrealkit/blob/main/examples/testing/README.md)

```sh
# Test all resolved schemas in sequence (default when schemas are defined)
surrealkit test

# Test a single schema
surrealkit test --schema main

# Skip template schemas with unresolved variables instead of erroring
surrealkit test --skip-template-schemas
```

When named schemas are defined in `surrealkit.toml`, the runner tests each resolved schema in sequence — applying that schema's SQL files and seed files before each suite, and using its `ns`/`db` as the base for the isolated test namespace and database. Use `--schema <name>` to target a single schema.

For legacy flat-schema projects (no `[schema.*]` in `surrealkit.toml`), `--schema` can be omitted and the flat `database/schema/` and `database/seed/` directories are used.

The runner executes declarative TOML suites from `database/tests/suites/*.toml` and supports:

- SQL assertion tests (`sql_expect`)
- Permission rule matrices (`permissions_matrix`)
- Schema metadata assertions (`schema_metadata`)
- Schema behavior assertions (`schema_behavior`)
- HTTP API endpoint assertions (`api_request`)

By default, each suite runs in an isolated ephemeral namespace/database and fails CI on any test failure.

### CLI Flags

`surrealkit test` supports:

- `--schema <name>` — target a single schema; omit to test all resolved schemas
- `--skip-template-schemas` — skip template schemas with missing vars instead of erroring (incompatible with `--schema`)
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
- `SURREALDB_HOST` or `DATABASE_HOST` (used as API base URL fallback when test-specific base URL is not set)

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
action = "select"
allow = true

[[cases.rules]]
action = "update"
allow = false
error_contains = "permission"
```

### JSON Reports for CI

Generate machine-readable output:

```sh
surrealkit test --json-out database/tests/report.json
```

The command exits non-zero if any case fails. When a single schema (or legacy flat mode) is tested, the JSON shape is the normal run report. When multiple named schemas are tested, the JSON file contains a top-level aggregate summary with one report per schema.
