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
| `--auth-level` | Authentication level: `root`, `namespace` / `ns`, or `database` / `db` | `root`                  |

### Environment Variables

- `SURREALDB_HOST` (fallback: `DATABASE_HOST`)
- `SURREALDB_NAME` (fallback: `DATABASE_NAME`)
- `SURREALDB_NAMESPACE` (fallback: `DATABASE_NAMESPACE`)
- `SURREALDB_USER` (fallback: `DATABASE_USER`)
- `SURREALDB_PASSWORD` (fallback: `DATABASE_PASSWORD`)
- `SURREALDB_AUTH_LEVEL` (fallback: `DATABASE_AUTH_LEVEL`) — accepted values: `root`, `namespace` / `ns`, `database` / `db`
- `SURREALDB_FOLDER` — root folder for schema, rollouts, snapshots, seed, and tests (default: `./database`)

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

6. Start the rollout, let application cutover happen, then complete it:

```sh
surrealkit rollout start 20260302153045__add_customer_indexes
surrealkit rollout complete 20260302153045__add_customer_indexes
```

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

[Testing Example](https://github.com/ForetagInc/surrealkit/blob/main/examples/testing/README.md)

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

The command exits non-zero if any case fails.
