# SurrealKit

[![Crates.io](https://img.shields.io/crates/v/surrealkit.svg)](https://crates.io/crates/surrealkit) [![Documentation](https://docs.rs/surrealkit/badge.svg)](https://docs.rs/surrealkit)
[![License](https://img.shields.io/badge/license-Unlicense-blue.svg)](https://unlicense.org/)

Manage SurrealDB schema sync, phased rollouts, seeding, and testing for SurrealDB.

## Scope

This project manages SurrealDB schema sync, phased rollouts, seed data, testing, and database administration for SurrealDB v3. The rollout path is designed for shared and production-like databases, but should still be treated as experimental until it has broader field validation.

## Usage

Install via Cargo:

```sh
cargo install surrealkit
```

Or download a prebuilt binary from [GitHub Releases](https://github.com/ForetagInc/surrealkit/releases) (Linux, macOS Intel/Apple Silicon, Windows).

Initialise a new project:

```sh
surrealkit init
```

This creates a directory `/database` with the necessary scaffolding

The following ENV variables will be picked up for your `.env` file, SurrealKit assumes you're using SurrealDB as a Web Database.

- `PUBLIC_DATABASE_HOST`
- `PUBLIC_DATABASE_NAME`
- `PUBLIC_DATABASE_NAMESPACE`
- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`

SurrealKit creates and manages its internal sync and rollout metadata tables on your configured database.

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

- `database/.surrealkit/schema_snapshot.json`
- `database/.surrealkit/catalog_snapshot.json`

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

`surrealkit sync` is the local/dev reconciliation path. `surrealkit rollout ...` is the shared/prod migration path.

### Seeding

Seeding runs on demand:

```sh
surrealkit seed
```

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
- `PUBLIC_DATABASE_HOST` (used as API base URL fallback when test-specific base URL is not set)

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
