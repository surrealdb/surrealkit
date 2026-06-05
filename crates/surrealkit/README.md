# surrealkit: Rust library

[![Crates.io](https://img.shields.io/crates/v/surrealkit.svg)](https://crates.io/crates/surrealkit)
[![Documentation](https://docs.rs/surrealkit/badge.svg)](https://docs.rs/surrealkit)
[![License](https://img.shields.io/badge/license-Unlicense-blue.svg)](https://unlicense.org/)

This document covers **SurrealKit as a Rust library**. If you are looking for the CLI, see the [project README](../../README.md).

The library is useful when you want schema management to happen inside your process at startup, for example with an embedded SurrealDB backend (RocksDB, SpeeDB) or when running SurrealDB in the same binary during tests.

## Add to your project

```toml
[dependencies]
surrealkit = { version = "0.5", default-features = false }
```

`default-features = false` skips the CLI dependencies (TLS, file-watching, etc.) and pulls in only the library surface.

---

## Schema sync

Named schema workspaces can be resolved from `surrealkit.toml` and then passed to the sync, rollout, and seed APIs. A schema’s inheritance chain maps to `database/schemas/<name>/` for schema files and `database/seed/<name>/` for seed files.

```rust
let catalog = surrealkit::load_schema_catalog(None)?;
let vars = surrealkit::TemplateVars::default();
let admin = catalog.resolve("admin", "./database", &vars)?;
let cfg = surrealkit::Cfg::from_env(None, &Default::default())?
    .with_target(admin.ns.clone(), admin.db.clone());
let db = surrealkit::connect(&cfg).await?;

surrealkit::sync::run_sync_with_workspace(
    &db,
    &admin.workspace,
    surrealkit::SyncOpts {
        folder: "./database".into(),
        vars,
        ..Default::default()
    },
)
.await?;
```

For tools that need to handle both legacy flat projects and named schemas, resolve `SchemaTarget` values from the catalog. A target exposes the selected namespace/database plus the workspace and seed directories for that mode.

### `embed_schema!` (compile-time embedding)

`embed_schema!` is a proc-macro that walks your `.surql` files at build time and bakes them into the binary. At runtime the generated `embedded_schema::sync` function applies any file whose content has changed, using the same hash-tracking logic as the CLI.

```rust
// Reads database/schema/**/*.surql relative to your Cargo.toml at compile time.
surrealkit::embed_schema!();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = surrealkit::connect(&surrealkit::DbCfg::from_env(None, &Default::default())?).await?;
    embedded_schema::sync(&db).await?;
    Ok(())
}
```

A custom path relative to your `Cargo.toml` can be passed as a string literal:

```rust
surrealkit::embed_schema!("my/schema/dir");
```

The generated module is always named `embedded_schema` regardless of the path argument.

### `run_sync_embedded` (runtime slice)

If you want to construct the schema slice yourself (e.g. for tests or when the SQL comes from another source), use `run_sync_embedded` directly:

```rust
use surrealkit::{EmbeddedSchemaFile, run_sync_embedded};

static SCHEMA: &[EmbeddedSchemaFile] = &[
    EmbeddedSchemaFile {
        path: "database/schema/person.surql",
        sql: "DEFINE TABLE person SCHEMALESS;",
    },
];

run_sync_embedded(&db, SCHEMA).await?;
```

`run_sync_embedded` calls `run_setup` internally, so you do not need to call it separately.

### `run_sync_embedded_with_opts` (full control)

`run_sync_embedded_with_opts` accepts a `SyncOpts` value for fine-grained control:

```rust
use surrealkit::{EmbeddedSchemaFile, SyncOpts, run_sync_embedded_with_opts};

run_sync_embedded_with_opts(
    &db,
    SCHEMA,
    &SyncOpts {
        watch: false,        // ignored for embedded sync
        debounce_ms: 0,
        dry_run: false,
        fail_fast: true,
        prune: true,         // remove DB objects no longer in SCHEMA
        allow_shared_prune: false,
    },
)
.await?;
```

---

## Rollouts

Rollouts can be defined entirely in code. No TOML files or `.surql` files on disk are required.

### Data types

```rust
use surrealkit::{
    RolloutPhase, RolloutSpec, RolloutStep, RolloutStepKind,
    schema_state::EntityKey,
};
```

| Type | Description |
|---|---|
| `RolloutSpec` | The full rollout definition (id, name, steps) |
| `RolloutStep` | One step: phase, kind, inline SQL or file list |
| `RolloutPhase` | `Start`, `Complete`, `Rollback` |
| `RolloutStepKind` | `ApplySchema`, `RemoveEntities`, `RunSql`, `Expect` |
| `EntityKey` | `{ kind, scope, name }` identifying a DB object |

### Full lifecycle example

```rust
use surrealkit::{
    EmbeddedSchemaFile, RolloutPhase, RolloutSpec, RolloutStep, RolloutStepKind,
    run_start_with_spec, run_complete_with_spec,
    schema_state::EntityKey,
};

// The full desired schema after this rollout completes.
static TARGET: &[EmbeddedSchemaFile] = &[
    EmbeddedSchemaFile { path: "database/schema/person.surql",  sql: "DEFINE TABLE person SCHEMALESS;" },
    EmbeddedSchemaFile { path: "database/schema/account.surql", sql: "DEFINE TABLE account SCHEMALESS;" },
];

let spec = RolloutSpec {
    id:   "add_account".to_string(),
    name: "add_account".to_string(),
    source_schema_hash: String::new(),
    target_schema_hash: String::new(),
    compatibility: "phased".to_string(),
    renames: vec![],
    steps: vec![
        // Start phase: apply the new table.
        RolloutStep {
            id:    "apply".to_string(),
            phase: RolloutPhase::Start,
            kind:  RolloutStepKind::ApplySchema,
            sql:   Some("DEFINE TABLE account SCHEMALESS;".to_string()),
            files: vec![],
            expect: None,
            entities: vec![],
            idempotent: None,
        },
        // Rollback phase: undo the start phase if needed.
        RolloutStep {
            id:    "rollback".to_string(),
            phase: RolloutPhase::Rollback,
            kind:  RolloutStepKind::RemoveEntities,
            entities: vec![
                EntityKey { kind: "table".to_string(), scope: None, name: "account".to_string() },
            ],
            files: vec![],
            sql:   None,
            expect: None,
            idempotent: None,
        },
    ],
};

// Apply the start phase. Blocks if another rollout is already active.
run_start_with_spec(&db, &spec, TARGET).await?;

// ... deploy new application code, wait for traffic drain, etc. ...

// Apply the complete phase, marking the rollout done.
run_complete_with_spec(&db, &spec).await?;
```

### Rolling back

Call `run_rollback_with_spec` instead of `run_complete_with_spec` to execute the `Rollback` steps and mark the rollout as rolled back:

```rust
use surrealkit::run_rollback_with_spec;

run_rollback_with_spec(&db, &spec).await?;
```

### Notes

- `spec.id` is the stable key stored in the database. Use a unique, unchanging string per rollout (e.g. a timestamp prefix or migration name). It must be identical across `run_start_with_spec` and `run_complete_with_spec` calls.
- Only one rollout can be active at a time. `run_start_with_spec` returns an error if a different rollout is already in the `running_start` or `ready_to_complete` state.
- Inline SQL (`step.sql`) and file references (`step.files`) are mutually exclusive within one step. Use one or the other.

---

## Seeding

`seed_from_dir` executes `.surql` files from any directory in lexicographic order:

```rust
use surrealkit::seed_from_dir;

seed_from_dir(&db, std::path::Path::new("fixtures/seed")).await?;
```

---

## Connecting

`DbCfg` reads connection details from environment variables (with CLI-argument overrides). `connect` wraps `surrealdb::Surreal` construction and authentication:

```rust
use surrealkit::{DbCfg, DbOverrides, connect};

let cfg = DbCfg::from_env(None, &DbOverrides::default())?;
let db = connect(&cfg).await?;
```

For in-process SurrealDB (e.g. `kv-mem`, `kv-rocksdb`), construct a `surrealdb::Surreal` directly and pass it to any of the library functions:

```rust
use surrealdb::{Surreal, engine::any::connect, opt::Config};
use surrealdb::opt::capabilities::Capabilities;

let db = connect(("mem://", Config::new().capabilities(Capabilities::all()))).await?;
db.use_ns("my_ns").use_db("my_db").await?;

surrealkit::run_sync_embedded(&db, SCHEMA).await?;
```

---

## Metadata tables

SurrealKit creates two internal tables in your configured namespace/database:

| Table | Purpose |
|---|---|
| `__entity` | Tracks every schema object managed by SurrealKit (hash, file key, namespace) |
| `__rollout` | Tracks rollout execution state (`planned`, `running_start`, `ready_to_complete`, `completed`, `rolled_back`) |

These tables are created automatically on the first call to any library function that needs them.
