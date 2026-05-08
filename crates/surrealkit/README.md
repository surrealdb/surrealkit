# surrealkit: Rust library

[![Crates.io](https://img.shields.io/crates/v/surrealkit.svg)](https://crates.io/crates/surrealkit)
[![Documentation](https://docs.rs/surrealkit/badge.svg)](https://docs.rs/surrealkit)
[![License](https://img.shields.io/badge/license-Unlicense-blue.svg)](https://unlicense.org/)

This document covers **SurrealKit as a Rust library**. If you are looking for the CLI, see the [project README](../../README.md).

The library is useful when you want schema management to happen inside your process at startup, for example with an embedded SurrealDB backend (RocksDB, SpeeDB) or when running SurrealDB in the same binary during tests.

## Add to your project

```toml
[dependencies]
surrealkit = { version = "0.6", default-features = false }
```

`default-features = false` skips the CLI-only dependencies (TLS, file-watching, etc.) and pulls in only the library surface.

---

## Schema sync

Schema sync is the fast, day-to-day operation: SurrealKit compares the hash of each schema fragment against the hash stored in `__entity` and re-applies any fragment whose content has changed. It is safe to call on every startup.

### `embed_schema!` (compile-time embedding)

`embed_schema!` is a proc-macro that walks your `.surql` files at **build time** and bakes them into the binary. At runtime the generated `embedded_schema::sync` function applies any file whose content has changed, using the same hash-tracking logic as the CLI.

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

If you want to construct the schema slice yourself — for tests or when the SQL comes from another source — use `run_sync_embedded` directly:

```rust
use surrealkit::{EmbeddedSchemaFile, run_sync_embedded};

static SCHEMA: &[EmbeddedSchemaFile] = &[
    EmbeddedSchemaFile {
        // Stable tracking key stored in __entity. Typically the original relative
        // file path, but any unique, consistent string works.
        path: "database/schema/person.surql",
        // The SurrealQL content to apply when this fragment's hash changes.
        sql: "DEFINE TABLE person SCHEMALESS;",
    },
];

run_sync_embedded(&db, SCHEMA).await?;
```

`run_sync_embedded` calls `run_setup` internally, so you do not need to call it separately.

### `run_sync_embedded_with_opts` (full control)

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

Rollouts are the production migration path. They use a two-phase **expand / contract** model:

1. **Start** — additive, non-destructive changes (new tables, new fields). Safe to apply while the old application is still running.
2. **Complete** — destructive cleanup (remove legacy tables/fields). Applied after all application instances are on the new version.
3. **Rollback** — undoes the start phase if something goes wrong before completing.

Rollouts can be defined entirely in code. No TOML files or `.surql` files on disk are required.

### Step types

Each step in a rollout has a `phase` (when it runs) and an `action` (what it does). The `RolloutAction` enum encodes all constraints at the type level — there are no optional fields that silently conflict:

| Action variant | Phase(s) | Purpose |
|---|---|---|
| `ApplySchema { sql }` | Start / Complete | Apply DDL (`DEFINE TABLE`, `DEFINE FIELD`, …). Always idempotent. |
| `RunSql { sql }` | Start / Complete | Execute data-mutation SQL (backfills, …). **Must be written to be safe if re-run.** |
| `AssertSql { sql, expect }` | Start / Complete | Assert a query's output before continuing. |
| `RemoveEntities { entities }` | Complete / Rollback | `REMOVE` named database objects. |

### Full lifecycle example

```rust
use surrealkit::{
    EmbeddedSchemaFile, RolloutCompatibility, RolloutPhase, RolloutSpec, RolloutStep,
    run_start_with_spec, run_complete_with_spec,
    schema_state::EntityKey,
};

// The full desired schema after this rollout completes.
static TARGET: &[EmbeddedSchemaFile] = &[
    EmbeddedSchemaFile {
        path: "database/schema/person.surql",
        sql: "DEFINE TABLE person SCHEMALESS;",
    },
    EmbeddedSchemaFile {
        path: "database/schema/account.surql",
        sql: "DEFINE TABLE account SCHEMALESS;",
    },
];

let spec = RolloutSpec {
    id:   "add_account".to_string(),   // stable key stored in the database
    name: "add_account".to_string(),
    source_schema_hash: String::new(), // leave empty to skip hash verification
    target_schema_hash: String::new(),
    compatibility: RolloutCompatibility::Phased,
    renames: vec![],
    steps: vec![
        // Start phase: apply the new table (non-destructive).
        RolloutStep::apply_schema(
            "apply_account",
            RolloutPhase::Start,
            "DEFINE TABLE account SCHEMALESS;",
        ),
        // Rollback phase: undo the start phase if needed.
        RolloutStep::remove_entities(
            "remove_account",
            RolloutPhase::Rollback,
            vec![EntityKey { kind: "table".to_string(), scope: None, name: "account".to_string() }],
        ),
        // Complete phase: nothing extra needed for a purely additive rollout.
    ],
};

// Apply the start phase. Blocks if another rollout is already active.
run_start_with_spec(&db, &spec, TARGET).await?;

// ... deploy new application code, wait for traffic drain ...

// Apply the complete phase, marking the rollout done.
run_complete_with_spec(&db, &spec).await?;
```

### Rolling back

Call `run_rollback_with_spec` instead of `run_complete_with_spec` to execute the `Rollback` steps and mark the rollout as rolled back:

```rust
use surrealkit::run_rollback_with_spec;

run_rollback_with_spec(&db, &spec).await?;
```

### Data migration example

For a rollout that backfills data alongside a schema change:

```rust
steps: vec![
    RolloutStep::apply_schema(
        "add_tier_field",
        RolloutPhase::Start,
        "DEFINE FIELD tier ON TABLE customer TYPE string DEFAULT 'standard';",
    ),
    RolloutStep::run_sql(
        "backfill_tier",
        RolloutPhase::Start,
        // This must be safe to re-run if the step is retried:
        "UPDATE customer SET tier = 'premium' WHERE plan = 'enterprise';",
    ),
    RolloutStep::remove_entities(
        "rollback_tier",
        RolloutPhase::Rollback,
        vec![EntityKey {
            kind: "field".to_string(),
            scope: Some("customer".to_string()),
            name: "tier".to_string(),
        }],
    ),
],
```

### `RolloutCompatibility`

`RolloutCompatibility::Phased` is currently the only variant. It means the rollout uses the two-phase expand/contract model described above — no schema changes that are destructive to running code happen during `start`.

### Notes

- `spec.id` is the stable key stored in the database. Use a unique, unchanging string per rollout (e.g. a timestamp prefix or a descriptive slug). It must be identical across all calls for the same rollout (`run_start_with_spec`, `run_complete_with_spec`, `run_rollback_with_spec`).
- Only one rollout can be active at a time. `run_start_with_spec` returns an error if a different rollout is already in the `running_start`, `ready_to_complete`, or `failed` state.
- `ApplySchema` steps use `DEFINE … OVERWRITE IF NOT EXISTS` semantics — they are safe to re-apply on retry. `RunSql` steps are your responsibility to write idempotently.

### Recovery from a stuck or failed rollout

If a rollout ends up in `failed` state (e.g. due to a network error mid-execution), it blocks new rollouts from starting. You have three options:

**Option 1 — resume**: Re-run `run_start_with_spec` with the same spec. SurrealKit skips steps that already completed and retries from where it failed.

**Option 2 — rollback**: Call `run_rollback_with_spec` to execute the `Rollback` steps and reach a clean state.

**Option 3 — abandon** (when you cannot resume or roll back):

```rust
use surrealkit::run_abandon_rollout;

// Force-transitions the rollout to rolled_back without running any steps.
// Only use this when you have verified the database is in a consistent state.
run_abandon_rollout(&db, "your_rollout_id").await?;
```

After abandonment, a new rollout can start. `run_abandon_rollout` is a no-op if the rollout is already in a terminal state (`completed` or `rolled_back`).

To inspect rollout state programmatically, query the `__rollout` table:

```rust
// Check which rollout is active and what state it is in.
run_status(&db, None).await?;
// Or for a specific rollout:
run_status(&db, Some("your_rollout_id".to_string())).await?;
```

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
| `__entity` | Tracks every schema object managed by SurrealKit (hash, file key, namespace scope) |
| `__rollout` | Tracks rollout execution state (`planned`, `running_start`, `ready_to_complete`, `completed`, `rolled_back`, `failed`) |

These tables are created automatically on the first call to any library function that needs them.
