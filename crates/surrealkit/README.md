# surrealkit: Rust library

[![Crates.io](https://img.shields.io/crates/v/surrealkit.svg)](https://crates.io/crates/surrealkit)
[![Documentation](https://docs.rs/surrealkit/badge.svg)](https://docs.rs/surrealkit)
[![License](https://img.shields.io/badge/license-Unlicense-blue.svg)](https://unlicense.org/)

This document covers **SurrealKit as a Rust library**. If you are looking for the CLI, see the [project README](../../README.md).

The library is useful when you want schema management to happen inside your process at startup — for example with an embedded SurrealDB backend (RocksDB, SpeeDB) or when running SurrealDB in the same binary during tests.

## Add to your project

```toml
[dependencies]
surrealkit = "0.7"
```

---

## Concepts: sync vs rollout

SurrealKit gives you two ways to get schema into a database. Pick based on whether the database is disposable or shared.

| | **Sync** | **Rollout** |
|---|---|---|
| Mental model | Declarative *desired state* — "make the DB match this schema" | Staged, reviewable *migration* with an explicit undo |
| Applies | All changed files, idempotently | Ordered steps across `start` / `complete` / `rollback` phases |
| Removes objects | Automatically (prune) | Only in the `complete` phase, via explicit steps |
| Reversible | No | Yes (`rollback`) |
| Use when | Dev/test/CI, single-owner or embedded databases | Shared/production databases where you need expand→contract and a rollback path |

The two compose: use **sync** for everyday schema and reach for a **rollout** when a change needs to land safely while old and new code run side-by-side.

---

## Connecting

`DbCfg` reads connection details from environment variables (with optional overrides); `connect` builds the `surrealdb::Surreal` client and authenticates:

```rust,no_run
use surrealkit::{DbCfg, DbOverrides, connect};

# async fn run() -> anyhow::Result<()> {
let cfg = DbCfg::from_env(None, &DbOverrides::default())?;
let db = connect(&cfg).await?;
# Ok(()) }
```

For an in-process SurrealDB (e.g. `mem://`, `rocksdb://`), construct a `Surreal` directly and pass it to any library function:

```rust,no_run
use surrealdb::engine::any::connect;
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

# async fn run() -> anyhow::Result<()> {
let db = connect(("mem://", Config::new().capabilities(Capabilities::all()))).await?;
db.use_ns("my_ns").use_db("my_db").await?;
# Ok(()) }
```

---

## Schema sync

### `embed_schema!` (compile-time embedding)

`embed_schema!` walks your `.surql` files at build time and bakes them into the binary. The generated `embedded_schema::sync` applies any file whose content changed:

```rust,ignore
// Reads database/schema/**/*.surql relative to your Cargo.toml at compile time.
surrealkit::embed_schema!();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = surrealkit::connect(&surrealkit::DbCfg::from_env(None, &Default::default())?).await?;
    embedded_schema::sync(&db).await?;
    Ok(())
}
```

A custom path relative to your `Cargo.toml` may be passed: `embed_schema!("my/schema/dir")`. The generated module is always named `embedded_schema`.

### `Sync` builder (runtime control)

To build the schema slice yourself, or to customize behavior, use the [`Sync`] builder:

```rust,no_run
use surrealkit::{EmbeddedSchemaFile, Sync, Surreal};
use surrealkit::engine::any::Any;

static SCHEMA: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
    path: "database/schema/person.surql",
    sql:  "DEFINE TABLE person SCHEMALESS;",
}];

# async fn run(db: &Surreal<Any>) -> anyhow::Result<()> {
// Defaults: prune = true, fail_fast = true.
Sync::embedded(SCHEMA).run(db).await?;

// Customized:
Sync::embedded(SCHEMA)
    .prune(false)               // don't remove objects missing from SCHEMA
    .allow_all_statements(true) // permit non-DEFINE statements (INSERT/UPDATE/…)
    .dry_run(true)              // report what would change without applying
    .run(db)
    .await?;
# Ok(()) }
```

`Sync` calls setup internally and reads nothing from the filesystem — it never writes scaffolding files.

#### `EmbeddedSchemaFile`: `path` vs `sql`

This trips people up, so to be explicit:

- **`path` is a stable tracking key**, *not* a path that must exist on disk. SurrealKit stores it in its metadata tables to identify the file, detect content changes, and prune files that disappear. **Keep it stable across releases** — renaming it makes SurrealKit treat the old key as deleted and the new one as added.
- **`sql` is the content** that gets applied. Changing `sql` while holding `path` constant is exactly what triggers a re-apply on the next sync.

---

## Rollouts

Rollouts are defined entirely in code — no TOML or `.surql` files on disk required. Build a spec with [`RolloutSpec::builder`] and drive it with the [`Rollout`] facade.

### Status lifecycle

```text
planned → running_start → ready_to_complete → running_complete → completed
                                   │
                                   └── running_rollback → rolled_back
```

`completed` and `rolled_back` are terminal. `failed` and the `running_*` states are stuck states from an interrupted run — recover them with [`Rollout::abandon`] (or the CLI `repair` command). **Only one rollout may be in a non-terminal state at a time.**

### Lifecycle example

```rust,no_run
use surrealkit::{
    Rollout, RolloutSpec, RolloutStep, RolloutPhase, RolloutCompatibility,
    EmbeddedSchemaFile, EntityKey, EntityKind, Surreal,
};
use surrealkit::engine::any::Any;

// The desired schema once the rollout completes (used to compute the managed
// catalog). Pass `&[]` if your steps fully describe the entity changes.
static TARGET: &[EmbeddedSchemaFile] = &[EmbeddedSchemaFile {
    path: "database/schema/account.surql",
    sql:  "DEFINE TABLE account SCHEMAFULL;",
}];

# async fn run(db: &Surreal<Any>) -> anyhow::Result<()> {
let spec = RolloutSpec::builder("20260604__add_account")
    .name("Add account table")
    .compatibility(RolloutCompatibility::Phased)
    // Expand: add the new table (non-destructive).
    .step(RolloutStep::apply_schema(
        "create_account", RolloutPhase::Start,
        "DEFINE TABLE account SCHEMAFULL;",
    ))
    // Backfill during complete. RunSql must be safe to re-run.
    .step(RolloutStep::run_sql(
        "backfill", RolloutPhase::Complete,
        "UPDATE account SET active = true WHERE active = NONE;",
    ))
    // Undo the expand phase on rollback.
    .step(RolloutStep::remove_entities(
        "undo", RolloutPhase::Rollback,
        vec![EntityKey { kind: EntityKind::Table, scope: None, name: "account".into() }],
    ))
    .build();

let rollout = Rollout::new(spec, TARGET);

rollout.start(db).await?;        // expand — blocks if another rollout is active
// ... deploy new code, drain traffic ...
rollout.complete(db).await?;     // contract — or: rollout.rollback(db).await?
# Ok(()) }
```

### Step actions

Each [`RolloutStep`] carries exactly one action, built with a constructor — invalid combinations cannot be represented:

| Constructor | What it does |
|---|---|
| `RolloutStep::apply_schema(id, phase, sql)` | Apply inline DDL (`OVERWRITE` is injected; safe to retry) |
| `RolloutStep::run_sql(id, phase, sql)` | Run data-mutation SQL (must be safe to re-run) |
| `RolloutStep::assert_sql(id, phase, sql, expect)` | Assert a query's output equals `expect` |
| `RolloutStep::remove_entities(id, phase, entities)` | `REMOVE … IF EXISTS` the given objects |

### Recovery / stuck rollouts

If a process dies mid-rollout, the rollout is left in a `running_*` or `failed` state and blocks new rollouts. To inspect and recover:

```rust,no_run
# use surrealkit::{Rollout, RolloutSpec, Surreal};
# use surrealkit::engine::any::Any;
# async fn run(db: &Surreal<Any>, spec: RolloutSpec) -> anyhow::Result<()> {
// Inspect the recorded state.
let rollout = Rollout::new(spec, &[]);
if let Some(report) = rollout.status(db).await? {
    println!("{:?}: {:?}", report.status, report.last_error);
}

// Last resort: force a wedged rollout to `rolled_back` so a new one can start.
// This does NOT revert schema changes already applied — reconcile those with a
// fresh sync or a follow-up rollout.
Rollout::abandon(db, "20260604__add_account").await?;
# Ok(()) }
```

---

## Seeding

[`seed`] runs `.surql` files from a `seed/` directory (lexicographic order), applying template variables:

```rust,no_run
# use surrealkit::{seed, TemplateVars, Surreal};
# use surrealkit::engine::any::Any;
# async fn run(db: &Surreal<Any>) -> anyhow::Result<()> {
seed(db, "database", &TemplateVars::default()).await?;
# Ok(()) }
```

---

## Template variables

`${VAR}` placeholders in schema/seed/rollout SQL are substituted from a [`TemplateVars`] map before execution (lookups are case-insensitive; undefined variables are an error that names the missing key and file). Pass them via `Sync::vars(...)`, `Rollout::vars(...)`, or `seed`.

---

## Metadata tables

SurrealKit maintains two internal tables in your namespace/database, created automatically:

| Table | Purpose |
|---|---|
| `__entity` | Tracks every schema object SurrealKit manages (content hash, tracking key) |
| `__rollout` | Tracks rollout execution state (see the status lifecycle above) |
