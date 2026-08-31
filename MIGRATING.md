# Migrating to SurrealKit 1.0

## If you use the CLI: almost nothing to do

Every 0.7 command still works, and with no new flags every command produces the
same output against the same files and the same database metadata. Upgrading and
running `surrealkit sync` on an existing project re-applies nothing and prunes
nothing.

Three things do need attention.

### 1. Move `database/seed.surql`

The single-file seed fallback was deprecated in 0.6 — it printed a warning on
every run saying it would be removed in v1 — and is now gone.

```bash
mkdir -p database/seed
git mv database/seed.surql database/seed/000_init.surql
```

**Seed tracking is keyed by file path**, so the moved file counts as a new one
and will run again on your next `surrealkit seed`. If your seed is not idempotent,
re-key it first instead of letting it re-run:

```surql
UPDATE __seed SET key = 'database/seed/000_init.surql'
  WHERE key = 'database/seed.surql';
```

### 2. Rename `DATABASE_*` environment variables

`DATABASE_HOST`, `DATABASE_NAME`, `DATABASE_NAMESPACE`, `DATABASE_USER`,
`DATABASE_PASSWORD` and `DATABASE_AUTH_LEVEL` are no longer accepted. Rename each
to its `SURREALDB_*` equivalent.

SurrealKit **fails** if it finds one set without its replacement, rather than
ignoring it. That is deliberate: an ignored `DATABASE_HOST` would fall back to
`http://localhost:8000`, so a deployment would quietly connect to the wrong
database instead of failing. Setting both is fine — `SURREALDB_*` wins.

### 3. Check whether you pass `--folder`

`--folder` never actually worked: it was parsed and then discarded, so SurrealKit
always used `SURREALDB_FOLDER` or `./database`. It works now.

If you have been passing `--folder ./db` while SurrealKit was really syncing
`./database`, it will now sync `./db`. If that directory is empty, SurrealKit
refuses rather than pruning your schema:

```
refusing to prune all 14 managed entities: no schema files were found in ./db/schema.
```

Either point `--folder` at the right directory, or drop the flag.

## Opting into multiple schema modules

Adopting modules is additive. Your existing schema stays in the default module
(`<folder>/schema`) with its metadata untouched; new modules live alongside it.

```toml
# surrealkit.toml
[schema.billing]
depends_on = ["core"]
```

```
database/
  schema/                     # the default module — unchanged
  modules/
    billing/
      schema/                 # a named module
```

```bash
surrealkit sync                     # default module, as before
surrealkit sync --schema billing    # just billing
surrealkit sync --all               # every module × every target
```

Named modules are deliberately **not** nested inside `database/schema`: the
default module walks its schema directory recursively, so nesting would make it
collect the named module's files and claim ownership of them.

> **Do not rename a module, or move which module is the default, once it has been
> applied.** A module's identity determines where its metadata lives, so renaming
> presents the whole module as stale and the next sync would drop its database
> objects. Create the new module and migrate deliberately instead.

If you reorganise files *within* a module, note that tracking keys are file
paths: moving `database/schema/user.surql` to a subdirectory makes SurrealKit see
one file removed and one added. Run the first sync after such a move with
`--no-prune` and check `surrealkit status`.

## Adding database targets

```toml
[target.acme]
ns = "acme"
db = "prod"
pass_env = "ACME_DB_PASSWORD"    # never inline a password

[target.globex]
ns = "globex"
db = "prod"
pass_env = "GLOBEX_DB_PASSWORD"
```

```bash
surrealkit sync --target acme
surrealkit sync --all              # every module against every target
surrealkit sync --all --keep-going # don't stop at the first failing target
```

Targets are applied one at a time and there is no cross-database transaction, so
a failing run can leave some targets applied and others not. Every operation is
idempotent, so re-running after a fix is safe.

## If you use the Rust library

### `Rollout` no longer writes to disk

This is the one silent behaviour change for library users.

`Rollout::{start, complete, rollback}` used to default to `./database` and create
`./database/setup.surql` in the caller's working directory. They are now purely
in-database.

```rust
// 0.7: created ./database/setup.surql as a side effect
Rollout::new(spec, files).start(&db).await?;

// 1.0: writes nothing to disk
Rollout::new(spec, files).start(&db).await?;

// 1.0: opt back into the filesystem workflow
Rollout::new(spec, files).folder("database").start(&db).await?;
```

### Applying a named module

```rust
Sync::embedded(BILLING).module("billing")?.run(&db).await?;
```

### Embedding several modules

```rust
surrealkit::embed_schema!(
    core    = "database/modules/core/schema",
    billing = "database/modules/billing/schema",
);

embedded_schema::sync(&db).await?;           // all of them, in declaration order
embedded_schema::billing::sync(&db).await?;  // just one
```

`embedded_schema::sync` exists in both the single- and named-module forms, so
moving from one module to several needs no call-site change. Order the arms so a
module follows the ones it depends on.

`embed_schema!()` and `embed_schema!("dir")` are unchanged.

> `include_str!` makes cargo rebuild when an embedded file *changes*, but the
> directory listing is not tracked, so **adding** a `.surql` file does not trigger
> a rebuild. This is long-standing rather than new. Add a `build.rs` containing
> `println!("cargo:rerun-if-changed=database");`.

### Renamed and removed items

| 0.7 | 1.0 |
|---|---|
| `constants::deprecated_seed_surql_path` | removed |
| `tester::build_filter_input` | `FilterInput::from_opts` |
| `rollout::run_baseline(db, folder)` | `run_baseline(db, folder, &module)` |
| `rollout::run_abandon_rollout(db, id)` | `run_abandon_rollout(db, &module, id)` |
| `SyncOpts { .. }` | gains `module` and `allow_empty_prune` |

## If you use the Vite plugin

- The package now declares `Apache-2.0` (it previously declared `Unlicense`,
  which did not match the repository).
- `@biomejs/biome` moved to `devDependencies`. It was never imported at runtime;
  as a dependency it forced consumers to download the Biome binary. Your lockfile
  will change.
- New `schemas`, `targets` and `all` options map to `--schema`, `--target` and
  `--all`. The default watch globs now cover `database/modules/*/schema`.
