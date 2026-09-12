# Migrating to SurrealKit 1.0

## If you use the CLI: almost nothing to do

Every 0.7 command still works against a valid project. With no new flags it
produces the same database metadata, apart from the safety refusal for an empty
filesystem source set described below. Upgrading and running `surrealkit sync`
on an existing project re-applies nothing and prunes nothing.

Seven things do need attention.

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
refuses before opening a database connection:

```
refusing filesystem sync: schema_module=default resolved_schema_dir=./db/schema source_count=0; ...
```

Either point `--folder` at the right directory, drop the flag, or pass
`--allow-empty-prune` when the empty source set is intentional. The preflight
also applies to `--dry-run`, `--no-prune`, and the initial `--watch` sync because
an empty filesystem selection otherwise cannot distinguish intentional absence
from a wrong path.

### 4. Tracked paths are now relative to the project folder

Before 1.0.0-beta.2, SurrealKit keyed tracked schema and seed files by their path
**relative to the process working directory**. The same file therefore had a
different key depending on where you ran the command — `database/schema/001.surql`
from your repo root, but `/database/schema/001.surql` inside a container whose
`WORKDIR` was not `/`.

That single detail caused four separate symptoms: `sync` saw every file as new,
prunes were unsafe, committed snapshots churned in git between a developer
checkout and CI, and a rollout planned locally failed `rollout start` in a
container with `target schema hash mismatch` for byte-identical SQL.

Keys are now relative to the project folder — `schema/001.surql`,
`seed/000_init.surql`, `modules/billing/schema/001.surql` — and are identical in
every environment.

**You do not need to do anything.** The first `surrealkit sync` after upgrading
matches your existing keys by path suffix and rewrites them in place, logging
`re-keyed N tracked file(s)`. `surrealkit seed` does the same for `__seed`.
Snapshot files are rewritten on the next `rollout plan` or `rollout baseline`.

Two things to know:

- **Run `sync` before `seed` once**, on each database, from a checkout where
  every tracked file still exists. Re-keying matches against the files present on
  disk; a file deleted in the same change as the upgrade is treated as removed
  (which it is) and pruned normally.
- **Rollout manifests generated before the upgrade carry an old
  `target_schema_hash`.** They still start, with a warning naming the manifest.
  Re-run `surrealkit rollout plan` to regenerate any manifest you have not yet
  executed. The compatibility fallback is removed in 1.1.0.

If you worked around this by pinning your container's `WORKDIR` to `/`, you can
drop that.

**One new refusal comes with this.** `sync` now stops if none of the file keys it
has tracked match any file it found on disk, because that combination means the
key matcher is broken rather than that you deleted everything, and pruning on it
would drop the live schema. The same refusal fires on a legitimate wholesale
rewrite of your schema files, where every path really did change at once. Pass
`--allow-empty-prune` for that case.

### 5. `rollout` subcommands take a rollout id again

1.0.0-beta.1 added a global `-t/--target` whose argument id collided with the
positional on `rollout start`, `complete`, `rollback`, `status`, `lint` and
`repair`. The rollout id was parsed as a database target name, so every one of
those commands failed with `unknown target "<rollout-id>"` before connecting, and
`--target` was rejected outright on them. **Rollout execution was unusable on
1.0.0-beta.1.** Upgrade.

Two deliberate behaviour changes came with the fix:

- `--target` now actually selects the database for `baseline`, `start`,
  `complete`, `rollback` and `repair`, instead of being accepted and ignored while
  the command ran against the ambient `SURREALDB_*` connection. If you passed
  `--target` to those before, **check which database they were really hitting.**
  Selecting more than one target is refused: a rollout is a locked, resumable,
  per-database state machine, and fanning one id across N databases turns a
  partial failure into N databases in different phases. `rollout status` makes no
  rollout changes and does fan out, though like every command it runs `setup`
  first, which applies the metadata DDL.
- `rollout plan` and `rollout lint` never connect, so `--target`/`--all` cannot
  mean anything to them. They now log a warning saying the flag was ignored,
  rather than accepting it silently. They still exit zero, so a CI wrapper that
  passes the same flags to every subcommand keeps working.

### 6. A connect deadline, on by default

Nothing between the CLI and the database had a timeout. An endpoint that accepted
the connection and never completed the handshake — a database restarting behind a
proxy, a mid-deploy app swap — blocked forever, which in a deploy pipeline looks
like a rollout that hangs until CI kills the job and leaves `__rollout` in an
intermediate state.

Connect and sign-in now have a 30-second budget. Tune it with
`--connect-timeout-secs`, `SURREALDB_CONNECT_TIMEOUT_SECS`, or
`connect_timeout_secs` in a `[target.*]` section; `0` restores the old
wait-forever behaviour.

Step SQL is **not** bounded by default, because a legitimate index build can take
hours. Opt in with `--query-timeout-secs` / `SURREALDB_QUERY_TIMEOUT_SECS` when
you want one. Independently of any timeout, `rollout start`/`complete` now log
each step as it begins and every 15 seconds while it runs, so a slow step is
distinguishable from a hang.

### 7. Static analysis ships in the default build

`surrealkit check`, `generate` and `watch` are new, and the analyzer behind them
is a default feature. Installing from source therefore compiles 18 more crates
(21 counting the platform-specific file-watcher backends), four of which build C:
`tree-sitter` and the three grammars it parses SurrealQL, TypeScript and Svelte
with. That is about 100 seconds of extra compilation and roughly 7.5 MiB of extra
binary — 13.9 MiB to 21.4 MiB on macOS/arm64.

**A C toolchain was already required**, on 1.0.0-beta.2 and before: `surrealdb-core`
pulls in `aws-lc-sys` (via `jsonwebtoken`), plus `blake3`, `lz4-sys` and `ring`,
and `aws-lc-sys` needs `cmake` as well. So this changes how much C is compiled,
not whether any is.

Nothing to do if you install a prebuilt binary — the release artifacts and
`cargo binstall surrealkit` are unaffected. If you install from source and would
rather not build the analyzer, leave it out:

```bash
cargo install surrealkit --no-default-features --features kv-mem,cli
```

That binary has no `check`/`generate`/`watch` subcommands; everything else is
unchanged, and an `[analyze]` section in `surrealkit.toml` still parses, so the
same config file works with either build.

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

## If you use the tester

### Missing paths and headers now fail instead of passing

Through 0.7, an assertion on a JSON path or response header that did **not exist**
silently passed. The check compared "not found" against an unset `exists` field and
matched, so the `equals` / `contains` / `regex` comparison was never reached:

```toml
[[cases.assertions]]
path = "0.owner"     # typo, or a query that matched zero rows
equals = "user:alice"
```

On 0.7 that reported a pass. On 1.0 it fails with `path '0.owner' not found`. The same
applies to `header_assertions` against a header the response never sent.

**If assertions go red on upgrade, they were most likely never being evaluated.** Check
the actual shape of the result before assuming SurrealKit regressed — in this repository
the change surfaced an example suite whose `RELATE` verify query had `in` and `out`
swapped, matched zero rows, and had been green for the whole 0.7 line.

To assert that something is genuinely absent, say so explicitly:

```toml
[[cases.assertions]]
path = "0.secret"
exists = false
```

`exists = false` is the only way to pass on a missing path or header; there is no
suite-level opt-out, and unknown keys in an assertion are rejected at parse time.

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
| `DbOverrides { .. }` | gains `connect_timeout_secs` and `query_timeout_secs` |
| `DbCfg { .. }` | gains `connect_timeout` and `query_timeout` |
| `schema_state::collect_schema_files_at(dir)` | `collect_schema_files_at(root, dir)` |
| `sync::collect_filesystem_schema_files(dir, ..)` | gains a leading `root` |
| `rollout::load_managed_entities(db, module)` | gains a trailing `folder: Option<&str>` |

`SchemaFile.path` changes meaning rather than shape. It was the file's path
relative to the process working directory and is now relative to the project
folder, so a value that read `database/schema/user.surql` now reads
`schema/user.surql`. If you construct `EmbeddedSchemaFile` or `EmbeddedSeedFile`
by hand rather than through `embed_schema!` / `embed_seed!`, use the same
convention or your entries will not match what the CLI tracks for the same files.
The macros were updated to emit it, so regenerating is enough.

## If you use the Vite plugin

- The package now declares `Apache-2.0` (it previously declared `Unlicense`,
  which did not match the repository).
- `@biomejs/biome` moved to `devDependencies`. It was never imported at runtime;
  as a dependency it forced consumers to download the Biome binary. Your lockfile
  will change.
- New `schemas`, `targets` and `all` options map to `--schema`, `--target` and
  `--all`. The default watch globs now cover `database/modules/*/schema`.
