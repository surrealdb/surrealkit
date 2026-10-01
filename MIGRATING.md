# Migrating to SurrealKit 1.0

## If you use the CLI: almost nothing to do

Every 0.7 command still works against a valid project. With no new flags it
produces the same database metadata, apart from the safety refusal for an empty
filesystem source set described below. Upgrading and running `surrealkit sync`
on an existing project re-applies nothing and prunes nothing.

Seven things do need attention, and 1.0.0-beta.6 adds five more (8 to 12).
Upgrading from 0.7.0 or any 1.0 beta carries existing state forward; CI checks
that against every published release.

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

  This needs **1.0.0-beta.3 or later** if the manifest was planned somewhere
  other than where you are running it, which is the normal case when CI or a
  container does the planning. On 1.0.0-beta.2 the fallback could only
  reconstruct the old key spelling from the folder it was configured with, so a
  manifest carrying another machine's paths failed with `target schema hash
  mismatch`, and an `apply_files` step recording an absolute path could not find
  its files. Both are fixed in beta.3, for the CLI and for the `Rollout` builder
  alike; on beta.2 the library path compared hashes directly and had no fallback
  at all.

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

### 7. Finishing a rollout no longer rewrites the whole catalog

Before 1.0.0-beta.4, `rollout complete`, `rollback` and `repair` replaced the
entity catalog (`__entity` rows with `ns = 'schema'`) by deleting all of it and
then creating every row, in two transactions. The second one grew with the whole
schema rather than with the rollout, and on a large schema it could fail after
the first had committed. The catalog was then left empty, the rollout stayed in
`running_complete`, and `repair` failed the same way every time.

The catalog is now brought to its target by writing only what changed, in chunks
of bounded size. Rows are updated in place and stale rows are deleted last, so a
failure at any point leaves every row that was there before. Transaction
conflicts are retried, and the catalog is read back before the rollout is marked
completed. A failed write is recorded in the rollout's `last_error`.

**If a rollout is stuck in `running_complete`** after that failure, upgrade and
run `surrealkit rollout repair <id>`. Repair rebuilds the catalog from the
rollout record, whether it is empty, partly written or intact. If such a rollout
was marked completed by hand while the catalog was still empty, the next
`rollout start` restores it from that rollout's record before it starts, and
logs that it did.

The metadata DDL (your `setup.surql` and the built-in definitions) now runs only
when it has changed since it last ran against that database, or when one of the
metadata indexes is missing. On SurrealDB 3.2 every `DEFINE INDEX OVERWRITE`
rebuilds the index, so each command used to rebuild the unique index the catalog
and the rollout lock rely on. `surrealkit setup` still runs the DDL
unconditionally.

### 8. SurrealDB 3.3 and Rust 1.95 (1.0.0-beta.6)

SurrealKit builds against the SurrealDB 3.3 SDK, so embedded engines
(`mem://`, `surrealkv://`, `rocksdb://`) are 3.3. Servers from 3.2.0 on still
work, and CI tests 3.2.0, 3.2.4 and 3.3.0. Building SurrealKit, or a crate that
depends on it, needs Rust 1.95.

Schema files follow the parser of the server they run on. One difference
matters: 3.3 spells modules `DEFINE MODULE mod::x FROM f"..." UNSIGNED`, where
3.2 takes `AS f"..."`.

### 9. Rollout manifests carry their SQL (1.0.0-beta.6)

`rollout plan` now copies every schema file a rollout changes into a directory
beside the manifest, `rollouts/<id>/`, and records each copy's sha256:

```toml
[[steps]]
id = "apply_expand_schema"
phase = "start"
kind = "apply_files"

[[steps.files]]
path = "schema/user.surql"
hash = "9f2c..."
```

The rollout applies those copies. A database several releases behind can catch
up with `surrealkit rollout up`, which runs every rollout it has not run, in
order, each applying what was planned (#91). What changes for you:

- **Commit and ship `rollouts/<id>/` with the manifest.** A deploy image that
  copies only `rollouts/*.toml` fails with the missing file's path.
- **Frozen files are not to be edited.** A changed file fails its hash check;
  plan a new rollout instead. On Windows checkouts, add
  `database/rollouts/** -text` to `.gitattributes` so git leaves their line
  endings alone.
- **`rollout start` enforces order** for a frozen rollout: it refuses one whose
  predecessors have not run here, and names them. It no longer checks the
  schema folder, which may have moved on.
- **Two plans from the same snapshots are a fork.** Two branches that each ran
  `plan` produce two rollouts from one schema; `rollout lint` (with no id, which
  now checks every manifest) fails on that. Delete the later one and plan again
  after merging.
- **`rollout lint` with no id** checks the whole directory, including schema
  changes no rollout plans yet. It needs no database, so it suits CI.

Manifests planned before beta.6 still work as before: they run against the
schema they were planned from, and `rollout start`/`complete` treat them as they
always have. `up` runs one only as the last pending rollout, with the schema
folder matching it. To let a database catch up across older manifests, check
out each one's planning commit and run `surrealkit rollout freeze <id>`, which
converts it in place, keeping its comments, and commit the result. Freezing
changes a manifest's checksum, so finish any rollout that has started somewhere
before freezing it.

A database that already skipped releases on an earlier beta, running only the
newest manifest, is reported by `rollout status` and `rollout up`. They list the
rollouts it moved past, whose data steps never ran there. Run those steps by
hand if they still matter.

### 10. Sequences are never overwritten implicitly (1.0.0-beta.6)

Sync and rollouts used to apply every `DEFINE` as `OVERWRITE`, including
`DEFINE SEQUENCE ... IF NOT EXISTS`. For a sequence that resets its counter to
`START`: immediately on SurrealDB 3.3, after a restart on 3.2. New records then
collide with existing ones (#93). Now:

- a plain `DEFINE SEQUENCE` is applied as `IF NOT EXISTS`;
- an explicit `IF NOT EXISTS` is kept;
- an explicit `OVERWRITE` is applied as written, with a warning each time.

So a changed sequence definition (a new `START` or `BATCH`) no longer reaches a
database where the sequence exists. Sync warns when it sees one. Reposition a
sequence on purpose, in a rollout `run_sql` step or by hand. `rollout plan` no
longer asks for `--allow-modified` over such a change, and rollback no longer
restores sequences, users or access methods (their `INFO` text is either not
their state or has its secrets redacted).

SurrealKit also warns when it overwrites a record `DEFINE ACCESS` that has no
`WITH JWT ... KEY`: SurrealDB gives it a new random key each time, which signs
every user out.

### 11. Schema files are read with a real tokenizer (1.0.0-beta.6)

The statement splitter did not know about regex literals, so a `;`, `#` or quote
inside one broke the file (#92). It also split names on whitespace, so
`` DEFINE TABLE `audit log` `` was tracked as the table `` `audit ``. The new
scanner reads SurrealQL the way SurrealDB does. Expect:

- files that failed to sync over a regex now sync;
- a file SurrealKit cannot read now fails before anything is applied, naming
  the file, line and column, where it used to glue statements together and
  could then prune the ones it lost;
- `DEFINE` followed by a newline or comment before the kind is accepted;
- names cut short by the old splitter are repaired in the catalog on the next
  sync, `rollout plan` or `rollout up`, with a warning. Nothing is removed for
  them.

Entity names and statement hashes are otherwise unchanged, so the first sync
after upgrading re-applies nothing.

### 12. TypeScript output file (1.0.0-beta.6)

Nothing to do. `[typegen]` takes a `filename` for the file inside the
`typescript` directory (default `index.ts`), or `typescript` can name the file
itself, e.g. `typescript = "src/types/database.ts"` (#75).
`surrealkit typegen --typescript <path>` overrides it for one run.

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

### 1.0.0-beta.6 additions

Nothing to change; all additive:

- `[[cases.rules]]` takes a `name`, shown in the report.
- `record_id = "$auth"` targets the record a record actor signed in as, and
  update and delete rules on it act on that record (then put it back).
- `equals_auth = "$auth.id"` now resolves for a record user; before, it never
  could.
- `schema_from = "sync" | "rollouts" | "both"` (in `[defaults]` or per suite,
  or `--schema-from`) builds suite databases from replayed rollouts, with a
  parity check against sync.

One behaviour change: a TOML date or time without an offset in
`signup_params`/`signin_params` (`dob = 1979-05-27`) stays a string. SurrealDB
3.3's parser accepts those as datetimes and 3.2's did not, so without this they
would have changed type on upgrade.

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
| `schema_state::verify_schema_hash(..)` | gains a trailing `legacy_prefixes: &[String]` (beta.3) |
| `schema_state::legacy_schema_hashes(..)` | gains a trailing `legacy_prefixes: &[String]` (beta.3) |

From 1.0.0-beta.6:

| 1.0.0-beta.5 | 1.0.0-beta.6 |
|---|---|
| `RolloutAction::ApplyFiles { files: Vec<String> }` | `files: Vec<FileRef>`; `FileRef::Path(String)` is the old form |
| `RolloutStep::apply_files(id, phase, Vec<String>)` | takes any `IntoIterator` of `Into<FileRef>`, so existing calls compile |
| `schema_state::ensure_overwrite(sql) -> String` | deprecated; `prepare_schema_sql(sql) -> Result<PreparedSql>` returns warnings and errors |
| `tester::TestOpts { .. }` | gains `schema_from` |
| `variables::TypegenConfig { .. }` | gains `filename`; `typescript_path()` resolves the file |
| `typegen::write_typescript(doc, dir)` | takes a directory (as before) or a `.ts` file |
| `rollout::run_lint(folder, opts)` | `opts.selector = None` lints every manifest instead of erroring |
| `rollout::LoadedRolloutSpec { .. }` | gains `frozen_root` |

New: `Rollouts` (run a project's rollouts in order), `FileRef`, `FrozenFile`,
`RolloutChainReport`, `UpReport`, `schema_state::{prepare_schema_sql, PreparedSql,
PrepareWarning}`, `tester::SchemaSource`, `typegen::typescript_file`.

`Rollout::new(spec, files)` runs a frozen spec (one with `FrozenFile` entries)
only with `.folder(..)`, which says where `rollouts/<id>/` is. Without it, start
fails before recording anything.

Those last two are internal plumbing for the pre-1.0.0-beta.2 manifest fallback
and are now `#[doc(hidden)]`. They were added in beta.2 and the fallback goes in
1.1.0, so neither is intended to be called directly. Pass the prefixes from
`schema_state::canonicalise_recorded_path` if you do.

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

### 0.2.0 requires Vite 8

`vite-plugin-surrealkit@0.2.0` narrows its peer range to `vite@^8` and raises
its Node floor to `^20.19.0 || >=22.12.0`, matching Vite 8's own `engines`.
Staying on Vite 7 means staying on `0.1.x`.

**Do not run `0.1.x` on Vite 8.** It passed raw globs to Vite's file watcher.
Vite builds that watcher with chokidar globbing disabled, so each glob was
registered as a literal path that never exists — and on Vite 8 that suppresses
change events for the real files beside it. The result is a dev server that
runs the startup sync and then silently never syncs again. `0.2.0` registers
the globs' base directories instead, and the behaviour is now covered by tests
that boot a real dev server.

Two smaller fixes ship with it: teardown moved from `httpServer`'s `close`
event (always `null` in middleware mode, so nothing was ever cleaned up) to the
`closeBundle` hook, and the plugin no longer imports anything from `vite` at
runtime — only its types.
