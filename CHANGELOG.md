# Changelog

All notable changes to SurrealKit are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-beta.1]

The headline change is **multi-schema support**: a project can now declare
several named schema modules and several database targets, and apply any
combination of them.

Existing projects are unaffected. With no `--schema`/`--target` flags and no
`[schema.*]`/`[target.*]` config, every command behaves exactly as it did in
0.7 — same output, same on-disk layout, same metadata partitions — so upgrading
an existing database re-applies nothing and prunes nothing.

### Added

- **Named schema modules.** `surrealkit sync --schema billing` applies and prunes
  one module independently of every other. Modules are tracked in their own
  metadata partitions, so two modules can share a database without removing each
  other's objects. Files live in `<folder>/modules/<name>/schema`.
- **Named database targets.** `[target.<name>]` in `surrealkit.toml` plus
  `--target`, for applying schema across several namespaces/databases.
- **`surrealkit sync --all`** — the full (module × target) matrix, with a summary
  table and a non-zero exit if any pair failed.
- **`surrealkit.toml` gains `[schema.<name>]` and `[target.<name>]`.** Schema
  modules support `path` and `depends_on`; targets support
  `host`/`ns`/`db`/`user`/`auth_level`, a `schemas` allow-list, and `primary`.
  Both sections are optional.
- **`depends_on` between modules**, topologically ordered with alphabetical
  tie-breaking. `--schema billing` pulls in `core` first, like `cargo build -p`;
  `--no-deps` opts out. Cycles are detected at load time.
- **`pass_env`** for target credentials. Passwords are read from the named
  environment variable; a literal `pass`/`password` key is rejected.
- **`--keep-going`** continues to the next target after a failure.
- **`--allow-empty-prune`** for the rare case where removing every managed entity
  is genuinely intended.
- **Library:** `Sync::module(name)`, `Rollout::folder(path)`, and the new
  `module`, `project` and `constants::Layout` types.
- **Macros:** a named-module form —
  `embed_schema!(core = "…", billing = "…")` — generating one submodule per
  entry plus a `MODULES` listing and an all-modules `sync`. `embed_seed!` gains
  the same. The pre-1.0 forms are unchanged.
- **Vite plugin:** `schemas`, `targets` and `all` options; the default watch
  globs now cover named modules.
- **A `cli` cargo feature** (on by default, so `cargo install surrealkit` is
  unaffected). `default-features = false` now genuinely drops `clap`, `inquire`,
  `rustls`/`aws-lc-rs` and `tempfile` from a library consumer's dependency tree —
  45 fewer entries — since none are used by the library itself.
- **CI:** the Vite plugin is linted, typechecked and built on pushes and PRs; a
  packaging check guards the published crate's file manifest; and the
  library-only build (`--no-default-features`) is linted so it cannot rot.

### Changed

- **The library no longer prints to stdout/stderr.** Progress now goes through
  the [`log`](https://docs.rs/log) facade, so a library consumer gets silence by
  default instead of unsuppressible console output; install any `log`
  implementation to see it. The CLI installs its own logger and its output is
  unchanged — verified byte-for-byte across every command. `-v/--verbose`, which
  was previously parsed and then ignored, now raises the log level.
- **`surrealkit.toml` is discovered by walking up from the working directory.**
  It was read only from the exact directory, so running from a subdirectory of a
  project silently lost every variable and typegen setting.
- **`acquire_lock` is a real lock.** It previously ran `DELETE` then `CREATE`,
  which unconditionally stole the lock and provided no mutual exclusion at all;
  two concurrent processes both "acquired" successfully. Locks now carry an
  owner and a 15-minute expiry, so a crashed run is taken over rather than
  wedging the project, and releasing requires proof of ownership.
- **`Rollout` no longer writes to the filesystem by default.** It hardcoded
  `./database` and created `./database/setup.surql` in the caller's working
  directory. Call `.folder(path)` to opt into the filesystem workflow. *(Breaking
  for library users.)*
- **`cargo clippy` is enforced in CI** with `-D warnings --all-targets
  --all-features`, so the workspace lint table is no longer decorative.
  `unwrap()`/`expect()` remain allowed in tests.
- Both crates now inherit version and metadata from `[workspace.package]`.
- `main` returns `anyhow::Result`; error output is unchanged.

### Removed

- **`<folder>/seed.surql`.** The single-file seed fallback was deprecated in 0.6
  with a runtime warning promising removal in v1. A missing seed directory is now
  an error. See [MIGRATING.md](MIGRATING.md).
- **`DATABASE_*` environment variables.** They are no longer accepted as aliases
  for `SURREALDB_*` — but they are *rejected*, not ignored: setting one without
  its replacement is an error. Ignoring them would have silently fallen back to
  the defaults and connected to the wrong database.
- `constants::deprecated_seed_surql_path`.
- `tester::build_filter_input` (dead; folded into `FilterInput::from_opts`).

### Fixed

- **`--folder` was silently ignored.** `DbCfg::from_env` resolved the folder
  without consulting the CLI override, so the flag was parsed, stored and then
  discarded. It now takes effect — which means a project that passed `--folder`
  while unknowingly syncing `./database` will start syncing the folder it asked
  for. The empty-prune guard below exists partly to make that safe.
- **A sync that finds no schema files no longer prunes everything.** An empty
  file set made every managed entity look stale; in practice that means the
  folder is wrong, not that the schema was deleted. Pass `--allow-empty-prune` if
  it really was.
- **Rollout metadata writes are scoped.** `replace_managed_entities` and
  `replace_sync_hashes` opened with an unconditional
  `DELETE __entity WHERE ns = 'schema'` / `'sync'`.
- The test runner used `./database` for `setup.surql` while using the configured
  folder for everything else.
- A no-op self-assignment in the test reporter (`report.message =
  format!("{}", report.message)`) and an `unwrap()` after an `is_none()` bail.
- A flaky test: one case invoked a CLI entry point that scaffolds relative to the
  working directory without holding the test lock, leaking files into other
  tests' temporary directories.
- The Vite plugin declared the `Unlicense` while the repository is Apache-2.0,
  shipped its formatter as a runtime dependency, and had a Biome config that
  errored on every invocation and so had never actually run.

## [0.7.0] and earlier

Releases before 1.0.0-beta.1 predate this changelog. See the
[GitHub releases](https://github.com/surrealdb/surrealkit/releases) for their
auto-generated notes.
