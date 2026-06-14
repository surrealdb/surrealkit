# surrealkit-macros

[![Crates.io](https://img.shields.io/crates/v/surrealkit-macros.svg)](https://crates.io/crates/surrealkit-macros)
[![Documentation](https://docs.rs/surrealkit-macros/badge.svg)](https://docs.rs/surrealkit-macros)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

Proc-macros for [SurrealKit](https://github.com/surrealdb/surrealkit). Embeds `.surql` schema and seed files into your binary at compile time.

---

## Add to your project

This crate is re-exported through the main `surrealkit` crate. You do not normally need to add it directly.

```toml
[dependencies]
surrealkit = { version = "0.6", default-features = false }
```

`default-features = false` omits the CLI dependencies (TLS, file-watching, etc.) and keeps only the library surface.

---

## `embed_schema!`

`embed_schema!` walks your `.surql` files at build time and bakes their contents into the binary via `include_str!`. At runtime the generated module applies any file whose content has changed, using the same hash-tracking logic as the CLI.

### Default path

```rust
// Reads database/schema/**/*.surql relative to your Cargo.toml.
surrealkit::embed_schema!();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = surrealkit::connect(&surrealkit::DbCfg::from_env(None, &Default::default())?).await?;
    embedded_schema::sync(&db).await?;
    Ok(())
}
```

### Custom path

Pass a string literal to use a different directory, still relative to `Cargo.toml`:

```rust
surrealkit::embed_schema!("my/schema/dir");
```

The generated module is always named `embedded_schema` regardless of the path.

---

## `embed_seed!`

`embed_seed!` is the seed-file counterpart of `embed_schema!`. It bakes `.surql` seed files into the binary and generates an `embedded_seed::seed(db)` function. Seeding is tracked in the `__seed` table, so each file runs only on first boot or when its content changes.

```rust
// Reads database/seed/**/*.surql relative to your Cargo.toml.
surrealkit::embed_seed!();
surrealkit::embed_seed!("my/seed/dir"); // custom path

# async fn run(db: &surrealkit::Surreal<surrealkit::engine::any::Any>) -> anyhow::Result<()> {
embedded_seed::seed(db).await?;
# Ok(()) }
```

The generated module is always named `embedded_seed` and exposes a `SEEDS: &[surrealkit::EmbeddedSeedFile]` static plus an async `seed(db)` function.

---

## Generated module

Invoking `embed_schema!` produces:

```rust
pub mod embedded_schema {
    pub static SCHEMA: &[surrealkit::EmbeddedSchemaFile] = &[ /* ... */ ];

    pub async fn sync(
        db: &surrealkit::Surreal<surrealkit::engine::any::Any>,
    ) -> surrealkit::anyhow::Result<()> { /* ... */ }
}
```

| Item | Description |
|------|-------------|
| `SCHEMA` | Slice of `EmbeddedSchemaFile` structs, one per `.surql` file found |
| `EmbeddedSchemaFile.path` | Relative path used as the tracking key in the database |
| `EmbeddedSchemaFile.sql` | File content, embedded at compile time via `include_str!` |
| `sync(db)` | Applies all files to the database; skips files whose hash has not changed |

Files are collected recursively (symlinks followed), filtered to `.surql` only, and sorted alphabetically before embedding.

The macro panics at compile time if the schema directory does not exist.

---

## Template variables

`${VAR_NAME}` tokens in `.surql` files are substituted at runtime by `run_sync_embedded`, not at compile time. The raw SQL (including any tokens) is what gets baked into the binary. See the [template variables](../../README.md#template-variables) section in the project README for resolution order and options.

---

## Full API reference

For `run_sync_embedded`, `run_sync_embedded_with_opts`, rollouts, seeding, and connecting, see [`crates/surrealkit/README.md`](../surrealkit/README.md).
