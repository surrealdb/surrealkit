# Migrating the SurrealKit library API (0.6 → 0.7)

`0.7.0` is a breaking, ergonomics-focused release of the **library** API (the CLI
is unchanged). The goals were to make invalid states unrepresentable, document the
happy path, and shrink the crate-root surface to what library users actually need.
This guide maps every removed or renamed symbol to its replacement.

## Connection

| 0.6 | 0.7 |
|---|---|
| `surrealkit::Cfg` | `surrealkit::DbCfg` |
| `surrealkit::ConfigOverrides` | `surrealkit::DbOverrides` |
| `AuthLevel`, `connect` | unchanged |

## Schema sync

The `run_sync_embedded*` free functions and the public `SyncOpts` are replaced by
the [`Sync`] builder.

```rust,ignore
// 0.6
run_sync_embedded(&db, "database", SCHEMA).await?;
run_sync_embedded_with_opts(&db, "database", SCHEMA, &SyncOpts {
    prune: true, fail_fast: true, vars, ..Default::default()
}).await?;

// 0.7
Sync::embedded(SCHEMA).run(&db).await?;
Sync::embedded(SCHEMA).prune(true).vars(vars).run(&db).await?;
```

- The redundant `folder` argument is gone — embedded sync has no on-disk schema
  folder (`EmbeddedSchemaFile.path` is a tracking key, not a real path).
- `embed_schema!`'s generated `embedded_schema::sync(&db)` is unchanged (it now
  calls `Sync` internally).
- `EmbeddedSchemaFile` is unchanged (now also `Debug`/`Clone`/`Copy`).

## Rollouts

`RolloutStepKind` and the wide `RolloutStep { kind, files, sql, expect, entities,
idempotent }` struct are replaced by a tagged [`RolloutAction`] flattened into
`RolloutStep`, built with constructors. The four `*_with_spec` free functions and
`run_abandon_rollout` are replaced by the [`Rollout`] facade.

```rust,ignore
// 0.6 — hand-built struct with mutually-exclusive optional fields
let spec = RolloutSpec {
    id: "add_account".into(), name: "add_account".into(),
    source_schema_hash: String::new(), target_schema_hash: String::new(),
    compatibility: "phased".into(), renames: vec![],
    steps: vec![RolloutStep {
        id: "apply".into(), phase: RolloutPhase::Start,
        kind: RolloutStepKind::ApplySchema,
        files: vec![], sql: Some("DEFINE TABLE account SCHEMALESS;".into()),
        expect: None, entities: vec![], idempotent: None,
    }],
};
run_start_with_spec(&db, "database", &spec, TARGET, &vars).await?;
run_complete_with_spec(&db, "database", &spec, &vars).await?;
run_rollback_with_spec(&db, "database", &spec, &vars).await?;
run_abandon_rollout(&db, "add_account").await?;

// 0.7 — builder + constructors + facade
let spec = RolloutSpec::builder("add_account")
    .step(RolloutStep::apply_schema("apply", RolloutPhase::Start, "DEFINE TABLE account SCHEMALESS;"))
    .build();
let rollout = Rollout::new(spec, TARGET).vars(vars);
rollout.start(&db).await?;
rollout.complete(&db).await?;     // or rollout.rollback(&db).await?
Rollout::abandon(&db, "add_account").await?;
```

| 0.6 | 0.7 |
|---|---|
| `RolloutStepKind` | removed — folded into [`RolloutAction`] |
| `RolloutStep { kind, files, sql, … }` | `RolloutStep::apply_schema / apply_files / run_sql / assert_sql / remove_entities` |
| `RolloutSpec { compatibility: String, … }` | `RolloutSpec::builder(id)…build()` + [`RolloutCompatibility`] enum |
| `run_start_with_spec` | `Rollout::new(spec, target).start(&db)` |
| `run_complete_with_spec` | `Rollout::new(spec, target).complete(&db)` |
| `run_rollback_with_spec` | `Rollout::new(spec, target).rollback(&db)` |
| `run_abandon_rollout` | `Rollout::abandon(&db, id)` |
| (none) | `Rollout::status(&db)` → `RolloutStatusReport` |

Notes:
- The `idempotent` field is gone. `RunSql` steps are still expected to be
  re-runnable; this is now documented rather than enforced by a flag.
- `RolloutSpec` still exposes `source_schema_hash` / `target_schema_hash` /
  `renames`, but the builder defaults them — they are only used by the CLI's
  filesystem drift detection.

## Entity kinds

`EntityKey.kind` / `CatalogEntity.kind` changed from `String` to the
[`EntityKind`] enum (both are now exported at the crate root).

```rust,ignore
// 0.6
EntityKey { kind: "table".to_string(), scope: None, name: "account".into() }
// 0.7
EntityKey { kind: EntityKind::Table, scope: None, name: "account".into() }
```

`EntityKind` serializes to the same lowercase keywords, so existing catalog
snapshots, rollout TOML, and `__entity` rows remain compatible. Unknown kinds from
newer/foreign state deserialize into `EntityKind::Other(String)`.

## Removed from the crate root

These were CLI internals and are no longer re-exported at `surrealkit::*`. The
filesystem CLI functions still exist under their modules (e.g.
`surrealkit::rollout::run_plan`) but are `#[doc(hidden)]`; the facade/builder above
is the supported library API.

`run_baseline`, `run_plan`, `run_lint`, `run_status`, `run_start`, `run_complete`,
`run_rollback`, `run_repair`, `run_sync`, `run_setup`, `run_typegen`, `build_vars`,
`parse_var_flag`, `seed_from_dir`, `SyncOpts`, `RolloutExecutionOpts`,
`RolloutPlanOpts`.

`seed` (directory seeding) remains exported.

## On-disk rollout manifests

Rollout TOML written by 0.6 for **inline** steps (`kind = "apply_schema"` with
`sql`) still parses. File-based expand steps are now serialized as
`kind = "apply_files"`; regenerate manifests with `surrealkit rollout plan` if you
hand-edited older ones. Finish or `abandon` any in-flight rollout before upgrading.
