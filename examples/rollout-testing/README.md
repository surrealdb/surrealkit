# Testing against replayed rollouts

A project whose schema was built up through three rollouts, each planned with
`surrealkit rollout plan` from the one before, starting from an empty schema
folder:

| rollout | what it does |
| --- | --- |
| `create_accounts` | `account` table and the `member` record access |
| `add_notes` | `note` table, owned by the account that writes it |
| `number_notes` | a `note_no` sequence and the `number` field it fills, plus a hand-written `assert_sql` step |

Each manifest's frozen SQL is in the directory beside it.

`database/tests/config.toml` sets `schema_from = "both"`, so `surrealkit test`
runs every suite twice: on a database built by `surrealkit sync` from
`database/schema`, and on one built by replaying the rollouts in order from
empty, the way a production database got its schema. Before either, it checks
that the two databases define the same schema. A schema change nobody planned
a rollout for fails that check.

```bash
surrealkit --folder examples/rollout-testing/database test
surrealkit --folder examples/rollout-testing/database rollout lint
```
