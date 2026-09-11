#!/usr/bin/env bash
# The rollout happy path, end to end, against a live SurrealDB.
#
# Nothing covered this before: every rollout test called the library functions
# directly, so the CLI surface — where the 1.0.0-beta.1 arg-id regression lived —
# was never exercised against a real database.
. "$(dirname "$0")/ci_lib.sh"
new_workspace lifecycle

kit rollout baseline
ok "baseline"

# A new table: a pure addition, which plan has always supported.
cat > "$SURREALDB_FOLDER/schema/003_invoice.surql" <<'SQL'
DEFINE TABLE invoice SCHEMAFULL;
DEFINE FIELD total ON invoice TYPE number;
SQL

kit rollout plan --name add_invoice
ID="$(manifest_id)"
ok "planned $ID"

kit rollout lint "$ID"
ok "lint"

kit rollout start "$ID"
ok "start"

kit rollout status "$ID" | tee /tmp/status.log
grep -q 'ready_to_complete' /tmp/status.log || fail "status should report ready_to_complete"
ok "status"

kit rollout complete "$ID"
ok "complete"

sql 'INFO FOR DB;' | grep -q 'invoice' || fail "the rollout did not reach the database"
ok "invoice table is live"

# Snapshots advance on complete, not on plan: a plan that is never completed must
# not leave the snapshot claiming the new state.
grep -q '003_invoice' "$SURREALDB_FOLDER/snapshots/schema_snapshot.json" \
    || fail "complete did not advance the schema snapshot"
ok "snapshots advanced on complete"

# And the keys in it are folder-relative, so the file is identical in any
# environment.
grep -q '"schema/003_invoice.surql"' "$SURREALDB_FOLDER/snapshots/schema_snapshot.json" \
    || fail "snapshot keys are not folder-relative"
ok "snapshot keys are folder-relative"
