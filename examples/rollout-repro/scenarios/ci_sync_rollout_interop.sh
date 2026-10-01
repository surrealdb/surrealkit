#!/usr/bin/env bash
# Running sync against a database rollouts manage does not move it along the
# chain: its completed rollouts say where it is, and `up` carries on from there.
. "$(dirname "$0")/ci_lib.sh"
new_workspace sync_interop
S="$SURREALDB_FOLDER/schema"

kit sync
kit rollout baseline
echo "DEFINE TABLE one SCHEMALESS;" > "$S/060_one.surql"
kit rollout plan --name one
ONE="$(manifest_id_named one)"
kit rollout up --complete
sleep 1

echo "DEFINE TABLE two SCHEMALESS;" > "$S/061_two.surql"
kit rollout plan --name two
TWO="$(manifest_id_named two)"
# Someone syncs the folder straight onto the rollout-managed database.
kit sync --allow-shared-prune
kit rollout status | tee /tmp/status.log
grep -q "after rollout '$ONE'" /tmp/status.log || fail "sync should not move the database's position"
grep -q "$TWO" /tmp/status.log || fail "$TWO should still be pending"
kit rollout up --complete
[ "$(status_of "$TWO")" = completed ] || fail "$TWO should complete"
ok "sync on a rollout-managed database leaves the chain alone"
