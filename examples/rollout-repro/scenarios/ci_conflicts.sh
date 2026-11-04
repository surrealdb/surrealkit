#!/usr/bin/env bash
# Rollout bookkeeping that loses a transaction conflict is retried. A writer
# keeps rewriting the rollout's record and lock row in bursts while `rollout up`
# runs, so the status, step and lock writes conflict now and then. Before the
# retries one lost conflict failed the run, and could leave the lock behind.
. "$(dirname "$0")/ci_lib.sh"
new_workspace conflicts
S="$SURREALDB_FOLDER/schema"

kit sync
kit rollout baseline
for n in 1 2 3; do
    echo "DEFINE TABLE contended_$n SCHEMALESS;" > "$S/05${n}_contended.surql"
    kit rollout plan --name "contended_$n"
    ID="$(manifest_id_named "contended_$n")"
    cat >> "$SURREALDB_FOLDER/rollouts/$ID.toml" <<TOML

[[steps]]
id = "settle_$n"
phase = "start"
kind = "run_sql"
sql = "SLEEP 500ms;"

[[steps]]
id = "contract_$n"
phase = "complete"
kind = "run_sql"
sql = "SLEEP 500ms;"
TOML
done

# Bursts of concurrent writes to the same rows the bookkeeping writes.
contend() {
    while :; do
        for _ in $(seq 1 "${CONTENDERS:-12}"); do
            sql "UPDATE __rollout SET updated_at = time::now(); \
                 UPDATE __entity SET updated_at = time::now() WHERE ns = 'lock';" >/dev/null 2>&1 &
        done
        wait
        sleep 0.2
    done
}
contend & CONTENDER=$!
trap 'kill "$CONTENDER" 2>/dev/null || true' EXIT

rc=0
RUST_LOG="${RUST_LOG:-warn}" kit rollout up --complete >/tmp/conflicts.log 2>&1 || rc=$?
kill "$CONTENDER" 2>/dev/null || true
wait "$CONTENDER" 2>/dev/null || true
cat /tmp/conflicts.log

[ "$rc" -eq 0 ] || fail "rollout up should get through the conflicts (rc $rc)"
for n in 1 2 3; do
    ID="$(manifest_id_named "contended_$n")"
    [ "$(status_of "$ID")" = completed ] || fail "$ID should be completed, is $(status_of "$ID")"
done
[ "$(sql_result "SELECT VALUE key FROM __entity WHERE ns = 'lock';")" = '[]' ] \
    || fail "a lock was left behind: $(sql_result "SELECT key, val FROM __entity WHERE ns = 'lock';")"
if grep -q "transaction conflict on attempt" /tmp/conflicts.log; then
    ok "rollouts completed through $(grep -c 'transaction conflict on attempt' /tmp/conflicts.log) retried conflicts, no lock left"
else
    ok "rollouts completed and no lock was left (no conflict was hit on this run)"
fi
