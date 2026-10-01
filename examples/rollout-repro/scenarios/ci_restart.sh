#!/usr/bin/env bash
# Rollout state survives the server restarting between phases, and a run killed
# in the middle of a step resumes from where it was. Needs SURREALDB_RESTART (a
# command that restarts an on-disk server); without one it only runs the kill.
. "$(dirname "$0")/ci_lib.sh"
new_workspace restart
S="$SURREALDB_FOLDER/schema"

kit sync
kit rollout baseline
sql "CREATE person:keep SET email = 'keep@example.test', age = 40;" >/dev/null

echo "DEFINE TABLE first SCHEMALESS;" > "$S/030_first.surql"
kit rollout plan --name first
A="$(manifest_id_named first)"
sleep 1
echo "DEFINE TABLE second SCHEMALESS;" > "$S/031_second.surql"
kit rollout plan --name second
B="$(manifest_id_named second)"
kit rollout start "$A"
if restart_server; then
    [ "$(status_of "$A")" = ready_to_complete ] || fail "a restart lost the rollout's state"
    kit rollout complete "$A"
    ok "complete after a restart between start and complete"
else
    kit rollout complete "$A"
fi

kit rollout up --complete
[ "$(status_of "$B")" = completed ] || fail "up should complete $B"
restart_server || true

echo "DEFINE TABLE third SCHEMALESS;" > "$S/032_third.surql"
kit rollout plan --name third
C="$(manifest_id_named third)"
cat >> "$SURREALDB_FOLDER/rollouts/$C.toml" <<'TOML'

[[steps]]
id = "slow"
phase = "start"
kind = "run_sql"
sql = "SLEEP 4s; UPSERT counter:slow SET runs += 1;"
TOML


# Kill `up` in the middle of the slow step.
"$KIT" rollout up --complete & PID=$!  # the binary itself, so the kill reaches it
sleep 2
kill -9 "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
st="$(status_of "$C")"
[ "$st" = running_start ] || fail "the killed run should leave $C in running_start, got $st"
restart_server || true

kit rollout up --complete >/tmp/up.log 2>&1 && fail "the dead run's lock should still be held"
grep -q "holds the 'up' lock" /tmp/up.log || fail "expected the lock message: $(cat /tmp/up.log)"
clear_locks
kit rollout up --complete
[ "$(status_of "$C")" = completed ] || fail "$C should complete after the resume"
ok "a killed run resumes once its lock is cleared, as the message says"

[ "$(sql_result 'SELECT VALUE email FROM person:keep;')" = '["keep@example.test"]' ] || fail "data was lost"
sql 'INFO FOR DB;' | jq -e '.[-1].result.tables | has("first") and has("second") and has("third")' >/dev/null || fail "a table is missing"
ok "schema and data intact"
