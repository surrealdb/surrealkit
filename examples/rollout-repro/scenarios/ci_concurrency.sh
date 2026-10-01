#!/usr/bin/env bash
# Two deploys running `rollout up` at once: one runs the rollouts, the other is
# refused, and nothing runs twice.
. "$(dirname "$0")/ci_lib.sh"
new_workspace concurrency
S="$SURREALDB_FOLDER/schema"

kit sync
kit rollout baseline
echo "DEFINE TABLE raced SCHEMALESS;" > "$S/040_raced.surql"
kit rollout plan --name raced
ID="$(manifest_id_named raced)"
cat >> "$SURREALDB_FOLDER/rollouts/$ID.toml" <<'TOML'

[[steps]]
id = "slow"
phase = "start"
kind = "run_sql"
sql = "SLEEP 3s; UPSERT counter:raced SET runs += 1;"
TOML

kit rollout up --complete >/tmp/up1.log 2>&1 & P1=$!
sleep 0.5
kit rollout up --complete >/tmp/up2.log 2>&1 & P2=$!
rc1=0; wait "$P1" || rc1=$?
rc2=0; wait "$P2" || rc2=$?
[ $((rc1 == 0)) -ne $((rc2 == 0)) ] || fail "exactly one run should succeed (rc $rc1 and $rc2)"
cat /tmp/up1.log /tmp/up2.log | grep -q "holds the 'up' lock" || fail "the loser should name the lock"
[ "$(status_of "$ID")" = completed ] || fail "the rollout should be completed"
[ "$(sql_result 'SELECT VALUE runs FROM counter:raced;')" = '[1]' ] || fail "the step ran more than once"
ok "concurrent up: one ran, one was refused, the step ran once"
