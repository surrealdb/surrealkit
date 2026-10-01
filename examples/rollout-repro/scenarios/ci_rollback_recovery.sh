#!/usr/bin/env bash
# After a rollback the chain must not be stuck. A rolled-back rollout can be
# run again by name, or discarded (manifest, directory and snapshots) and
# planned again. Both paths come from review on #97.
. "$(dirname "$0")/ci_lib.sh"
new_workspace rollback_recovery
S="$SURREALDB_FOLDER/schema"

kit sync
kit rollout baseline
sed_i 's/ASSERT \$value >= 0;/ASSERT \$value >= 18;/' "$S/001_person.surql"
kit rollout plan --name tighten_age --allow-modified
X="$(manifest_id_named tighten_age)"
kit rollout start "$X"
kit rollout rollback "$X"
[ "$(status_of "$X")" = rolled_back ] || fail "setup: $X should be rolled back"

kit rollout status | tee /tmp/status.log
grep -q "rolled back: $X" /tmp/status.log || fail "status should say $X was rolled back"
grep -q "could not work out" /tmp/status.log && fail "status should place the database"
kit rollout up >/tmp/up.log 2>&1 && fail "up must not re-run a rolled-back rollout on its own"
grep -q "rollout start $X" /tmp/up.log && grep -q "rollout discard $X" /tmp/up.log || fail "up should offer both remedies: $(cat /tmp/up.log)"
grep -q "planned after it" /tmp/up.log && fail "nothing was planned after $X"
ok "status and up explain the rolled-back rollout"

# Remedy one: run it again by name.
kit rollout start "$X"
kit rollout status | tee /tmp/status.log
grep -q "in flight: $X (ready_to_complete)" /tmp/status.log || fail "status should show $X in flight"
kit rollout up --complete
[ "$(status_of "$X")" = completed ] || fail "$X should complete after running again"
ok "a rolled-back rollout runs again when started by name"

# Remedy two: roll the next one back and discard it.
echo "DEFINE TABLE extra SCHEMALESS;" > "$S/090_extra.surql"
cp "$SURREALDB_FOLDER/snapshots/schema_snapshot.json" "$WORK/schema_before.json"
kit rollout plan --name add_extra
Y="$(manifest_id_named add_extra)"
kit rollout start "$Y"
kit rollout rollback "$Y"
kit rollout discard "$Y"
[ ! -e "$SURREALDB_FOLDER/rollouts/$Y.toml" ] && [ ! -e "$SURREALDB_FOLDER/rollouts/$Y" ] || fail "discard left $Y behind"
cmp -s "$WORK/schema_before.json" "$SURREALDB_FOLDER/snapshots/schema_snapshot.json" || fail "discard did not put the snapshots back"
kit rollout plan --name add_extra_again
Z="$(manifest_id_named add_extra_again)"
grep -q '090_extra.surql' "$SURREALDB_FOLDER/rollouts/$Z.toml" || fail "the new plan should pick the change up again"
kit rollout lint
kit rollout up --complete
[ "$(status_of "$Z")" = completed ] || fail "$Z should complete"
ok "a discarded rollout is planned again cleanly"
