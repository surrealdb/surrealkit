#!/usr/bin/env bash
# Resuming the same rollout after an interrupted start.
#
# Before #79 `load_active_rollout_id` returned `__rollout:<id>` and compared it
# to the bare id, so a rollout permanently blocked its own retry. The fix has
# never been covered end to end.
. "$(dirname "$0")/ci_lib.sh"
new_workspace resume

kit rollout baseline
cat > "$SURREALDB_FOLDER/schema/003_ledger.surql" <<'SQL'
DEFINE TABLE ledger SCHEMAFULL;
DEFINE FIELD amount ON ledger TYPE number;
SQL
kit rollout plan --name add_ledger
ID="$(manifest_id)"

kit rollout start "$ID"

# Force the record back into the interrupted state a killed process leaves.
sql "UPDATE __rollout SET status = 'running_start' WHERE record::id(id) = '$ID';" >/dev/null
ok "simulated an interrupted start"

out="$("$KIT" rollout start "$ID" 2>&1 || true)"
grep -q 'cannot start while rollout' <<<"$out" \
    && fail "a rollout still blocks its own resume: $out"
ok "the same rollout resumes"

# A *different* rollout must still be blocked while one is active.
sql "UPDATE __rollout SET status = 'running_start' WHERE record::id(id) = '$ID';" >/dev/null
out="$("$KIT" rollout start "19990101000000__other" 2>&1 || true)"
grep -qE 'cannot start while rollout|unable to find rollout' <<<"$out" \
    || fail "a different rollout should not start while one is active: $out"
ok "a different rollout is still refused"
