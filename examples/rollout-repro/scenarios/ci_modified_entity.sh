#!/usr/bin/env bash
# Tightening an ASSERT on an existing field: refused by default, planned with
# --allow-modified, and undone by a rollback that restores the old definition.
. "$(dirname "$0")/ci_lib.sh"
new_workspace modified

# Apply the schema first: `rollout baseline` only records metadata, it does not
# create anything, so without this the "previous" definition would not exist in
# the database for the rollback to capture.
kit sync
kit rollout baseline

# The change under test: one clause, on one field, in one statement.
sed_i 's/ASSERT \\\$value >= 0;/ASSERT \\\$value >= 18;/' "$SURREALDB_FOLDER/schema/001_person.surql"
sed_i 's/ASSERT \$value >= 0;/ASSERT \$value >= 18;/' "$SURREALDB_FOLDER/schema/001_person.surql"
# Refused by default, and the refusal has to be usable: it must name the table
# and the file, not just `field:age`.
out="$("$KIT" rollout plan --name tighten_age --dry-run 2>&1 || true)"
grep -q 'allow-modified' <<<"$out" || fail "the refusal should name the opt-in: $out"
grep -q 'field:person.age' <<<"$out" || fail "the refusal should qualify by table: $out"
ok "refused by default, with an actionable message"

kit rollout plan --name tighten_age --allow-modified
ID="$(manifest_id)"
grep -q 'restore_definitions' "$SURREALDB_FOLDER/rollouts/$ID.toml" \
    || fail "a modified entity needs a restore step, not a remove step"
ok "planned $ID with a restore_definitions rollback step"

kit rollout start "$ID"
sql 'INFO FOR TABLE person;' | grep -q '18' || fail "the tightened ASSERT did not apply"
ok "tightened ASSERT is live"

# reversibility must be surfaced, not implied.
kit rollout status "$ID" | grep -q 'definition_only' \
    || fail "status should report definition_only reversibility"
ok "status reports definition_only"

kit rollout rollback "$ID"
sql 'INFO FOR TABLE person;' | grep -q '>= 0' || fail "rollback did not restore the old ASSERT"
ok "rollback restored the previous definition"

# Resuming an interrupted start must not re-capture the rollback definitions.
#
# `start` is idempotent and re-running it is the documented recovery, but by then
# the expand phase may already have applied. A second capture would read back the
# NEW definitions and overwrite the originals, so rollback would "restore" what is
# already live and report success. Silent, and it only shows up when someone
# actually needs the rollback.
new_workspace modified_resume
kit sync
kit rollout baseline
sed_i 's/ASSERT \$value >= 0;/ASSERT \$value >= 18;/' "$SURREALDB_FOLDER/schema/001_person.surql"
kit rollout plan --name tighten_resume --allow-modified
ID="$(manifest_id)"
kit rollout start "$ID"

captured_first="$(sql "SELECT restore_definitions FROM __rollout WHERE record::id(id) = '$ID';" \
    | jq -r '.[0].result[0].restore_definitions["field:person:age"]')"
grep -q '>= 0' <<<"$captured_first" || fail "first start captured the wrong definition: $captured_first"
ok "first start captured the pre-change definition"

# The interrupted-run state a killed process leaves behind.
sql "UPDATE __rollout SET status = 'running_start' WHERE record::id(id) = '$ID';" >/dev/null
kit rollout start "$ID"

captured_after="$(sql "SELECT restore_definitions FROM __rollout WHERE record::id(id) = '$ID';" \
    | jq -r '.[0].result[0].restore_definitions["field:person:age"]')"
[ "$captured_after" = "$captured_first" ] \
    || fail "resume overwrote the captured definition: $captured_first -> $captured_after"
ok "resume left the captured definition alone"

kit rollout rollback "$ID"
sql 'INFO FOR TABLE person;' | grep -q '>= 0' \
    || fail "rollback after a resume did not restore the original ASSERT"
ok "rollback after a resume still restores the original"
