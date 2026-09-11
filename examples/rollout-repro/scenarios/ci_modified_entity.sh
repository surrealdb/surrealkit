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
