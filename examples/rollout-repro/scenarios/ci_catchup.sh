#!/usr/bin/env bash
# #91: a database several releases behind catches up through every rollout it
# missed, in order, each applying exactly what was planned.
#
# The database is baselined at v1. Three releases each plan a rollout: a field
# with a hand-written backfill and assertion, a table removal (complete phase),
# and a new table with an index.
# Then the schema folder moves on without planning anything. `rollout up` must
# run the three rollouts as planned and ignore the unplanned change.
. "$(dirname "$0")/ci_lib.sh"
new_workspace catchup
S="$SURREALDB_FOLDER/schema"

write_v1() {
    rm -f "$S"/*.surql
    printf '%s\n' 'DEFINE TABLE person SCHEMAFULL;' 'DEFINE FIELD name ON person TYPE string;' \
        'DEFINE FIELD age ON person TYPE int;' > "$S/001_person.surql"
    printf '%s\n' 'DEFINE TABLE account SCHEMALESS;' > "$S/002_account.surql"
}
write_v1

kit sync
kit rollout baseline
sql "CREATE person:ann SET name = 'Ann', age = 30; CREATE person:bob SET name = 'Bob', age = 41;" >/dev/null
ok "v1 synced, baselined, with data"

# v2: a field, its backfill, and a check that the backfill ran.
cat > "$S/010_email.surql" <<'SQL'
DEFINE FIELD email ON person TYPE option<string>;
SQL
kit rollout plan --name v2_email
V2="$(manifest_id_named v2_email)"
cat >> "$SURREALDB_FOLDER/rollouts/$V2.toml" <<'TOML'

[[steps]]
id = "backfill_email"
phase = "start"
kind = "run_sql"
sql = "UPDATE person SET email = string::lowercase(name) + '@example.test' WHERE email = NONE;"

[[steps]]
id = "everyone_has_email"
phase = "start"
kind = "assert_sql"
sql = "RETURN count(SELECT * FROM person WHERE email = NONE)"
expect = "0"
TOML
sleep 1

# v3: a removed table, which the complete phase drops.
rm "$S/002_account.surql"
kit rollout plan --name v3_drop_account
V3="$(manifest_id_named v3_drop_account)"
sleep 1

# v4: a new table and an index.
cat > "$S/011_audit.surql" <<'SQL'
DEFINE TABLE audit SCHEMALESS;
DEFINE INDEX person_email ON person FIELDS email;
SQL
kit rollout plan --name v4_audit
V4="$(manifest_id_named v4_audit)"
ok "planned $V2, $V3, $V4"

ls "$SURREALDB_FOLDER/rollouts/$V2/schema/010_email.surql" >/dev/null || fail "plan did not freeze 010_email.surql"
kit rollout lint
ok "lint of the whole chain passes"

# The folder moves on: an unplanned table, and v2's file disappears.
echo "DEFINE TABLE unplanned SCHEMALESS;" > "$S/013_unplanned.surql"
rm "$S/010_email.surql"
kit rollout lint >/tmp/lint.log 2>&1 && fail "lint should fail on unplanned changes"
grep -q "no rollout plans" /tmp/lint.log || fail "lint did not name the unplanned changes: $(cat /tmp/lint.log)"
ok "lint fails on schema changes no rollout plans"

kit rollout status | tee /tmp/status.log
grep -q "$V2" /tmp/status.log && grep -q "pending, in order" /tmp/status.log || fail "status should list the pending rollouts"
ok "status lists the pending chain"

kit rollout start "$V4" >/tmp/start.log 2>&1 && fail "starting v4 before v2 and v3 must be refused"
grep -q "rollout up" /tmp/start.log || fail "the refusal should point at rollout up: $(cat /tmp/start.log)"
ok "out-of-order start is refused"

kit rollout up
[ "$(status_of "$V2")" = completed ] || fail "v2 should be completed"
[ "$(status_of "$V3")" = completed ] || fail "v3 should be completed"
[ "$(status_of "$V4")" = ready_to_complete ] || fail "v4 should wait at ready_to_complete"
ok "up: v2 and v3 completed, v4 waiting for cutover"

[ "$(sql_result "SELECT VALUE email FROM person ORDER BY id;")" = '["ann@example.test","bob@example.test"]' ] \
    || fail "v2's backfill did not run: $(sql_result 'SELECT VALUE email FROM person ORDER BY id;')"
sql 'INFO FOR DB;' | jq -e '.[-1].result.tables | has("audit") and (has("account") | not) and (has("unplanned") | not)' >/dev/null \
    || fail "tables after v3 are wrong: $(sql_result 'INFO FOR DB;' | jq -c '.tables | keys')"
sql 'INFO FOR TABLE person;' | jq -e '.[-1].result.indexes | has("person_email")' >/dev/null || fail "v4's index is missing"
ok "every rollout applied what it planned, and nothing it did not"

kit rollout up | tee /tmp/up2.log
[ "$(status_of "$V4")" = ready_to_complete ] || fail "a second up must not complete the newest"
ok "a second up leaves the newest waiting"

kit rollout up --complete
[ "$(status_of "$V4")" = completed ] || fail "up --complete should complete v4"
expected="$(jq -c '[.entities[] | "\(.kind):\(.scope // ""):\(.name)"] | sort' "$SURREALDB_FOLDER/snapshots/catalog_snapshot.json")"
actual="$(sql "SELECT VALUE key FROM __entity WHERE ns = 'schema';" | jq -c '.[-1].result | sort')"
[ "$expected" = "$actual" ] || fail "catalog differs from the plan: expected $expected, got $actual"
ok "the recorded catalog is exactly what v4 was planned to leave"

# A tampered frozen file stops the next database before anything starts. Build
# another v1 database: put v1 back on disk to sync and baseline it, then restore
# the folder (baseline rewrites the snapshots too).
export SURREALDB_NAME="rollout_catchup_tampered_db"
sql "REMOVE DATABASE IF EXISTS rollout_catchup_tampered_db;" >/dev/null
SAVED="$(mktemp -d)"
cp -R "$S" "$SURREALDB_FOLDER/snapshots" "$SAVED/"
write_v1
kit sync >/dev/null
kit rollout baseline >/dev/null
rm -rf "$S" "$SURREALDB_FOLDER/snapshots"
cp -R "$SAVED/schema" "$SAVED/snapshots" "$SURREALDB_FOLDER/"
echo "DEFINE TABLE hijacked;" > "$SURREALDB_FOLDER/rollouts/$V4/schema/011_audit.surql"
kit rollout up >/tmp/tampered.log 2>&1 && fail "a tampered frozen file must stop up"
grep -q "does not match its manifest" /tmp/tampered.log || fail "unexpected error: $(cat /tmp/tampered.log)"
[ "$(sql_result 'SELECT VALUE record::id(id) FROM __rollout;')" = '[]' ] || fail "no rollout may start"
ok "a tampered frozen file stops up before any rollout starts"
