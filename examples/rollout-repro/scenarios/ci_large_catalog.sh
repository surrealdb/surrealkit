#!/usr/bin/env bash
# A large entity catalog through the whole lifecycle, and a wiped one through repair.
#
# `complete` used to rewrite the catalog by deleting it and then creating every
# row, in two transactions. With a few hundred entities the second transaction
# failed on SurrealDB Cloud after the first had committed, so the catalog was
# left empty and `complete` and `repair` failed the same way on every retry. The
# write is now a chunked diff that never deletes a row before writing the rest.
. "$(dirname "$0")/ci_lib.sh"
new_workspace large_catalog

catalog_count() {
    sql "RETURN array::len(SELECT VALUE key FROM __entity WHERE ns = 'schema');" \
        | grep -o '"result":[0-9]*' | cut -d: -f2
}

# `[{ key, at }]`, where `at` is the row's updated_at: what a write touched.
catalog_stamps() {
    sql "SELECT key, <string> updated_at AS at FROM __entity WHERE ns = 'schema' ORDER BY key;" \
        | jq -c '.[0].result'
}

# How many rows of stamps $2 are not identical in stamps $1.
rows_written() {
    jq -n --argjson before "$1" --argjson after "$2" \
        '[$after[] | select(. as $row | $before | index([$row]) | not)] | length'
}

# 30 tables of 10 fields and an index each: several chunks' worth of entities.
for t in $(seq -w 1 30); do
    {
        echo "DEFINE TABLE wide_$t SCHEMAFULL;"
        for f in $(seq 1 10); do
            echo "DEFINE FIELD f$f ON wide_$t TYPE option<string>;"
        done
        echo "DEFINE INDEX wide_${t}_f1 ON wide_$t FIELDS f1;"
    } > "$SURREALDB_FOLDER/schema/1${t}_wide.surql"
done

kit rollout baseline
BEFORE="$(catalog_count)"
[ "$BEFORE" -gt 300 ] || fail "baseline recorded $BEFORE entities, expected more than 300"
ok "baseline recorded $BEFORE entities"

cat > "$SURREALDB_FOLDER/schema/900_extra.surql" <<'SQL'
DEFINE TABLE extra SCHEMAFULL;
DEFINE FIELD label ON extra TYPE string;
SQL
kit rollout plan --name add_extra
ID="$(manifest_id)"
kit rollout start "$ID"
UNCHANGED="$(catalog_stamps)"
kit rollout complete "$ID"

AFTER="$(catalog_count)"
[ "$AFTER" = "$((BEFORE + 2))" ] || fail "complete left $AFTER entities, expected $((BEFORE + 2))"
ok "complete recorded all $AFTER entities"

# The two new rows are the only ones `complete` should have written.
WRITTEN="$(rows_written "$UNCHANGED" "$(catalog_stamps)")"
[ "$WRITTEN" = 2 ] || fail "complete wrote $WRITTEN rows for a two-entity change"
ok "complete wrote only the difference"

# The state the old two-transaction write left behind: the catalog deleted, the
# rollout stuck in running_complete.
sql "DELETE __entity WHERE ns = 'schema'; UPDATE __rollout SET status = 'running_complete' WHERE record::id(id) = '$ID';" >/dev/null
[ "$(catalog_count)" = 0 ] || fail "could not simulate the wiped catalog"
kit rollout repair "$ID"
[ "$(catalog_count)" = "$AFTER" ] || fail "repair restored $(catalog_count) of $AFTER entities"
kit rollout status "$ID" | grep -q 'completed' || fail "repair did not complete the rollout"
ok "repair rebuilt a wiped catalog of $AFTER entities"

# Repair over a catalog that is already right must not rewrite it.
sql "UPDATE __rollout SET status = 'running_complete' WHERE record::id(id) = '$ID';" >/dev/null
INTACT="$(catalog_stamps)"
kit rollout repair "$ID"
WRITTEN="$(rows_written "$INTACT" "$(catalog_stamps)")"
[ "$WRITTEN" = 0 ] || fail "repair rewrote $WRITTEN rows of a catalog that was already right"
ok "repair over an intact catalog writes nothing"
