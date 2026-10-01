#!/usr/bin/env bash
# Sync against a live server: create, change, delete with prune, and re-sync as
# a no-op, with `__entity` matching the files every time.
. "$(dirname "$0")/ci_lib.sh"
new_workspace sync_lifecycle
S="$SURREALDB_FOLDER/schema"

catalog() { sql "SELECT VALUE key FROM __entity WHERE ns = 'schema';" | jq -c '.[-1].result | sort'; }

kit sync
[ "$(catalog)" = '["field:account:balance","field:account:owner","field:person:age","field:person:email","table::account","table::person"]' ] \
    || fail "catalog after first sync: $(catalog)"
ok "first sync"

cat > "$S/050_tag.surql" <<'SQL'
DEFINE TABLE tag SCHEMALESS;
DEFINE INDEX tag_name ON tag FIELDS name UNIQUE;
SQL
kit sync
catalog | jq -e 'index("table::tag") and index("index:tag:tag_name")' >/dev/null || fail "new entities not tracked: $(catalog)"
sql "CREATE account:a SET owner = person:x, balance = 5;" >/dev/null
ok "a new file is applied and tracked"

rm "$S/002_account.surql"
kit sync
catalog | jq -e '(index("table::account") | not) and (index("field:account:owner") | not)' >/dev/null || fail "deleted entities still tracked"
sql 'INFO FOR DB;' | jq -e '.[-1].result.tables | has("account") | not' >/dev/null || fail "the deleted table was not pruned"
ok "a deleted file is pruned"

before="$(catalog)"
kit sync 2>&1 | tee /tmp/sync.log
[ "$(catalog)" = "$before" ] || fail "an idle sync changed the catalog"
grep -q "applied" /tmp/sync.log && fail "an idle sync re-applied a file"
ok "an idle sync changes nothing"

kit sync --dry-run >/dev/null
ok "dry run"
