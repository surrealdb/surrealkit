#!/usr/bin/env bash
# Upgrade compatibility: state written by an older SurrealKit, carried on by
# this one.
#
#   OLD_BIN=path/to/old/surrealkit bash examples/rollout-repro/upgrade/upgrade_from.sh
#
# The old binary syncs, baselines, plans and runs rollouts, and leaves one
# rollout in flight. Then this build takes over the same project and database:
# sync stays idle and prunes nothing, the in-flight rollout completes (or rolls
# back), its manifest still lints, and a new frozen rollout runs after it with
# `rollout up`. Connection settings are passed as flags, which every release
# since 0.7.0 accepts; the project folder is the working directory's
# `./database`, because 0.7.0 ignored `--folder` (#76).
#
#   SURREALDB_HOST / SURREAL_HTTP  the server (default 127.0.0.1:8000)
#   NEW_BIN                        this build (default target/debug/surrealkit)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OLD="${OLD_BIN:?set OLD_BIN to the release to upgrade from}"
NEW="${NEW_BIN:-$REPO/target/debug/surrealkit}"
HOST="${SURREALDB_HOST:-ws://127.0.0.1:8000}"
HTTP="${SURREAL_HTTP:-http://127.0.0.1:8000}"
USER_="${SURREALDB_USER:-root}"
PASS_="${SURREALDB_PASSWORD:-secret}"
OLD_VERSION="$("$OLD" --version | awk '{print $2}')"
TAG="$(echo "$OLD_VERSION" | tr -c 'a-z0-9\n' '_')"

fail() { printf '\n  FAIL (from %s): %s\n' "$OLD_VERSION" "$*" >&2; exit 1; }
ok() { printf '  ok (from %s): %s\n' "$OLD_VERSION" "$*"; }

# Unset anything that would make one version read different settings from
# another (1.0 renamed the environment variables and rejects the old ones).
unset SURREALDB_FOLDER SURREALDB_NAMESPACE SURREALDB_NAME DATABASE_HOST DATABASE_NAME \
    DATABASE_NAMESPACE DATABASE_USER DATABASE_PASSWORD DATABASE_FOLDER 2>/dev/null || true

run() {
    local bin="$1" ns="$2"
    shift 2
    "$bin" --host "$HOST" --ns "$ns" --db upgrade --user "$USER_" --pass "$PASS_" "$@"
}
old() { local ns="$1"; shift; run "$OLD" "$ns" "$@"; }
# 1.0.0-beta.1 parsed a rollout id as a --target name, so none of its rollout
# commands that take an id ever worked (fixed in beta.2). Its baseline and plan
# did, so with beta.1 those come from the old release and the rest from this one.
old_rollout_run() {
    local ns="$1"; shift
    if [ "$OLD_VERSION" = "1.0.0-beta.1" ]; then
        run "$NEW" "$ns" "$@"
    else
        run "$OLD" "$ns" "$@"
    fi
}
new() { local ns="$1"; shift; run "$NEW" "$ns" "$@"; }

sql() {
    curl -sS -X POST "$HTTP/sql" -H "Accept: application/json" -H "surreal-ns: $1" -H "surreal-db: upgrade" \
        -u "$USER_:$PASS_" --data-binary "$2" | jq -c '.[-1].result'
}
catalog() { sql "$1" "SELECT VALUE key FROM __entity WHERE ns = 'schema';" | jq -c 'sort'; }
status_of() { sql "$1" "SELECT VALUE status FROM __rollout WHERE record::id(id) = '$2';" | jq -r '.[0] // "none"'; }
id_named() { basename "$(ls database/rollouts/*__"$1".toml | head -1)" .toml; }

fresh() {
    WORK="$(mktemp -d)"
    cd "$WORK"
    mkdir -p database/schema
    cat > database/schema/person.surql <<'SQL'
-- People, with a comment and an IF NOT EXISTS, as real schemas have.
DEFINE TABLE IF NOT EXISTS person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE int DEFAULT 0;
DEFINE INDEX person_name ON person FIELDS name;
SQL
    cat > database/schema/account.surql <<'SQL'
DEFINE TABLE account SCHEMALESS;
DEFINE FUNCTION fn::greet($name: string) { RETURN 'hi ' + $name; };
-- Releases before 1.0.0-beta.6 recorded this as the table `audit.
DEFINE TABLE `audit log` SCHEMALESS;
DEFINE FIELD note ON `audit log` TYPE option<string>;
SQL
    sql "$1" "REMOVE NAMESPACE IF EXISTS $1;" >/dev/null
}

# --- sync ---------------------------------------------------------------------
NS="upgrade_sync_$TAG"
fresh "$NS"
old "$NS" sync >/dev/null 2>&1 || fail "the old release could not sync"
sql "$NS" "CREATE person:ann SET name = 'Ann', age = 30;" >/dev/null
before="$(catalog "$NS")"

sql "$NS" "CREATE \`audit log\`:one SET note = 'kept';" >/dev/null
new "$NS" sync >/tmp/upgrade_sync.log 2>&1 || fail "sync after the upgrade failed: $(cat /tmp/upgrade_sync.log)"
grep -qiE "pruned [1-9]|would prune" /tmp/upgrade_sync.log && fail "the upgrade pruned something"
after="$(catalog "$NS")"
# The only change allowed is the repair of names older releases cut short.
[ "$(echo "$before" | jq 'length')" = "$(echo "$after" | jq 'length')" ] || fail "the catalog changed size: $before -> $after"
echo "$after" | jq -e 'index("table::`audit log`") and index("field:`audit log`:note")' >/dev/null \
    || fail "the quoted names were not repaired: $after"
[ "$(echo "$before" | jq -c '[.[] | select(contains("`audit") | not)]')" = "$(echo "$after" | jq -c '[.[] | select(contains("`audit") | not)]')" ] \
    || fail "the catalog changed on an idle sync: $before -> $after"
[ "$(sql "$NS" "SELECT VALUE name FROM person:ann;")" = '["Ann"]' ] || fail "data was lost"
[ "$(sql "$NS" "SELECT VALUE note FROM \`audit log\`:one;")" = '["kept"]' ] || fail "data in the quoted table was lost"
new "$NS" sync >/tmp/upgrade_sync2.log 2>&1 || fail "a second sync failed: $(cat /tmp/upgrade_sync2.log)"
ok "sync over the old release's state prunes nothing and repairs its catalog"

echo "DEFINE TABLE added SCHEMALESS;" > database/schema/added.surql
new "$NS" sync >/dev/null
catalog "$NS" | jq -e 'index("table::added")' >/dev/null || fail "a new file was not applied after the upgrade"
ok "sync applies changes after the upgrade"

# --- rollouts: completed history and one in flight -----------------------------
NS="upgrade_rollout_$TAG"
fresh "$NS"
old "$NS" sync >/dev/null 2>&1
old "$NS" rollout baseline >/dev/null 2>&1 || fail "the old release could not baseline"
sql "$NS" "CREATE person:bob SET name = 'Bob', age = 41;" >/dev/null

echo "DEFINE TABLE invoice SCHEMALESS;" > database/schema/invoice.surql
old "$NS" rollout plan --name first >/dev/null 2>&1 || fail "the old release could not plan"
FIRST="$(id_named first)"
old_rollout_run "$NS" rollout start "$FIRST" >/dev/null 2>&1 || fail "the old release could not start $FIRST"
old_rollout_run "$NS" rollout complete "$FIRST" >/dev/null 2>&1 || fail "the old release could not complete $FIRST"
sleep 1

echo "DEFINE TABLE receipt SCHEMALESS;" > database/schema/receipt.surql
old "$NS" rollout plan --name second >/dev/null 2>&1
SECOND="$(id_named second)"
old_rollout_run "$NS" rollout start "$SECOND" >/dev/null 2>&1 || fail "the old release could not start $SECOND"
[ "$(status_of "$NS" "$SECOND")" = ready_to_complete ] || fail "setup: $SECOND should be in flight"

new "$NS" rollout status >/tmp/upgrade_status.log 2>&1 || fail "status failed: $(cat /tmp/upgrade_status.log)"
new "$NS" rollout complete "$SECOND" >/tmp/upgrade_complete.log 2>&1 \
    || fail "completing the old release's in-flight rollout failed: $(cat /tmp/upgrade_complete.log)"
[ "$(status_of "$NS" "$SECOND")" = completed ] || fail "$SECOND should be completed"
ok "a rollout the old release started completes after the upgrade"

new "$NS" rollout lint "$SECOND" >/dev/null 2>&1 || fail "the old release's manifest no longer lints"
new "$NS" rollout lint >/tmp/upgrade_lint.log 2>&1 || fail "lint of the old release's chain failed: $(cat /tmp/upgrade_lint.log)"
ok "the old release's manifests still lint, one by one and as a chain"

sleep 1
echo "DEFINE TABLE refund SCHEMALESS;" > database/schema/refund.surql
new "$NS" rollout plan --name third >/dev/null 2>&1 || fail "planning on the old release's snapshots failed"
THIRD="$(id_named third)"
[ -d "database/rollouts/$THIRD" ] || fail "the new plan should be frozen"
new "$NS" rollout up --complete >/tmp/upgrade_up.log 2>&1 || fail "up after the upgrade failed: $(cat /tmp/upgrade_up.log)"
[ "$(status_of "$NS" "$THIRD")" = completed ] || fail "$THIRD should be completed"
expected="$(jq -c '[.entities[] | "\(.kind):\(.scope // ""):\(.name)"] | sort' database/snapshots/catalog_snapshot.json)"
[ "$(catalog "$NS")" = "$expected" ] || fail "catalog $(catalog "$NS") differs from the plan $expected"
[ "$(sql "$NS" "SELECT VALUE name FROM person:bob;")" = '["Bob"]' ] || fail "data was lost"
ok "a new frozen rollout runs after the old release's history with up"

# --- rollouts: an in-flight rollout rolled back after the upgrade --------------
NS="upgrade_rollback_$TAG"
fresh "$NS"
old "$NS" sync >/dev/null 2>&1
old "$NS" rollout baseline >/dev/null 2>&1
echo "DEFINE TABLE invoice SCHEMALESS;" > database/schema/invoice.surql
old "$NS" rollout plan --name only >/dev/null 2>&1
ONLY="$(id_named only)"
old_rollout_run "$NS" rollout start "$ONLY" >/dev/null 2>&1
new "$NS" rollout rollback "$ONLY" >/tmp/upgrade_rollback.log 2>&1 \
    || fail "rolling back the old release's rollout failed: $(cat /tmp/upgrade_rollback.log)"
[ "$(status_of "$NS" "$ONLY")" = rolled_back ] || fail "$ONLY should be rolled back"
sql "$NS" "INFO FOR DB;" | jq -e '.tables | has("invoice") | not' >/dev/null || fail "rollback left the table"
ok "a rollout the old release started rolls back after the upgrade"
