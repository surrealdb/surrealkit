#!/usr/bin/env bash
# The #94 class: the connection drops partway through writing state. Whatever
# point it drops at, re-running the documented recovery must converge, the
# catalog must end up complete, and no data may be lost.
#
# A toxiproxy sits between SurrealKit and SurrealDB. For each byte budget in a
# sweep, a `limit_data` toxic closes the connection after that many bytes, so
# successive runs are cut at successively later points of the same operation.
#
#   TOXIPROXY_API       toxiproxy's API (default http://127.0.0.1:8474)
#   TOXIPROXY_UPSTREAM  SurrealDB as toxiproxy reaches it (default surrealdb:8000)
#   TOXIPROXY_LISTEN    the proxy's port, published on 127.0.0.1 (default 28000)
#
# Setup goes straight to SurrealDB; only the operation under test goes through
# the proxy.
. "$(dirname "$0")/ci_lib.sh"

API="${TOXIPROXY_API:-http://127.0.0.1:8474}"
UPSTREAM="${TOXIPROXY_UPSTREAM:-surrealdb:8000}"
LISTEN="${TOXIPROXY_LISTEN:-28000}"
PROXIED="ws://127.0.0.1:$LISTEN"
# From before the first status change, through the steps and the catalog write,
# to after completion (measured against a 3.3 server; the scenario's ok lines
# hold wherever a cut lands).
BUDGETS="${FAULT_BUDGETS:-1500 3000 5000 8000 12000 18000 26000 30000 33000 36000 40000 44000 50000}"
curl -sf "$API/version" >/dev/null || { echo "  skip: no toxiproxy at $API"; exit 0; }

curl -sf -X DELETE "$API/proxies/surreal" >/dev/null 2>&1 || true
curl -sf -X POST "$API/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"surreal\",\"listen\":\"0.0.0.0:$LISTEN\",\"upstream\":\"$UPSTREAM\",\"enabled\":true}" >/dev/null \
    || fail "could not create the proxy"

cut_after() {
    curl -sf -X POST "$API/proxies/surreal/toxics" -H 'Content-Type: application/json' \
        -d "{\"name\":\"cut\",\"type\":\"limit_data\",\"stream\":\"upstream\",\"attributes\":{\"bytes\":$1}}" >/dev/null \
        || fail "could not add the toxic"
}
heal() { curl -sf -X DELETE "$API/proxies/surreal/toxics/cut" >/dev/null 2>&1 || true; }
through_proxy() { SURREALDB_HOST="$PROXIED" "$KIT" "$@"; }

wide_schema() {
    local n="$1" from="${2:-0}"
    rm -f "$SURREALDB_FOLDER"/schema/wide_*.surql
    for i in $(seq "$from" $((from + n - 1))); do
        printf 'DEFINE TABLE wide_%02d SCHEMAFULL;\nDEFINE FIELD v ON wide_%02d TYPE int;\n' "$i" "$i" \
            > "$SURREALDB_FOLDER/schema/wide_$(printf %02d "$i").surql"
    done
}

catalog_rows() {
    sql "SELECT count() FROM __entity WHERE ns = 'schema' GROUP ALL;" | jq '.[-1].result[0].count // 0'
}

catalog_matches_snapshot() {
    local expected actual dups
    expected="$(jq -c '[.entities[] | "\(.kind):\(.scope // ""):\(.name)"] | sort' "$SURREALDB_FOLDER/snapshots/catalog_snapshot.json")"
    actual="$(sql "SELECT VALUE key FROM __entity WHERE ns = 'schema';" | jq -c '.[-1].result | sort')"
    dups="$(sql "SELECT key, count() AS n FROM __entity WHERE ns = 'schema' GROUP BY key;" | jq '[.[-1].result[] | select(.n > 1)] | length')"
    [ "$dups" = 0 ] || fail "$1: duplicate catalog rows"
    [ "$expected" = "$actual" ] || fail "$1: catalog differs from the plan"
}

# --- rollout complete -------------------------------------------------------
for budget in $BUDGETS; do
    new_workspace "faults_complete_$budget"
    wide_schema 10
    kit sync >/dev/null
    kit rollout baseline >/dev/null
    sql "CREATE person:keep SET email = 'keep@example.test', age = 1; CREATE wide_00:keep SET v = 1;" >/dev/null
    wide_schema 40 0
    kit rollout plan --name grow >/dev/null
    ID="$(manifest_id_named grow)"
    kit rollout start "$ID" >/dev/null
    before="$(catalog_rows)"

    cut_after "$budget"
    through_proxy rollout complete "$ID" >/dev/null 2>&1 || true
    heal
    # #94: a failed write emptied the catalog. Wherever the cut lands, no row
    # may go before its replacement is written.
    [ "$(catalog_rows)" -ge "$before" ] || fail "complete cut at ${budget}B shrank the catalog to $(catalog_rows) rows"

    # The recovery an operator follows: clear the dead run's lock, then repair
    # a run stopped while recording, or complete one stopped before that.
    for _ in 1 2 3; do
        case "$(status_of "$ID")" in
            completed) break ;;
            running_complete) clear_locks; kit rollout repair "$ID" >/dev/null ;;
            *) clear_locks; kit rollout complete "$ID" >/dev/null ;;
        esac
    done
    [ "$(status_of "$ID")" = completed ] || fail "complete cut at ${budget}B did not recover"
    catalog_matches_snapshot "complete cut at ${budget}B"
    [ "$(sql_result 'SELECT VALUE v FROM wide_00:keep;')" = '[1]' ] || fail "complete cut at ${budget}B lost data"
done
ok "rollout complete, cut at $(echo "$BUDGETS" | wc -w | tr -d ' ') points: recovery always converges"

# --- sync ---------------------------------------------------------------------
for budget in $BUDGETS; do
    new_workspace "faults_sync_$budget"
    wide_schema 30
    kit sync >/dev/null
    sql "CREATE wide_29:keep SET v = 7;" >/dev/null
    # Drop ten tables and add ten: the sync applies and prunes.
    wide_schema 30 10
    cut_after "$budget"
    through_proxy sync >/dev/null 2>&1 || true
    heal
    clear_locks
    kit sync >/dev/null
    expected="$(cd "$SURREALDB_FOLDER/schema" && ls wide_*.surql | sed 's/\.surql$//' | jq -R . | jq -sc 'sort')"
    actual="$(sql "SELECT VALUE key FROM __entity WHERE ns = 'schema' AND string::starts_with(key, 'table::wide');" \
        | jq -c '[.[-1].result[] | sub("^table::"; "")] | sort')"
    [ "$expected" = "$actual" ] || fail "sync cut at ${budget}B: catalog $actual, files $expected"
    [ "$(sql_result 'SELECT VALUE v FROM wide_29:keep;')" = '[7]' ] || fail "sync cut at ${budget}B lost data"
done
ok "sync, cut at $(echo "$BUDGETS" | wc -w | tr -d ' ') points: a re-sync always converges"
