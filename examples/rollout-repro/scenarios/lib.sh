# Shared helpers for the reproduction scenarios.
#
# Each scenario prints exactly one verdict line:
#   REPRO  <bug> <detail>   the buggy behaviour was observed
#   CLEAN  <bug> <detail>   the behaviour is correct
# and exits 0 either way. `run.sh` decides whether that verdict is the expected
# one for the image under test, so a scenario never has to know which build it is
# running against.

set -uo pipefail

BUG="${BUG:-?}"

verdict() { printf '%-6s %-5s %s\n' "$1" "$BUG" "$2"; }
repro() { verdict REPRO "$1"; exit 0; }
clean() { verdict CLEAN "$1"; exit 0; }

# Run surrealkit, capturing stdout+stderr, never failing the script.
kit() { surrealkit "$@" 2>&1 || true; }

# Restore the fixture to its committed state so scenarios can run in any order.
reset_project() {
    local folder="${SURREALDB_FOLDER:-/database}"
    rm -f "$folder"/rollouts/*.toml 2>/dev/null || true
    rm -f "$folder"/snapshots/*.json 2>/dev/null || true
    rm -f "$folder"/setup.surql 2>/dev/null || true
    if [ -f "$folder/schema/001_person.surql" ]; then
        sed -i 's/ASSERT \$value >= 18;/ASSERT $value >= 0;/' \
            "$folder/schema/001_person.surql" 2>/dev/null || true
    fi
}

# The id of the single generated manifest, or empty.
manifest_id() {
    local folder="${SURREALDB_FOLDER:-/database}"
    basename "$(ls "$folder"/rollouts/*.toml 2>/dev/null | head -1)" .toml 2>/dev/null || true
}

# Query the database directly over HTTP.
#
# `surrealkit apply` executes SQL but prints no rows, and the harness needs to
# read `__entity` back to see what keys were written. curl against /sql is the
# smallest dependency that does that.
sql() {
    local endpoint="${SURREAL_HTTP:-http://surrealdb:8000}"
    curl -sS -X POST "$endpoint/sql" \
        -H "Accept: application/json" \
        -H "surreal-ns: ${SURREALDB_NAMESPACE:-repro_ns}" \
        -H "surreal-db: ${SURREALDB_NAME:-repro_db}" \
        -u "${SURREALDB_USER:-root}:${SURREALDB_PASSWORD:-secret}" \
        --data-binary "$1"
}

# The sync tracking keys currently recorded, one per line.
tracked_keys() {
    sql "SELECT key FROM __entity WHERE ns = 'sync' ORDER BY key;" \
        | jq -r '.[0].result[]?.key // empty' 2>/dev/null | sort
}
