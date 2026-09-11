# Shared setup for the CI rollout scenarios.
#
# These run natively against a SurrealDB service container on 127.0.0.1:8000,
# not inside the harness image. Each builds a throwaway project in its own
# namespace/database so one failure cannot leak state into the next, and so any
# single scenario is reproducible in isolation.
set -euo pipefail

# Resolved before any `cd`, so scenarios can move around freely.
KIT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)/target/debug/surrealkit"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
[ -x "$KIT" ] || { echo "build surrealkit first: cargo build -p surrealkit --bin surrealkit" >&2; exit 2; }

export SURREALDB_HOST="${SURREALDB_HOST:-ws://127.0.0.1:8000}"
export SURREALDB_USER="${SURREALDB_USER:-root}"
export SURREALDB_PASSWORD="${SURREALDB_PASSWORD:-secret}"
SURREAL_HTTP="${SURREAL_HTTP:-http://127.0.0.1:8000}"

new_workspace() {
    local name="$1"
    WORK="$(mktemp -d)"
    export SURREALDB_FOLDER="$WORK/database"
    export SURREALDB_NAMESPACE="rollout_${name}_ns"
    export SURREALDB_NAME="rollout_${name}_db"
    mkdir -p "$SURREALDB_FOLDER/schema"
    : > "$WORK/surrealkit.toml"
    cp "$REPO"/examples/rollout-repro/project/database/schema/*.surql "$SURREALDB_FOLDER/schema/"
    # Drop any state a previous run left, so a scenario can be re-run directly
    # while debugging without tripping "baseline can only be run once".
    sql "REMOVE DATABASE IF EXISTS \`$SURREALDB_NAME\`;" >/dev/null 2>&1 || true
    cd "$WORK"
}

kit() { "$KIT" "$@"; }

manifest_id() {
    basename "$(ls "$SURREALDB_FOLDER"/rollouts/*.toml | head -1)" .toml
}

sql() {
    curl -sS -X POST "$SURREAL_HTTP/sql" \
        -H "Accept: application/json" \
        -H "surreal-ns: $SURREALDB_NAMESPACE" \
        -H "surreal-db: $SURREALDB_NAME" \
        -u "$SURREALDB_USER:$SURREALDB_PASSWORD" \
        --data-binary "$1"
}

# In-place edit that works on both GNU sed (CI) and BSD sed (macOS, where `-i`
# requires an explicit backup suffix).
sed_i() {
    local expr="$1" file="$2"
    perl -pi -e "$expr" "$file"
}

fail() { printf '\n  FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }
