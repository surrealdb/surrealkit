#!/usr/bin/env bash
# The container case: a working directory that is not the project root.
#
# This is the only way to exercise the branch where the old key stripping fell
# through to an absolute path. Keys must be identical to the ones a local run
# produces, and a manifest planned in one environment must verify in the other.
. "$(dirname "$0")/ci_lib.sh"
new_workspace container

# Plan from the project root, the way a developer does.
kit rollout baseline
cat > "$SURREALDB_FOLDER/schema/003_audit.surql" <<'SQL'
DEFINE TABLE audit SCHEMAFULL;
DEFINE FIELD at ON audit TYPE datetime;
SQL
kit rollout plan --name add_audit
ID="$(manifest_id)"

grep -q '"schema/003_audit.surql"' "$SURREALDB_FOLDER/rollouts/$ID.toml" \
    || fail "the manifest should record folder-relative paths"
ok "manifest records folder-relative paths"

# Now act on it from an unrelated working directory with an absolute --folder:
# exactly what a container with a WORKDIR does.
cd /
"$KIT" rollout lint "$ID" --folder "$SURREALDB_FOLDER" \
    || fail "a manifest planned at the project root must lint from elsewhere"
ok "lint succeeds from a different working directory"

"$KIT" rollout start "$ID" --folder "$SURREALDB_FOLDER" \
    || fail "a manifest planned at the project root must start from elsewhere"
ok "start succeeds from a different working directory"

sql 'INFO FOR DB;' | grep -q 'audit' || fail "the apply_files step did not resolve its files"
ok "apply_files resolved its paths against the folder, not the working directory"
