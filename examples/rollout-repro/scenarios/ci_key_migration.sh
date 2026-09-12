#!/usr/bin/env bash
# Upgrading a database that was synced from BOTH environments.
#
# This is the state a real project ends up in after running the same project
# locally and in a container on pre-1.0.0-beta.2: `__entity` holds two spellings
# of every tracked file. The migration has to collapse them onto one canonical
# key without re-applying anything and, critically, without pruning — if the
# suffix match ever failed here, every tracked key would look removed and the
# prune would drop the live schema.
. "$(dirname "$0")/ci_lib.sh"
new_workspace keymigration

kit sync >/dev/null

before="$(sql "SELECT count() FROM __entity WHERE ns = 'schema' GROUP ALL;" \
    | jq -r '.[0].result[0].count')"
[ "$before" -gt 0 ] || fail "expected managed entities after the first sync"
ok "$before managed entities before the upgrade"

# Rewrite the canonical keys into the two legacy spellings the old code wrote:
# `database/schema/x.surql` from a repo root, `/database/schema/x.surql` from a
# container whose working directory was not `/`.
sql "
LET \$rows = (SELECT key, val FROM __entity WHERE ns = 'sync');
FOR \$r IN \$rows {
    CREATE __entity CONTENT { ns: 'sync', key: 'database/' + \$r.key, val: \$r.val, updated_at: time::now() };
    CREATE __entity CONTENT { ns: 'sync', key: '/database/' + \$r.key, val: \$r.val, updated_at: time::now() };
};
DELETE __entity WHERE ns = 'sync' AND string::starts_with(key, 'schema/');" >/dev/null

legacy="$(sql "SELECT count() FROM __entity WHERE ns = 'sync' GROUP ALL;" | jq -r '.[0].result[0].count')"
ok "seeded $legacy legacy keys (two spellings per file)"

out="$(kit sync 2>&1)"
grep -q 're-keyed' <<<"$out" || fail "the migration did not report re-keying: $out"
grep -q 'already in sync' <<<"$out" \
    || fail "files were re-applied; the migration lost their tracked hashes: $out"
ok "re-keyed, and nothing was re-applied"

after_keys="$(sql "SELECT key FROM __entity WHERE ns = 'sync' ORDER BY key;" | jq -r '.[0].result[].key')"
count="$(printf '%s\n' "$after_keys" | grep -c .)"
[ "$count" -eq $((legacy / 2)) ] \
    || fail "expected $((legacy / 2)) canonical keys, got $count: $after_keys"
printf '%s\n' "$after_keys" | grep -qv '^schema/' \
    && fail "a non-canonical key survived: $after_keys"
ok "collapsed to $count canonical keys"

after="$(sql "SELECT count() FROM __entity WHERE ns = 'schema' GROUP ALL;" | jq -r '.[0].result[0].count')"
[ "$after" = "$before" ] || fail "prune ran during migration: $before -> $after managed entities"
ok "nothing pruned ($after managed entities)"

sql 'INFO FOR TABLE person;' | grep -q 'email' || fail "the live schema was damaged"
ok "live schema intact"
