#!/usr/bin/env bash
# #75: sync regenerates TypeScript types into the configured file.
. "$(dirname "$0")/ci_lib.sh"
new_workspace sync_typegen

printf '%s\n' '[typegen]' 'typescript = "types"' 'filename = "schema.generated.ts"' > surrealkit.toml
kit sync
[ -f types/schema.generated.ts ] || fail "typegen did not write types/schema.generated.ts"
[ -f types/index.ts ] && fail "index.ts must be left to the project"
grep -q "export interface Person" types/schema.generated.ts || fail "unexpected content"
ok "sync writes the configured file name"

printf '%s\n' '[typegen]' 'typescript = "lib/database.ts"' > surrealkit.toml
kit sync
[ -f lib/database.ts ] || fail "typegen did not write lib/database.ts"
ok "sync writes a .ts path"

kit typegen --typescript out/cli.ts >/dev/null
[ -f out/cli.ts ] || fail "--typescript did not write out/cli.ts"
ok "typegen --typescript"

printf '%s\n' '[typegen]' 'json = "out/schema.json"' > surrealkit.toml
kit sync
[ -f out/schema.json ] || fail "sync did not write out/schema.json"
grep -q "\"namespace\": \"$SURREALDB_NAMESPACE\"" out/schema.json \
    || fail "schema.json does not name the session namespace"
ok "sync writes [typegen] json"

rm out/schema.json
kit typegen >/dev/null
[ -f out/schema.json ] || fail "typegen ignored [typegen] json"
ok "typegen writes to [typegen] json"

printf '%s\n' '[typegen]' 'json = "gen"' > surrealkit.toml
kit sync
[ -f gen/schema.json ] || fail "sync did not write gen/schema.json for a directory"
ok "sync writes schema.json into a [typegen] json directory"
