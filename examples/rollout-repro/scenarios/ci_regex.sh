#!/usr/bin/env bash
# #92: a regex literal in a schema file. The old splitter read the `;` in the
# character class as a statement end and the `#` as a comment, failed the file
# as "a non-DEFINE statement", and with a lone quote could glue the statements
# after it together, which sync then pruned.
. "$(dirname "$0")/ci_lib.sh"
new_workspace regex
S="$SURREALDB_FOLDER/schema"

cat > "$S/020_utils.surql" <<'SQL'
DEFINE PARAM OVERWRITE $PRE_FILTER VALUE /[ \-_().,\\\/$&+,:;=?@#|'<>^*%!]/;
DEFINE FUNCTION OVERWRITE fn::preFilter($str: option<string>) {
	RETURN IF $str {
		string::replace($str, $PRE_FILTER, '')
	};
};
DEFINE TABLE after_regex SCHEMALESS;
SQL

kit sync
[ "$(sql_result "RETURN fn::preFilter('a-b c#d;e');")" = '"abcde"' ] || fail "the regex param or function is wrong"
sql 'INFO FOR DB;' | jq -e '.[-1].result.tables | has("after_regex")' >/dev/null || fail "the table after the regex was lost"
ok "the #92 file syncs, and the statements after the regex survive"

echo "-- touched" >> "$S/020_utils.surql"
kit sync
sql 'INFO FOR DB;' | jq -e '.[-1].result.tables | has("after_regex") and has("person")' >/dev/null || fail "a re-sync pruned something"
ok "re-sync prunes nothing"

# A file the scanner cannot read stops sync before it applies or prunes.
cat > "$S/021_broken.surql" <<'SQL'
DEFINE PARAM $x VALUE 'never closed;
DEFINE TABLE swallowed;
SQL
kit sync >/tmp/sync.log 2>&1 && fail "an unterminated string must stop sync"
grep -q "021_broken.surql" /tmp/sync.log && grep -q "line 1" /tmp/sync.log || fail "the error should name the file and line: $(cat /tmp/sync.log)"
sql 'INFO FOR DB;' | jq -e '.[-1].result.tables | has("after_regex") and has("person") and (has("swallowed") | not)' >/dev/null \
    || fail "a failed scan must change nothing"
rm "$S/021_broken.surql"
ok "an unreadable file stops sync with its line, and changes nothing"

# Through a rollout, on a database baselined before the regex existed.
export SURREALDB_NAME="rollout_regex_rollout_db"
sql "REMOVE DATABASE IF EXISTS rollout_regex_rollout_db;" >/dev/null
mv "$S/020_utils.surql" "$WORK/020_utils.surql"
kit sync >/dev/null
kit rollout baseline
mv "$WORK/020_utils.surql" "$S/020_utils.surql"
kit rollout plan --name add_prefilter
kit rollout up --complete
[ "$(sql_result "RETURN fn::preFilter('x/y z');")" = '"xyz"' ] || fail "the rollout did not apply the regex intact"
ok "a rollout applies the regex intact"
