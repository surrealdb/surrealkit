#!/usr/bin/env bash
# #93: re-applying a schema file must never rewind a sequence.
#
# SurrealKit used to send every DEFINE as OVERWRITE, including
# `DEFINE SEQUENCE ... IF NOT EXISTS`. On 3.3 that rewinds a live counter
# immediately; on 3.2 it does after a restart. Each check below hands out ids,
# re-applies the file through sync or a rollout, and takes the next id.
#
# With SURREALDB_RESTART set (a command that restarts an on-disk server), it
# also restarts the server before re-applying, which is how 3.2 shows the bug.
. "$(dirname "$0")/ci_lib.sh"
new_workspace sequences
S="$SURREALDB_FOLDER/schema"
rm -f "$S"/*.surql

write_schema() {
    cat > "$S/001_orders.surql" <<SQL
$1
DEFINE TABLE orders SCHEMAFULL;
DEFINE FIELD n ON orders TYPE int DEFAULT sequence::nextval('order_no') READONLY;
-- ${2:-v1}
SQL
}

next_n() {
    sql "CREATE orders;" >/dev/null
    sql_result "SELECT VALUE n FROM orders ORDER BY n DESC LIMIT 1;" | jq -r '.[0]'
}

expect_next() {
    local got
    got="$(next_n)"
    [ "$got" = "$1" ] || fail "$2: expected the next id to be $1, got $got"
}

# The control: SurrealDB itself rewinds on OVERWRITE, so a pass below is not an
# accident of the server. On 3.2 this only shows after a restart.
sql "DEFINE SEQUENCE control BATCH 1 START 1;" >/dev/null
sql "RETURN [sequence::nextval('control'), sequence::nextval('control')];" >/dev/null
if restart_server; then
    sql "DEFINE SEQUENCE OVERWRITE control BATCH 1 START 1;" >/dev/null
    [ "$(sql_result "RETURN sequence::nextval('control');")" = 1 ] \
        || fail "control: OVERWRITE after a restart should rewind the counter"
    ok "control: the server rewinds a sequence on OVERWRITE after a restart"
fi

write_schema "DEFINE SEQUENCE order_no BATCH 1 START 1;"
kit sync
for want in 1 2 3; do expect_next "$want" "first ids"; done
ok "sync defined the sequence; ids 1..3 handed out"

write_schema "DEFINE SEQUENCE order_no BATCH 1 START 1;" touched
kit sync 2>&1 | tee /tmp/sync.log
expect_next 4 "re-sync of a plain DEFINE SEQUENCE"
ok "re-sync of a plain DEFINE SEQUENCE kept counting"

write_schema "DEFINE SEQUENCE IF NOT EXISTS order_no BATCH 1 START 1;" touched-again
kit sync
expect_next 5 "re-sync of DEFINE SEQUENCE IF NOT EXISTS"
ok "re-sync of DEFINE SEQUENCE IF NOT EXISTS kept counting"

if restart_server; then
    write_schema "DEFINE SEQUENCE IF NOT EXISTS order_no BATCH 1 START 1;" after-restart
    kit sync
    expect_next 6 "re-sync after a restart"
    ok "re-sync after a server restart kept counting"
fi

# A changed definition is not applied (that would reset the counter), and sync
# says so.
write_schema "DEFINE SEQUENCE order_no BATCH 1 START 1000;" changed
kit sync 2>&1 | tee /tmp/sync.log
grep -q "the definition of sequence \`order_no\` changed" /tmp/sync.log || fail "sync should warn about the changed sequence"
next="$(next_n)"
[ "$next" -lt 1000 ] || fail "a changed START must not reposition the counter, got $next"
ok "a changed sequence definition is reported, not applied"

# Through a rollout: baseline, touch the file, plan, start, complete.
write_schema "DEFINE SEQUENCE order_no BATCH 1 START 1000;" rollout-base
kit sync >/dev/null
kit rollout baseline
before="$(next_n)"
write_schema "DEFINE SEQUENCE order_no BATCH 1 START 1000;" rollout-change
kit rollout plan --name touch_sequence_file
ID="$(manifest_id_named touch_sequence_file)"
restart_server || true
kit rollout start "$ID"
kit rollout complete "$ID"
expect_next "$((before + 1))" "a rollout re-applying the file"
ok "a rollout re-applying the file kept counting"

# An explicit OVERWRITE is what the author asked for: it is applied, and warned
# about every time.
write_schema "DEFINE SEQUENCE OVERWRITE order_no BATCH 1 START 500;" explicit
kit sync 2>&1 | tee /tmp/sync.log
grep -q "sequence \`order_no\` is defined with OVERWRITE" /tmp/sync.log || fail "an explicit OVERWRITE should warn"
ok "an explicit OVERWRITE is applied and warned about"
