#!/usr/bin/env bash
# BUG 2 — `rollout plan` refuses any modified entity.
#
# The reported case is tightening an ASSERT on an existing field. The diff is a
# per-DEFINE-statement hash comparison, so this is not ASSERT-specific: any byte
# change to an existing DEFINE hits it. `plan` models only "present" and
# "absent".
BUG=2
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

FOLDER="${SURREALDB_FOLDER:-/database}"
reset_project

# Establish a baseline so the field counts as pre-existing.
kit rollout baseline >/dev/null

# Tighten the ASSERT: one clause, one field, one statement.
sed -i 's/ASSERT \$value >= 0;/ASSERT $value >= 18;/' "$FOLDER/schema/001_person.surql"

# --dry-run deliberately: the refusal precedes any write.
out="$(kit rollout plan --name tighten_age --dry-run)"

if grep -q 'refuses modified managed entities' <<<"$out"; then
    reset_project
    repro "plan refuses the tightened ASSERT with no way forward"
fi

if grep -q 'allow-modified' <<<"$out"; then
    # The refusal is now an opt-in gate. Check it actually names the entity.
    if ! grep -q 'field:person.age' <<<"$out"; then
        reset_project
        repro "the refusal does not qualify the entity by table (got: $(head -2 <<<"$out"))"
    fi
    out="$(kit rollout plan --name tighten_age --allow-modified --dry-run)"
    if ! grep -qi 'pending rollout plan' <<<"$out"; then
        reset_project
        repro "--allow-modified did not let the change plan: $(head -2 <<<"$out")"
    fi
    reset_project
    clean "plan refuses by default, explains why, and plans with --allow-modified"
fi

reset_project
clean "plan accepted the modified entity"
