#!/usr/bin/env bash
# Reproduction harness for the SurrealKit rollout pipeline issues.
#
#   ./run.sh --image before    # published 1.0.0-beta.1: every bug must REPRO
#   ./run.sh --image after     # this working tree: every bug must be CLEAN
#   ./run.sh                   # both, side by side
#
# Exits non-zero if `before` fails to reproduce a bug (the harness is broken) or
# `after` still reproduces one (the fix is incomplete). Asserting in both
# directions is what keeps this honest: a harness that can only pass proves
# nothing.
#
# Written for bash 3.2, which is what macOS ships — no associative arrays.
set -uo pipefail

cd "$(dirname "$0")"

BUGS="1 2 3 4"
RESULTS="$(mktemp -d)"
trap 'rm -rf "$RESULTS"' EXIT

die() { printf 'error: %s\n' "$*" >&2; exit 2; }
record() { printf '%s\n' "$3" > "$RESULTS/$1.$2"; }
recall() { cat "$RESULTS/$1.$2" 2>/dev/null; }

preflight() {
    command -v docker >/dev/null || die "docker is not installed"
    docker info >/dev/null 2>&1 || die \
        "docker daemon not reachable. Start Docker Desktop (open -a Docker), or 'colima start'."
    docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
}

compose() {
    local source="$1"; shift
    HARNESS_SOURCE="$source" docker compose "$@"
}

run_image() {
    local source="$1" label="$2"
    printf '\n=== building the %s image (%s) ===\n' "$label" "$source"
    compose "$source" build kit-root >/dev/null || die "build failed for $label"
    compose "$source" up -d surrealdb blackhole >/dev/null || die "services failed to start"

    printf '=== running scenarios (%s) ===\n' "$label"

    record "$label" 1 "$(compose "$source" run --rm --no-deps -T kit-root \
        bash /scenarios/bug1_cli_parse.sh 2>/dev/null | tail -1)"

    record "$label" 2 "$(compose "$source" run --rm -T kit-root \
        bash /scenarios/bug2_modified_entity.sh 2>/dev/null | tail -1)"

    # BUG 3 needs both environments: the divergence IS the bug, so neither side
    # can show it alone. Same image, same mount, same database — only the
    # working directory differs.
    local at_root at_workdir
    at_root="$(compose "$source" run --rm -T kit-root \
        bash /scenarios/bug3_path_keys.sh 2>/dev/null | grep -v '^cwd=' | grep . | sort)"
    at_workdir="$(compose "$source" run --rm -T kit-workdir \
        bash /scenarios/bug3_path_keys.sh 2>/dev/null | grep -v '^cwd=' | grep . | sort)"
    if [ -z "$at_root$at_workdir" ]; then
        record "$label" 3 "SKIP   3     could not read tracking keys from either environment"
    elif [ "$at_root" != "$at_workdir" ]; then
        record "$label" 3 "REPRO  3     keys differ by working directory: [$(echo $at_root)] vs [$(echo $at_workdir)]"
    else
        record "$label" 3 "CLEAN  3     identical keys from both working directories: [$(echo $at_root)]"
    fi

    record "$label" 4 "$(compose "$source" run --rm --no-deps -T kit-root \
        bash /scenarios/bug4_hang.sh 2>/dev/null | tail -1)"

    compose "$source" down -v >/dev/null 2>&1 || true
}

report() {
    local failures=0 label want bug line got
    printf '\n%-4s %-14s %s\n' "BUG" "EXPECTED" "OBSERVED"
    printf '%s\n' "--------------------------------------------------------------------------"
    for label in "$@"; do
        [ "$label" = before ] && want=REPRO || want=CLEAN
        for bug in $BUGS; do
            line="$(recall "$label" "$bug")"
            got="${line%% *}"
            printf '%-4s %-14s %s\n' "$bug" "$label=$want" "${line:-<no output>}"
            [ "$got" = "$want" ] || failures=$((failures + 1))
        done
    done
    printf '%s\n' "--------------------------------------------------------------------------"
    if [ "$failures" -gt 0 ]; then
        printf '%d scenario(s) did not match the expected verdict.\n' "$failures"
        return 1
    fi
    printf 'all scenarios matched.\n'
}

IMAGES="before after"
if [ "${1:-}" = "--image" ]; then
    [ -n "${2:-}" ] || die "--image needs a value (before|after)"
    IMAGES="$2"
fi

preflight
for image in $IMAGES; do
    case "$image" in
        before) run_image crates-io before ;;
        after) run_image worktree after ;;
        *) die "unknown image $image (expected 'before' or 'after')" ;;
    esac
done
report $IMAGES
