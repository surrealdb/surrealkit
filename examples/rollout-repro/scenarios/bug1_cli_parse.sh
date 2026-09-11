#!/usr/bin/env bash
# BUG 1 — the rollout id is parsed as a database target.
#
# 1.0.0-beta.1 added a global `-t/--target` whose clap id collides with the
# positional named `target` on every rollout subcommand that takes an id. The
# positional shadows the global, so `--target` disappears from those
# subcommands, and the id is hoisted into the global and resolved as a
# `[target.*]` name — before the command ever connects.
BUG=1
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

ID="20260302153045__demo"

# (a) The plain, documented invocation.
out="$(kit rollout start "$ID")"
if grep -q 'unknown target' <<<"$out"; then
    repro "rollout start <id> resolved the id as a database target: $(head -1 <<<"$out")"
fi

# (b) Selecting a target explicitly.
out="$(kit rollout start "$ID" --target prod)"
if grep -q "unexpected argument '--target'" <<<"$out"; then
    repro "rollout start rejects --target outright"
fi

# (c) The cheapest detector: the global vanishes from --help.
out="$(kit rollout start --help)"
if ! grep -q -- '--target' <<<"$out"; then
    repro "--target is missing from 'rollout start --help'"
fi
if ! grep -q 'ROLLOUT_ID' <<<"$out"; then
    repro "'rollout start --help' does not name a ROLLOUT_ID positional"
fi

clean "rollout subcommands accept both a rollout id and --target"
