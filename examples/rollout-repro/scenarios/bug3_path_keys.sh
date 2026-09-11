#!/usr/bin/env bash
# BUG 3 — tracking keys depend on the process working directory.
#
# Run by `run.sh` inside BOTH kit-root (working_dir /) and kit-workdir
# (working_dir /work): same image, same mount, same database, differing only in
# the working directory. It prints the keys this environment writes, and
# `run.sh` compares the two sets.
#
# Through 1.0.0-beta.1 the key was the walked path with the *working directory*
# stripped, so `/database/schema/001_person.surql` reduced to
# `database/schema/001_person.surql` only when the working directory happened to
# be `/`. Anywhere else it stayed absolute, and every consumer of those keys —
# the `__entity` sync hashes, `__seed`, the committed snapshots, and a
# manifest's `target_schema_hash` — then disagreed between a developer checkout
# and CI.
BUG=3
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

kit sync --no-prune >/dev/null
printf 'cwd=%s\n' "$(pwd)"
tracked_keys
