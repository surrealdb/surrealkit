#!/usr/bin/env bash
# BUG 4 — no deadline between the CLI and the database.
#
# `blackhole` accepts the TCP connection and never completes the WebSocket
# handshake: a reachable but wedged peer, which is what a database restarting
# behind a proxy or a mid-deploy app swap looks like. Through 1.0.0-beta.1 the
# CLI waited forever, which is the reported "rollout start hung until our
# 25-minute CI timeout" — and left `__rollout` in an intermediate state.
#
# `status` is used rather than `start` on purpose: it takes no positional
# argument, so this scenario is not entangled with BUG 1.
BUG=4
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

# Must exceed the 30s default connect deadline, or this kills the process before
# the deadline can fire and a fixed build is misreported as still hanging. No
# --connect-timeout-secs flag is passed on purpose: 1.0.0-beta.1 would reject it
# as an unknown argument and exit instantly, which would look like a pass. This
# tests the default, which is the behaviour that matters.
BUDGET="${BUDGET:-45}"

start=$(date +%s)
timeout "$BUDGET" surrealkit rollout status --host ws://blackhole:8000 >/tmp/bug4.log 2>&1
code=$?
elapsed=$(( $(date +%s) - start ))

if [ "$code" -eq 124 ]; then
    repro "no deadline: still waiting on a wedged endpoint after ${elapsed}s (killed)"
fi

if grep -q 'timed out after' /tmp/bug4.log; then
    clean "bounded by the connect deadline after ${elapsed}s: $(grep -o 'timed out after [0-9]*s' /tmp/bug4.log | head -1)"
fi

clean "exited in ${elapsed}s (code $code): $(head -1 /tmp/bug4.log)"
