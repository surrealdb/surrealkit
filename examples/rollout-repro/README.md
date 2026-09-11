# Rollout reproduction harness

Reproduces the four rollout problems reported against SurrealKit `0.7.0` /
`1.0.0-beta.1`, and proves the fixes, against **SurrealDB v3.2.4**.

```bash
./run.sh --image before    # published 1.0.0-beta.1 from crates.io
./run.sh --image after     # this working tree
./run.sh                   # both, side by side
```

Each scenario prints one verdict — `REPRO` (buggy behaviour observed) or `CLEAN`
— and `run.sh` checks it against what that image *should* do. It exits non-zero
if `before` fails to reproduce a bug (the harness itself is broken) or if `after`
still reproduces one (the fix is incomplete). Asserting in both directions is
what keeps it honest: a harness that can only pass proves nothing.

Requires a running Docker daemon (`open -a Docker` on macOS) and Compose v2.

## What it reproduces

| Bug | Symptom | Root cause |
|---|---|---|
| 1 | `rollout start <id>` fails with `unknown target "<id>"`; `--target` is rejected | A global `-t/--target` added in `1.0.0-beta.1` shares a clap arg id with the positional named `target` on every rollout subcommand that takes an id |
| 2 | `rollout plan` refuses a tightened `ASSERT` | The diff models only "present" and "absent"; any change to an existing `DEFINE` is refused, because the generated rollback could only *remove* what it added |
| 3 | Snapshot paths differ between a checkout and a container | Tracking keys were relative to the process **working directory**, not the project folder |
| 4 | `rollout start` hangs until CI kills it | Nothing between the CLI and the database had a deadline |

## Why two containers

`kit-root` and `kit-workdir` are the **same image**, against the **same mount**,
talking to the **same database**. They differ in exactly one setting:
`working_dir` (`/` versus `/work`).

That isolation is the whole point of bug 3. Comparing a host run against a
container run would confound the working directory with the operating system,
the folder spelling, and the user. Comparing two containers that differ in one
variable attributes the divergence to that variable.

`/` is not an arbitrary choice: it is what the shipped image happens to give you,
and it is the one value for which the old key handling accidentally produced the
same result as a local run. Any `WORKDIR`, `-w`, or compose `working_dir`
broke it.

## Why a separate Dockerfile

The shipped runtime is `gcr.io/distroless/cc-debian12:nonroot`, which has no
shell and so cannot run a reproduction script. `Dockerfile.harness` reuses the
repo's builder stage verbatim and swaps in `debian:bookworm-slim`, so the binary
under test is built exactly the same way.

That image also runs as root, which sidesteps a real defect the distroless image
has: `run_setup` writes `<folder>/setup.surql`, and `rollout start` calls it
first, so on a read-only or differently-owned mount every rollout command failed.
That is fixed in the working tree (it degrades to the built-in metadata schema
with a warning) but it still bites anyone on `1.0.0-beta.1`.

## Layout

```
docker-compose.yml     surrealdb v3.2.4, a blackhole endpoint, and the two kit services
Dockerfile.harness     builder from the repo + a runtime with a shell; SOURCE=crates-io|worktree
project/               the fixture: a person table with two ASSERTed fields
scenarios/bug*.sh      one file per bug, each printing a single verdict line
scenarios/ci_*.sh      the end-to-end lifecycle scripts CI runs natively
run.sh                 preflight, orchestration, and the before/after table
```

## The blackhole service

`alpine/socat` accepting TCP and never answering. That is the shape of the
reported hang — a peer that is reachable but wedged, such as a database
restarting behind a proxy or a mid-deploy app swap — and it is deterministic,
unlike trying to induce a real stall.

Bug 4's scenario uses `rollout status` rather than `rollout start` deliberately:
`status` takes no positional argument, so the hang is measured independently of
bug 1.

## Running the CI scenarios directly

`scenarios/ci_*.sh` run natively against any SurrealDB, which is handy while
iterating:

```bash
docker run -d --rm -p 18000:8000 surrealdb/surrealdb:v3.2.4 \
    start --user root --pass secret memory
cargo build -p surrealkit --bin surrealkit --no-default-features --features cli

export SURREALDB_HOST=ws://127.0.0.1:18000 SURREAL_HTTP=http://127.0.0.1:18000
bash examples/rollout-repro/scenarios/ci_lifecycle.sh
bash examples/rollout-repro/scenarios/ci_modified_entity.sh
bash examples/rollout-repro/scenarios/ci_resume.sh
bash examples/rollout-repro/scenarios/ci_container_keys.sh
```

Each uses its own namespace and database, so they are independent and any one of
them can be re-run in isolation.
