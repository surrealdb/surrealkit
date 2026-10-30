# Publishing

How a release's binaries reach their users, and what each copy promises.

Pushing a `v<version>` tag runs `.github/workflows/release-binaries.yml`, and
that one run publishes both copies of the binaries:

| Where | What it is for |
|---|---|
| GitHub release | `cargo binstall surrealkit` and manual downloads: `surrealkit-<tag>-<target>` archives with `.sha256` files |
| `download.surrealdb.com/cli/kit/` | The `surreal` launcher: `surreal install kit`, then `surreal kit …` |

Each target is compiled once, and that build yields both the GitHub release
archive and the launcher's asset, so the two copies cannot drift. crates.io and
the Docker image are published by `cargo.yml` and `release-docker.yml`, from the
same tag.

## The executable

Cargo builds one binary, `surrealkit`. The launcher stores and runs it as
`surreal-kit`, the name every component of the `surreal` CLI has
(`surreal-<component>`), so it is published under that name. It is the same
program: help and usage name it as it was started, and as `surreal kit` when
the launcher says that is how it was typed.

---

## Cutting a release

1. Bump `version` in the root `Cargo.toml` (and `surrealkit-macros`'s in
   `[workspace.dependencies]`), run `cargo build` so `Cargo.lock` follows, and
   merge.
2. Push the tag `v<version>`. The workflow builds every target, then writes
   the GitHub release and publishes to `cli/kit/`.

The run refuses to publish anything if the tag is not `v<version>`, where
`<version>` is what `Cargo.toml` says at that tag.

A dispatch of the workflow from a branch is the rehearsal: every target builds
and is smoke-tested, the launcher's assets are staged and re-verified against
their sidecars, and the exact list of keys a publish would write is printed. No
credential is passed to any of its jobs.

### Which pointers move

There are no inputs. The pointers under `cli/kit/` follow the version:

| Pointer | Moved when |
|---|---|
| `latest.txt` | Stable, and newer than the version it names now (or it does not exist) |
| `v<major>.txt` | Stable, and newer than the version it names now (or it does not exist) |
| `beta.txt` / `alpha.txt` | A pre-release, on the version's channel |

The channel is derived from the version: no `-` is stable, an `alpha` first
pre-release identifier is alpha, and anything else with a `-` is beta. Until a
stable release exists, the launcher falls back from `latest.txt` to `beta.txt`,
then `alpha.txt`, so `surreal install kit` installs the newest pre-release.

---

## What gets written

The database's layout, under the prefix the launcher's registry gives the `kit`
component, as every component of the `surreal` CLI is published:

```
download.surrealdb.com/cli/kit/
  latest.txt                -> v1.0.0      mutable    no-store
  v1.txt                    -> v1.0.0      mutable    no-store
  beta.txt / alpha.txt      -> v1.1.0-beta.1, only for a pre-release
  v1.0.0/
    surreal-kit-v1.0.0.linux-amd64.tgz     immutable  INTELLIGENT_TIERING
    surreal-kit-v1.0.0.linux-amd64.txt
    surreal-kit-v1.0.0.linux-arm64.tgz   + .txt
    surreal-kit-v1.0.0.darwin-amd64.tgz  + .txt
    surreal-kit-v1.0.0.darwin-arm64.tgz  + .txt
    surreal-kit-v1.0.0.windows-amd64.exe + .txt
```

- Each `.tgz` holds exactly one member, `surreal-kit`, at the archive root,
  mode 0755. Windows publishes the bare `.exe`.
- Each `.txt` is the asset's SHA-256, 64 lowercase hex characters. Its name
  replaces the asset's extension rather than appending to it.
- Nothing else is written beside a version. The launcher reads every component
  with one reader, so a file one component has and another does not is a
  special case in it.

Linux is published from the static musl builds, which start on any
distribution. Windows links the C runtime statically, so the bare `.exe` starts
on a machine without the VC++ redistributable.

`scripts/package.sh` is the only implementation of the archive and sidecar
layout here, and a copy of the launcher repository's.

**Order matters:** assets, then sidecars, then pointers. A client that reads a
pointer must find the files it names already in place. Every mutable key gets
`Cache-Control: no-store`; the versioned keys are immutable and get none.

### Republish protection

A version is published at most once. Before uploading anything, the publish
job asks for the version's five `.txt` sidecars and refuses if any answers 200
(`403` and `404` both mean absent; anything else fails the run rather than
guess). The sidecars are uploaded after every asset, so one that answers is a
download somebody can already complete.

After an interrupted publish the version cannot be retried until its partial
`cli/kit/v<version>/` keys are deleted by hand, or the version is bumped. No
pointer will have moved, because pointers are written last.

### Propagation

After the pointers, `propagate-kit` copies `cli/kit/` to each of the eleven
regional buckets, `download.<region>.surrealdb.com`, in parallel, and fails
unless a majority succeeds. Unlike the house `propagate-binaries`, it syncs only
`cli/kit/` and never deletes, so the credential it needs is the narrow one
below.

---

## Security and prerequisites

The bucket's credentials are the organisation secrets `AMAZON_ACCESS_KEY` and
`AMAZON_SECRET_KEY`, shared with this repository. Only the one job that
uploads names them, and it runs no compiler: the builds run `build.rs` for the
whole dependency graph, which is arbitrary code from crates.io, and hold no
credential. The workflow's token is read-only except in the job that writes the
GitHub release.

The key behind those secrets must be able to write `download.surrealdb.com/cli/kit/*`,
and list and read that prefix and write the same prefix in each of the eleven
regional buckets `propagate-kit` syncs to. A rehearsal cannot check this; a
publish that is refused part-way through its uploads leaves the version
needing its partial keys deleted (see republish protection above).

If either secret stops reaching this repository, the publish fails at its
first upload, after the GitHub release has been written.
