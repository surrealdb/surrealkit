#!/usr/bin/env sh

# Packages one built executable in the layout download.surrealdb.com serves for
# every component of the `surreal` CLI: the database at the bucket root, and
# each other component, SurrealKit among them under `cli/kit/`, in the same
# layout under its own prefix, so one reader in the launcher fetches all of
# them.
#
# A copy of surrealdb/surrealctl's `scripts/package.sh`, the layout's reference
# implementation. Keep the two identical below this header: a difference is an
# archive one component's clients would read differently.
#
#   scripts/package.sh <executable> <binary> <version> <platform> <out-dir>
#
#   scripts/package.sh surreal-kit target/release/surrealkit 1.0.0 darwin-arm64 staging
#
# writes, for a Unix platform,
#
#   <out-dir>/<executable>-<name>.<platform>.tgz   one member, <executable>, at
#                                                  the archive root, mode 0755
#   <out-dir>/<executable>-<name>.<platform>.txt   64 lowercase hex, the SHA-256
#                                                  of the .tgz, and a newline
#
# and for `windows-amd64` the bare executable, `<executable>-<name>.windows-amd64.exe`,
# with its `.txt` beside it. A bare executable rather than an archive because it
# runs straight from a browser download, as the database's always has.
#
# <name> is `v<version>`. Only the database publishes nightlies; nothing
# packaged here has one.
#
# The stem is the executable's own name, never a product or package name, so a
# file name says what it unpacks to. The sidecar replaces the asset's extension
# rather than appending to it. Here the executable is `surreal-kit`, what the
# launcher stores and runs as `surreal kit`, packaged from the `surrealkit`
# binary cargo builds: the two are one program.
#
# This is the only implementation of the layout here. The release workflow calls
# it on all five targets (under bash on Windows too), so the archive a user
# unpacks is made by exactly the code that runs here. A nested directory or a versioned
# member name breaks every client with "the archive contained no `surreal`".
#
# POSIX sh, so it runs against the macOS bsdtar and the Linux and Git for
# Windows GNU tar alike.

set -eu

usage() {
    echo "usage: $0 <executable> <binary> <version> <platform> <out-dir>" >&2
    exit 2
}

[ $# -eq 5 ] || usage

member=$1
binary=$2
version=${3#v}
platform=$4
out=$5

fail() {
    echo "package.sh: $1" >&2
    exit 1
}

# The same shape the release workflows enforce, so a typo cannot reach a key.
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$' \
    || fail "\`$version\` is not a version number (X.Y.Z[-prerelease])"
name="v${version}"

# The five platforms the launcher's `Asset::for_platform` can ask for. Anything
# else would be published under a name no client requests.
case "$platform" in
    darwin-arm64 | darwin-amd64 | linux-amd64 | linux-arm64) extension=tgz ;;
    windows-amd64) extension=exe ;;
    *) fail "\`$platform\` is not one of darwin-arm64, darwin-amd64, linux-amd64, linux-arm64, windows-amd64" ;;
esac

case "$member" in
    "" | *[!0-9a-z-]*) fail "\`$member\` is not a bare executable name such as surreal" ;;
esac

[ -f "$binary" ] || fail "no executable at $binary"

stem="${member}-${name}.${platform}"
asset="${out}/${stem}.${extension}"
mkdir -p "$out"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -f1 -d' '
    else
        shasum -a 256 "$1" | cut -f1 -d' '
    fi
}

if [ "$extension" = exe ]; then
    cp "$binary" "$asset"
else
    # Built from a directory holding exactly one file, so nothing else can slip
    # into the archive.
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    cp "$binary" "${work}/${member}"
    chmod 0755 "${work}/${member}"
    # COPYFILE_DISABLE stops macOS tar adding an AppleDouble `._<member>` for
    # the executable's extended attributes, which would be a second member.
    COPYFILE_DISABLE=1 tar -C "$work" -czf "$asset" "$member"

    # Checked rather than assumed: the member list and its mode are the
    # contract, and a tar that disagrees should fail here, not in an installer.
    members=$(tar -tzf "$asset")
    [ "$members" = "$member" ] \
        || fail "${asset} should hold exactly one member named ${member}, holds: ${members}"
    mode=$(tar -tvzf "$asset" | cut -c1-10)
    [ "$mode" = "-rwxr-xr-x" ] || fail "${asset}: ${member} has mode ${mode}, expected -rwxr-xr-x"
fi

# The digest is of the asset as published, which is what a client can check
# before unpacking anything.
digest=$(sha256 "$asset")
printf '%s\n' "$digest" | grep -Eq '^[0-9a-f]{64}$' || fail "could not compute the SHA-256 of ${asset}"
printf '%s\n' "$digest" > "${out}/${stem}.txt"

echo "${asset}  ${digest}"
