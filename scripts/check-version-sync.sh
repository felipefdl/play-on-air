#!/bin/sh
# Fail when the release version surfaces disagree (AGENTS.md "Deploy and release").
#
# Usage: scripts/check-version-sync.sh [expected-version]
#
# The root Cargo.toml [workspace.package] version is the reference. When
# expected-version is given (for example the version from a vX.Y.Z tag), it must
# match too.

set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

failures=0

fail() {
  printf 'version mismatch: %s\n' "$1" >&2
  failures=$((failures + 1))
}

check() {
  # $1 = surface label, $2 = found value
  if [ -z "$2" ]; then
    fail "$1: version not found"
  elif [ "$2" != "$version" ]; then
    fail "$1: found '$2', expected '$version'"
  fi
}

version=$(awk '
  /^\[/ { in_pkg = ($0 == "[workspace.package]"); next }
  in_pkg && /^version[[:space:]]*=/ {
    sub(/^version[[:space:]]*=[[:space:]]*"/, ""); sub(/".*$/, ""); print; exit
  }
' Cargo.toml)

if [ -z "$version" ]; then
  printf 'version mismatch: Cargo.toml: [workspace.package] version not found\n' >&2
  exit 1
fi

if [ "$#" -gt 0 ] && [ -n "$1" ] && [ "$1" != "$version" ]; then
  fail "expected version: got '$1', Cargo.toml [workspace.package] has '$version'"
fi

lock_version=$(awk '
  /^\[\[package\]\]/ { in_pkg = 0; next }
  $0 == "name = \"play-on-air\"" { in_pkg = 1; next }
  in_pkg && /^version = / {
    sub(/^version = "/, ""); sub(/".*$/, ""); print; exit
  }
' Cargo.lock)
check 'Cargo.lock package play-on-air' "$lock_version"

config_version=$(sed -n 's/^version:[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}[[:space:]]*$/\1/p' play-on-air/config.yaml | head -n 1)
check 'play-on-air/config.yaml version' "$config_version"

docker_version=$(sed -n 's/^ARG BUILD_VERSION=\([^[:space:]]*\).*$/\1/p' Dockerfile | head -n 1)
check 'Dockerfile ARG BUILD_VERSION' "$docker_version"

changelog_version=$(sed -n 's/^## \([^[:space:]]*\)[[:space:]]*$/\1/p' play-on-air/CHANGELOG.md | head -n 1)
check 'play-on-air/CHANGELOG.md latest section' "$changelog_version"

# Pinned image tags in docs (latest, <version>, and sha-* tags are not pinned).
# play-on-air/DOCS.md must pin the image at the app version; README.md may pin it.
for doc in README.md play-on-air/DOCS.md; do
  tags=$(grep -oE 'play-on-air:[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.+-]*' "$doc" | sed 's/^play-on-air://; s/\.$//' || true)
  if [ -z "$tags" ] && [ "$doc" = play-on-air/DOCS.md ]; then
    fail "$doc: no pinned image tag ghcr.io/<owner>/play-on-air:x.y.z"
  fi
  for tag in $tags; do
    check "$doc image tag" "$tag"
  done
done

if [ "$failures" -gt 0 ]; then
  printf '%d version surface(s) out of sync with %s\n' "$failures" "$version" >&2
  exit 1
fi

printf 'version surfaces in sync: %s\n' "$version"
