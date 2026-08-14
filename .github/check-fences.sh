#!/usr/bin/env bash
#
# Hexagonal boundary fence check.
#
# Every workspace crate that depends on `timing-core` must carry a
# `clippy.toml` fence denying `timing_core::domain::*` and
# `timing_core::services::*`, EXCEPT the two crates allowed to see every
# layer: `timing-core` itself and the composition root `timing-node`.
#
# The fences are enumerated by hand (clippy config takes exact paths, not
# globs), so the failure mode this guards against is drift: a new adapter
# lands without a fence, or someone edits one copy and not the others, and
# the boundary silently stops being enforced for part of the workspace.
#
# Run from the repo root.

set -euo pipefail

EXEMPT=("timing-core" "timing-node")
CANONICAL="adapter-ingest-tcp/clippy.toml"

if [[ ! -f "$CANONICAL" ]]; then
    echo "fence: canonical fence $CANONICAL is missing" >&2
    exit 1
fi

fail=0
canonical_sum="$(sha256sum "$CANONICAL" | cut -d' ' -f1)"

for manifest in */Cargo.toml; do
    crate="$(dirname "$manifest")"

    # Only crates that can actually see `timing-core`'s internals.
    grep -q '^timing-core\|^timing_core\|"timing-core"' "$manifest" || continue

    skip=0
    for e in "${EXEMPT[@]}"; do
        [[ "$crate" == "$e" ]] && skip=1
    done
    [[ $skip -eq 1 ]] && continue

    if [[ ! -f "$crate/clippy.toml" ]]; then
        echo "fence: $crate depends on timing-core but has no clippy.toml fence" >&2
        fail=1
        continue
    fi

    if [[ "$(sha256sum "$crate/clippy.toml" | cut -d' ' -f1)" != "$canonical_sum" ]]; then
        echo "fence: $crate/clippy.toml has drifted from $CANONICAL" >&2
        diff -u "$CANONICAL" "$crate/clippy.toml" >&2 || true
        fail=1
    fi
done

if [[ $fail -ne 0 ]]; then
    echo "" >&2
    echo "Fix by copying $CANONICAL over the offending file(s)." >&2
    exit 1
fi

echo "fence: all timing-core dependents carry an identical clippy.toml"
