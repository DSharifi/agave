#!/usr/bin/env bash

set -e

here="$(dirname "$0")"
src_root="$(readlink -f "${here}/..")"

cd "${src_root}"

# `cargo-audit` doesn't give us a way to do this nicely, so hammer it is...
dep_tree_filter="grep -Ev '│|└|├|─'"

while [[ -n $1 ]]; do
  if [[ $1 = "--display-dependency-trees" ]]; then
    dep_tree_filter="cat"
    shift
  fi
done

default_cargo_audit_extra_args=(
  # Crate:     ed25519-dalek
  # Version:   1.0.1
  # Title:     Double Public Key Signing Function Oracle Attack on `ed25519-dalek`
  # Date:      2022-06-11
  # ID:        RUSTSEC-2022-0093
  # URL:       https://rustsec.org/advisories/RUSTSEC-2022-0093
  # Solution:  Upgrade to >=2
  --ignore RUSTSEC-2022-0093

  # Crate:     idna
  # Version:   0.1.5
  # Title:     `idna` accepts Punycode labels that do not produce any non-ASCII when decoded
  # Date:      2024-12-09
  # ID:        RUSTSEC-2024-0421
  # URL:       https://rustsec.org/advisories/RUSTSEC-2024-0421
  # Solution:  Upgrade to >=1.0.0
  # need to solve this dependant tree:
  # jsonrpc-core-client v18.0.0 -> jsonrpc-client-transports v18.0.0 -> url v1.7.2 -> idna v0.1.5
  --ignore RUSTSEC-2024-0421

  # Crate:     curve25519-dalek
  # Version:   3.2.1
  # Title:     Timing variability in `curve25519-dalek`'s `Scalar29::sub`/`Scalar52::sub`
  # Date:      2024-06-18
  # ID:        RUSTSEC-2024-0344
  # URL:       https://rustsec.org/advisories/RUSTSEC-2024-0344
  # Solution:  Upgrade to >=4.1.3
  --ignore RUSTSEC-2024-0344

  # Crate:     crypto-bigint
  # Version:   0.5.5
  # Title:     Multiple timing variability issues in operations intended to be constant-time
  # Date:      2026-10-23
  # ID:        RUSTSEC-2026-0387
  # URL:       https://rustsec.org/advisories/RUSTSEC-2026-0387
  # Solution:  Upgrade to >=0.7.6
  # Agave Ok:  only reached through k256 in solana-secp256k1-program and
  # solana-secp256k1-recover, which verify and recover over public data.
  # The fix needs those SDK crates to move to elliptic-curve 0.14.
  --ignore RUSTSEC-2026-0387
)

xtask_cargo_audit_extra_args=(
  # Crate:     rsa
  # Version:   0.9.10
  # Title:     Marvin Attack: potential key recovery through timing sidechannels
  # Date:      2023-11-22
  # ID:        RUSTSEC-2023-0071
  # URL:       https://rustsec.org/advisories/RUSTSEC-2023-0071
  # Severity:  5.9 (medium)
  # Solution:  No fixed upgrade is available!
  --ignore RUSTSEC-2023-0071
)

# Prints "<advisory id> <crate> <version>" for each vulnerability that
# `cargo audit` finds in the given lock file. Fails if no report is produced.
list_vulnerabilities() {
  local lock_file="$1"
  shift
  cargo audit --no-fetch --json --file "$lock_file" "$@" </dev/null |
    jq -nr 'input | .vulnerabilities.list[] | "\(.advisory.id) \(.package.name) \(.package.version)"'
}

# On pull requests, only fail on vulnerabilities the target branch doesn't
# already have, so a new advisory against an existing dependency doesn't block
# unrelated PRs. Pushes still fail on every vulnerability (see
# ci/buildkite-secondary.yml), so the target branch itself gets fixed.
base_ref=
if [[ -n $CI_PULL_REQUEST && -n $CI_BASE_BRANCH ]]; then
  if git fetch --quiet origin "$CI_BASE_BRANCH"; then
    base_ref="$(git rev-parse FETCH_HEAD)"
  else
    echo "WARNING: failed to fetch origin/$CI_BASE_BRANCH, failing on all vulnerabilities" >&2
  fi
fi

# Succeeds if every vulnerability in the given lock file is also in that lock
# file on $base_ref.
only_preexisting_vulnerabilities() {
  local lock_file="$1"
  shift

  local head_vulnerabilities base_vulnerabilities= new_vulnerabilities
  # an empty list means `cargo audit` failed for some other reason
  head_vulnerabilities="$(list_vulnerabilities "$lock_file" "$@")" || return 1
  [[ -n $head_vulnerabilities ]] || return 1

  local base_lock_file
  base_lock_file="$(mktemp)"
  if git show "$base_ref:$lock_file" >"$base_lock_file" 2>/dev/null; then
    base_vulnerabilities="$(list_vulnerabilities "$base_lock_file" "$@")" || base_vulnerabilities=
  fi
  rm -f "$base_lock_file"

  new_vulnerabilities="$(comm -23 <(sort -u <<<"$head_vulnerabilities") <(sort -u <<<"$base_vulnerabilities"))"
  if [[ -n $new_vulnerabilities ]]; then
    echo "[$lock_file]: vulnerabilities not on $CI_BASE_BRANCH (rebase if they were fixed there):"
    sed 's/^/  /' <<<"$new_vulnerabilities"
    return 1
  fi

  echo "[$lock_file]: not failing this PR, these vulnerabilities already exist on $CI_BASE_BRANCH:"
  sed 's/^/  /' <<<"$head_vulnerabilities"
}

lock_files="$(git ls-files ':**Cargo.lock')"

for lock_file in $lock_files; do
  audit_extra_args=()
  case "$lock_file" in
  ci/xtask/Cargo.lock)
    audit_extra_args=("${xtask_cargo_audit_extra_args[@]}")
    ;;
  *)
    audit_extra_args=("${default_cargo_audit_extra_args[@]}")
    ;;
  esac

  echo "--- [$lock_file]: cargo audit ${audit_extra_args[*]}"

  set +e
  (
    cd "$(dirname "$lock_file")"
    cargo audit --color always "${audit_extra_args[@]}" </dev/null
  ) | $dep_tree_filter
  pipeline_status=("${PIPESTATUS[@]}")
  set -e

  exit_code="${pipeline_status[0]}"
  if [[ $exit_code -ne 0 ]]; then
    if [[ -n $base_ref ]] && only_preexisting_vulnerabilities "$lock_file" "${audit_extra_args[@]}"; then
      continue
    fi
    exit "$exit_code"
  fi
done
