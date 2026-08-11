#!/usr/bin/env bash
set -euo pipefail

PACKAGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
IMPORT_MODULE_PATTERN='^[[:space:]]*(?:@[A-Za-z_][A-Za-z0-9_]*(?:\([^)]*\))?[[:space:]]+)*(?:(?:private|fileprivate|internal|package|public)[[:space:]]+)?import[[:space:]]+(?:(?:typealias|struct|class|enum|protocol|let|var|func)[[:space:]]+)?\K[A-Za-z_][A-Za-z0-9_]*'

fail_if_found() {
  local pattern="$1"
  shift
  if rg -n "$pattern" "$@"; then
    echo "error: ElectricSync dependency boundary violation (matches above)." >&2
    exit 1
  fi
}

reject_imports_except() {
  local directory="$1"
  local allowed_modules="$2"
  local boundary_description="$3"
  local invalid_imports

  invalid_imports="$(
    rg --pcre2 -n -o "$IMPORT_MODULE_PATTERN" "$directory" \
      | rg -v ":(${allowed_modules})$" \
      || true
  )"
  if [[ -n "$invalid_imports" ]]; then
    printf '%s\n' "$invalid_imports"
    echo "error: $boundary_description" >&2
    exit 1
  fi
}

reject_imports_except \
  "$PACKAGE_DIR/Sources" \
  'Foundation|CryptoKit' \
  'ElectricSync library source may import only Foundation and CryptoKit.'
reject_imports_except \
  "$PACKAGE_DIR/Tests" \
  'Foundation|Testing|GRDB|ElectricSync' \
  'ElectricSync tests may import only Foundation, Testing, GRDB, and ElectricSync.'

fail_if_found '\.package\([[:space:]]*path[[:space:]]*:' "$PACKAGE_DIR/Package.swift"

manifest_json="$(swift package dump-package --package-path "$PACKAGE_DIR")"
printf '%s' "$manifest_json" | python3 -c '
import json
import sys

package = json.load(sys.stdin)
targets = [target for target in package["targets"] if target["name"] == "ElectricSync"]
if len(targets) != 1:
    print("error: Package.swift must declare exactly one ElectricSync library target.", file=sys.stderr)
    raise SystemExit(1)

dependencies = targets[0].get("dependencies", [])
if dependencies:
    print("error: ElectricSync library target must not declare dependencies:", file=sys.stderr)
    for dependency in dependencies:
        print(f"  {dependency}", file=sys.stderr)
    raise SystemExit(1)
'

echo "ElectricSync dependency boundary check passed."
