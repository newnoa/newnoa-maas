#!/usr/bin/env bash
set -euo pipefail
root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
for module in gateway owner network; do
  (cd "$root_dir/$module" && go test -race ./...)
done
