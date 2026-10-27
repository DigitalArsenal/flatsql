#!/usr/bin/env bash
# Local landing proof: package/native data flow plus actual browser interaction.
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/ci-e2e.sh
node scripts/ci-browser-e2e.mjs
