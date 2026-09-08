#!/bin/bash

set -euo pipefail

AGENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${AGENT_ROOT}"

./bin/build.sh dev dev-native
exec ./backupchief dev "$@"
