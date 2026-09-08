#!/bin/bash

set -e

VERSION="${1:-0.0.0}"
RELEASE="${2:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Building Backup Chief agent packages v${VERSION}"

echo "[1/4] Building Debian amd64 package..."
"${SCRIPT_DIR}/build-deb.sh" "${VERSION}" amd64

echo "[2/4] Building Debian arm64 package..."
"${SCRIPT_DIR}/build-deb.sh" "${VERSION}" arm64

echo "[3/4] Building RPM x86_64 package..."
"${SCRIPT_DIR}/build-rpm.sh" "${VERSION}" "${RELEASE}" x86_64

echo "[4/4] Building RPM aarch64 package..."
"${SCRIPT_DIR}/build-rpm.sh" "${VERSION}" "${RELEASE}" aarch64

echo "All packages built successfully."
ls -lh "backupchief_${VERSION}_amd64.deb" \
    "backupchief_${VERSION}_arm64.deb" \
    "backupchief-${VERSION}-${RELEASE}.x86_64.rpm" \
    "backupchief-${VERSION}-${RELEASE}.aarch64.rpm"
