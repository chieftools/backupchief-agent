#!/bin/bash

set -euo pipefail

VERSION="${1:-}"
OUTPUT_ROOT="${2:-runtime}"

if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Version must use numeric X.Y.Z format." >&2
    exit 1
fi

if [ -z "${OUTPUT_ROOT}" ] || [ "${OUTPUT_ROOT}" = "/" ]; then
    echo "Runtime output directory is unsafe." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESTIC_VERSION="$(tr -d '[:space:]' < "${REPOSITORY_DIR}/restic/VERSION")"
RELEASE_PARENT="${OUTPUT_ROOT}/v${VERSION}"
BUILD_DIR="$(mktemp -d)"
STAGING="${BUILD_DIR}/v${VERSION}"
trap 'rm -rf "${BUILD_DIR}"' EXIT

cd "${REPOSITORY_DIR}"

./bin/build.sh "${VERSION}" linux-amd64
./bin/build.sh "${VERSION}" linux-arm64
./bin/build.sh "${VERSION}" darwin-arm64

mkdir -p "${STAGING}"

install -m 0755 backupchief-linux-amd64 "${STAGING}/backupchief-${VERSION}-linux-amd64"
install -m 0755 backupchief-linux-arm64 "${STAGING}/backupchief-${VERSION}-linux-arm64"
install -m 0755 backupchief-darwin-arm64 "${STAGING}/backupchief-${VERSION}-darwin-arm64"

install -m 0644 LICENSE "${STAGING}/LICENSE"
install -m 0644 restic/LICENSE "${STAGING}/restic-LICENSE"
install -m 0644 CHANGELOG.md "${STAGING}/RELEASE_NOTES.md"

artifact_json() {
    local FILE="$1"
    local HASH
    local SIZE

    HASH="$(shasum -a 256 "${STAGING}/${FILE}" | awk '{print $1}')"
    SIZE="$(wc -c < "${STAGING}/${FILE}" | tr -d ' ')"
    printf '{"file":"%s","sha256":"%s","size":%s}' "${FILE}" "${HASH}" "${SIZE}"
}

DARWIN_HELPER="backupchief-${VERSION}-darwin-arm64"
LINUX_AMD64_HELPER="backupchief-${VERSION}-linux-amd64"
LINUX_ARM64_HELPER="backupchief-${VERSION}-linux-arm64"

cat > "${STAGING}/manifest.json" << EOF
{
    "schema": 1,
    "version": "${VERSION}",
    "restic_version": "${RESTIC_VERSION}",
    "platforms": {
        "darwin-arm64": {
            "backupchief": $(artifact_json "${DARWIN_HELPER}")
        },
        "linux-amd64": {
            "backupchief": $(artifact_json "${LINUX_AMD64_HELPER}")
        },
        "linux-arm64": {
            "backupchief": $(artifact_json "${LINUX_ARM64_HELPER}")
        }
    }
}
EOF

(
    cd "${STAGING}"
    shasum -a 256 \
        "${DARWIN_HELPER}" \
        "${LINUX_AMD64_HELPER}" \
        "${LINUX_ARM64_HELPER}" \
        LICENSE \
        RELEASE_NOTES.md \
        manifest.json \
        restic-LICENSE \
        > SHA256SUMS
)

mkdir -p "${OUTPUT_ROOT}"
rm -rf "${RELEASE_PARENT}"
mv "${STAGING}" "${RELEASE_PARENT}"

echo "Runtime release built: ${RELEASE_PARENT}"
shasum -a 256 "${RELEASE_PARENT}/manifest.json"
