#!/bin/bash

set -euo pipefail

VERSION="${1:-dev}"
TARGET="${2:-all}"
case "${TARGET}" in
    all|native|dev-native|linux-amd64|linux-arm64|darwin-arm64)
        ;;
    *)
        echo "Unsupported build target: ${TARGET}" >&2
        exit 1
        ;;
esac

export GOTOOLCHAIN="go$(awk '/^go / { print $2 }' go.mod)"

echo "Verifying pinned restic release inputs..."
RESTIC_DIR="$(pwd)/restic"
RESTIC_VERSION="$(tr -d '[:space:]' < "${RESTIC_DIR}/VERSION")"
VERIFY_DIR="$(mktemp -d)"
trap 'rm -rf "${VERIFY_DIR}"' EXIT
chmod 0700 "${VERIFY_DIR}"
gpg --batch --no-options --dearmor < "${RESTIC_DIR}/signing-key.asc" > "${VERIFY_DIR}/keyring.gpg"
gpgv --homedir "${VERIFY_DIR}" --keyring "${VERIFY_DIR}/keyring.gpg" --status-fd 1 \
    "${RESTIC_DIR}/SHA256SUMS.asc" "${RESTIC_DIR}/SHA256SUMS" > "${VERIFY_DIR}/signature"
if ! grep -q '^\[GNUPG:\] VALIDSIG CF8F18F2844575973F79D4E191A6868BD3F7A907 ' "${VERIFY_DIR}/signature"; then
    echo "Unexpected restic release signer." >&2
    exit 1
fi

RESTIC_PLATFORMS=""
case "${TARGET}" in
    all)
        RESTIC_PLATFORMS="linux_amd64 linux_arm64 darwin_arm64"
        ;;
    linux-amd64)
        RESTIC_PLATFORMS="linux_amd64"
        ;;
    linux-arm64)
        RESTIC_PLATFORMS="linux_arm64"
        ;;
    darwin-arm64)
        RESTIC_PLATFORMS="darwin_arm64"
        ;;
    native|dev-native)
        NATIVE_PLATFORM="$(go env GOOS)_$(go env GOARCH)"
        case "${NATIVE_PLATFORM}" in
            linux_amd64|linux_arm64|darwin_arm64)
                RESTIC_PLATFORMS="${NATIVE_PLATFORM}"
                ;;
        esac
        ;;
esac

for PLATFORM in ${RESTIC_PLATFORMS}; do
    ASSET="restic_${RESTIC_VERSION}_${PLATFORM}.bz2"
    EMBEDDED_ASSET="restic_${PLATFORM}.bz2"
    EXPECTED_HASH="$(awk -v asset="${ASSET}" '$2 == asset { print $1 }' "${RESTIC_DIR}/SHA256SUMS")"
    if [ -z "${EXPECTED_HASH}" ]; then
        echo "Pinned Restic checksum is missing for ${ASSET}." >&2
        exit 1
    fi
    if [ ! -f "${RESTIC_DIR}/${EMBEDDED_ASSET}" ]; then
		curl --fail --location --silent --show-error --max-time 120 \
			"https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/${ASSET}" \
			--output "${VERIFY_DIR}/${ASSET}"
		(cd "${VERIFY_DIR}" && awk -v asset="${ASSET}" '$2 == asset' "${RESTIC_DIR}/SHA256SUMS" | shasum -a 256 -c -)
		mv "${VERIFY_DIR}/${ASSET}" "${RESTIC_DIR}/${EMBEDDED_ASSET}"
	fi

	ACTUAL_HASH="$(shasum -a 256 "${RESTIC_DIR}/${EMBEDDED_ASSET}" | awk '{print $1}')"
	if [ "${ACTUAL_HASH}" != "${EXPECTED_HASH}" ]; then
		echo "Unexpected checksum for ${EMBEDDED_ASSET}." >&2
		exit 1
	fi
done

if [ "${TARGET}" = all ] || [ "${TARGET}" = native ] || [ "${TARGET}" = dev-native ]; then
    if [ "${TARGET}" = dev-native ]; then
        echo "Building Backup Chief development agent for the current platform..."
        CGO_ENABLED=0 go build -mod=readonly -trimpath -buildvcs=false -tags devtools \
            -ldflags="-s -w -buildid= -X main.version=${VERSION}" \
            -o backupchief .
    else
        echo "Building Backup Chief agent for the current platform..."
        CGO_ENABLED=0 go build -mod=readonly -trimpath -buildvcs=false \
            -ldflags="-s -w -buildid= -X main.version=${VERSION}" \
            -o backupchief .
    fi
fi

if [ "${TARGET}" = all ] || [ "${TARGET}" = linux-amd64 ]; then
    echo "Building Backup Chief agent for Linux amd64..."
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -buildvcs=false \
        -ldflags="-s -w -buildid= -X main.version=${VERSION}" \
        -o backupchief-linux-amd64 .
fi

if [ "${TARGET}" = all ] || [ "${TARGET}" = linux-arm64 ]; then
    echo "Building Backup Chief agent for Linux arm64..."
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -mod=readonly -trimpath -buildvcs=false \
        -ldflags="-s -w -buildid= -X main.version=${VERSION}" \
        -o backupchief-linux-arm64 .
fi

if [ "${TARGET}" = all ] || [ "${TARGET}" = darwin-arm64 ]; then
    echo "Building Backup Chief agent for Darwin arm64..."
    CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -mod=readonly -trimpath -buildvcs=false \
        -ldflags="-s -w -buildid= -X main.version=${VERSION}" \
        -o backupchief-darwin-arm64 .
fi
