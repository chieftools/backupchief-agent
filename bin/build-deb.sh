#!/bin/bash

set -e

PACKAGE_NAME="backupchief"
VERSION="${1:-0.0.0}"
ARCH="${2:-amd64}"
MAINTAINER="Backup Chief Team <hello@chief.app>"
DESCRIPTION="Backup Chief Linux backup agent."

export GOTOOLCHAIN="go$(awk '/^go / { print $2 }' go.mod)"
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-0}"

if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Version must use numeric X.Y.Z format." >&2
    exit 1
fi

case "${ARCH}" in
    amd64)
        GOARCH="amd64"
        ;;
    arm64)
        GOARCH="arm64"
        ;;
    *)
        echo "Unsupported architecture: ${ARCH}" >&2
        exit 1
        ;;
esac

echo "Building Backup Chief agent Debian package v${VERSION}"

BUILD_DIR="$(mktemp -d)"
DEB_DIR="${BUILD_DIR}/${PACKAGE_NAME}_${VERSION}_${ARCH}"
trap 'rm -rf "${BUILD_DIR}"' EXIT

echo "Creating package structure..."
mkdir -p "${DEB_DIR}/DEBIAN"
mkdir -p "${DEB_DIR}/usr/bin"
mkdir -p "${DEB_DIR}/etc/backupchief"
mkdir -p "${DEB_DIR}/usr/lib/systemd/system"
mkdir -p "${DEB_DIR}/usr/share/doc/backupchief"

./bin/build.sh "${VERSION}" "linux-${GOARCH}"

echo "Building Backup Chief agent binary..."
install -m 0755 "backupchief-linux-${GOARCH}" "${DEB_DIR}/usr/bin/backupchief"

echo "Installing configuration, service unit, and license..."
install -m 0640 packaging/config.json "${DEB_DIR}/etc/backupchief/config.json"
chmod 0750 "${DEB_DIR}/etc/backupchief"
install -m 0644 packaging/backupchief.service "${DEB_DIR}/usr/lib/systemd/system/backupchief.service"
install -m 0644 LICENSE "${DEB_DIR}/usr/share/doc/backupchief/copyright"
install -m 0644 restic/LICENSE "${DEB_DIR}/usr/share/doc/backupchief/restic-LICENSE"
install -m 0644 rclone/COPYING "${DEB_DIR}/usr/share/doc/backupchief/rclone-COPYING"
install -m 0644 config.schema.json "${DEB_DIR}/usr/share/doc/backupchief/config.schema.json"

echo "Creating package metadata..."
cat > "${DEB_DIR}/DEBIAN/control" << EOF
Package: ${PACKAGE_NAME}
Version: ${VERSION}
Section: admin
Priority: optional
Architecture: ${ARCH}
Maintainer: ${MAINTAINER}
Homepage: https://backup.chief.app
Description: ${DESCRIPTION}
 Run from a local configuration or connect the agent to the Backup Chief control plane.
Depends: passwd, systemd
EOF

cat > "${DEB_DIR}/DEBIAN/conffiles" << EOF
/etc/backupchief/config.json
EOF

echo "Installing package lifecycle scripts..."
install -m 0755 packaging/postinstall.sh "${DEB_DIR}/DEBIAN/postinst"

cat > "${DEB_DIR}/DEBIAN/prerm" << 'EOF'
#!/bin/sh
set -eu

if [ "$1" = "remove" ] && [ -d /run/systemd/system ]; then
    systemctl stop backupchief.service
    systemctl disable backupchief.service
fi
EOF
chmod 0755 "${DEB_DIR}/DEBIAN/prerm"

cat > "${DEB_DIR}/DEBIAN/postrm" << 'EOF'
#!/bin/sh
set -eu

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
fi
EOF
chmod 0755 "${DEB_DIR}/DEBIAN/postrm"

echo "Building Debian package..."
PACKAGE_FILE="${PACKAGE_NAME}_${VERSION}_${ARCH}.deb"
dpkg-deb --build --root-owner-group -Zgzip "${DEB_DIR}" "${PACKAGE_FILE}"

echo "Package built successfully: ${PACKAGE_FILE}"
dpkg-deb --info "${PACKAGE_FILE}"
