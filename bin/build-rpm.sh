#!/bin/bash

set -e

PACKAGE_NAME="backupchief"
VERSION="${1:-0.0.0}"
RELEASE="${2:-1}"
ARCH="${3:-x86_64}"
DESCRIPTION="Backup Chief Linux backup agent."
URL="https://backup.chief.app"
MAINTAINER="Backup Chief Team <hello@chief.app>"

export GOTOOLCHAIN="go$(awk '/^go / { print $2 }' go.mod)"
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-0}"

if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Version must use numeric X.Y.Z format." >&2
    exit 1
fi

if [[ ! "${RELEASE}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Release must be a positive integer." >&2
    exit 1
fi

case "${ARCH}" in
    x86_64)
        GOARCH="amd64"
        ;;
    aarch64)
        GOARCH="arm64"
        ;;
    *)
        echo "Unsupported architecture: ${ARCH}" >&2
        exit 1
        ;;
esac

echo "Building Backup Chief agent RPM package v${VERSION}-${RELEASE}"

BUILD_DIR="$(mktemp -d)"
RPMBUILD_DIR="${BUILD_DIR}/rpmbuild"
trap 'rm -rf "${BUILD_DIR}"' EXIT

echo "Creating RPM build structure..."
mkdir -p "${RPMBUILD_DIR}"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

./bin/build.sh "${VERSION}" "linux-${GOARCH}"

echo "Building Backup Chief agent binary..."
install -m 0755 "backupchief-linux-${GOARCH}" "${RPMBUILD_DIR}/SOURCES/backupchief"

echo "Installing configuration, service unit, and license..."
cp packaging/config.json "${RPMBUILD_DIR}/SOURCES/"
cp packaging/backupchief.service "${RPMBUILD_DIR}/SOURCES/"
cp packaging/backupchief-updater.service "${RPMBUILD_DIR}/SOURCES/"
cp packaging/backupchief-updater.path "${RPMBUILD_DIR}/SOURCES/"
cp packaging/postinstall.sh "${RPMBUILD_DIR}/SOURCES/"
install -m 0644 LICENSE "${RPMBUILD_DIR}/SOURCES/LICENSE"
install -m 0644 restic/LICENSE "${RPMBUILD_DIR}/SOURCES/restic-LICENSE"
install -m 0644 rclone/COPYING "${RPMBUILD_DIR}/SOURCES/rclone-COPYING"
install -m 0644 config.schema.json "${RPMBUILD_DIR}/SOURCES/config.schema.json"

echo "Creating RPM spec file..."
cat > "${RPMBUILD_DIR}/SPECS/backupchief.spec" << EOF
Name:           ${PACKAGE_NAME}
Version:        ${VERSION}
Release:        ${RELEASE}
Summary:        ${DESCRIPTION}

License:        Apache-2.0
URL:            ${URL}
Packager:       ${MAINTAINER}
Source0:        config.json
Source1:        backupchief.service
Source2:        LICENSE
Source3:        restic-LICENSE
Source4:        rclone-COPYING
Source5:        config.schema.json
Source6:        backupchief-updater.service
Source7:        backupchief-updater.path

%global debug_package %{nil}
%global _build_id_links none
%global __os_install_post %{nil}

AutoReqProv:    no
Requires:       shadow-utils
Requires:       systemd

%description
Run from a local configuration or connect the agent to the Backup Chief control plane.

%install
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/etc/backupchief
mkdir -p %{buildroot}/usr/lib/systemd/system
mkdir -p %{buildroot}/usr/share/licenses/backupchief

install -m 0755 %{_sourcedir}/backupchief %{buildroot}/usr/bin/backupchief
install -m 0640 %{SOURCE0} %{buildroot}/etc/backupchief/config.json
install -m 0644 %{SOURCE1} %{buildroot}/usr/lib/systemd/system/backupchief.service
install -m 0644 %{SOURCE6} %{buildroot}/usr/lib/systemd/system/backupchief-updater.service
install -m 0644 %{SOURCE7} %{buildroot}/usr/lib/systemd/system/backupchief-updater.path
install -m 0644 %{SOURCE2} %{buildroot}/usr/share/licenses/backupchief/LICENSE
install -m 0644 %{SOURCE3} %{buildroot}/usr/share/licenses/backupchief/restic-LICENSE
install -m 0644 %{SOURCE4} %{buildroot}/usr/share/licenses/backupchief/rclone-COPYING
install -m 0644 %{SOURCE5} %{buildroot}/usr/share/licenses/backupchief/config.schema.json

%files
%license %attr(0644, root, root) /usr/share/licenses/backupchief/LICENSE
%license %attr(0644, root, root) /usr/share/licenses/backupchief/restic-LICENSE
%license %attr(0644, root, root) /usr/share/licenses/backupchief/rclone-COPYING
%doc %attr(0644, root, root) /usr/share/licenses/backupchief/config.schema.json
%attr(0755, root, root) /usr/bin/backupchief
%config(noreplace) %attr(0640, root, root) /etc/backupchief/config.json
%attr(0750, root, root) %dir /etc/backupchief
%attr(0644, root, root) /usr/lib/systemd/system/backupchief.service
%attr(0644, root, root) /usr/lib/systemd/system/backupchief-updater.service
%attr(0644, root, root) /usr/lib/systemd/system/backupchief-updater.path

%post -f %{_sourcedir}/postinstall.sh

%preun
set -eu

if [ "\$1" -eq 0 ] && [ -d /run/systemd/system ]; then
    systemctl disable --now backupchief-updater.path
    systemctl stop backupchief-updater.service
    systemctl stop backupchief.service
    systemctl disable backupchief.service
fi

%postun
set -eu

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
fi
EOF

echo "Building RPM package..."
rpmbuild --define "_topdir ${RPMBUILD_DIR}" \
    --define "_tmppath ${BUILD_DIR}" \
    --define '_rpmformat 4' \
    --define '_binary_payload w9.gzdio' \
    --define '_buildhost reproducible' \
    --define 'use_source_date_epoch_as_buildtime 1' \
    --define 'clamp_mtime_to_source_date_epoch 1' \
    --target "${ARCH}-linux" \
    -bb "${RPMBUILD_DIR}/SPECS/backupchief.spec"

PACKAGE_FILE="${PACKAGE_NAME}-${VERSION}-${RELEASE}.${ARCH}.rpm"
mv "${RPMBUILD_DIR}/RPMS/${ARCH}/${PACKAGE_FILE}" .

echo "Package built successfully: ${PACKAGE_FILE}"
rpm -qip "${PACKAGE_FILE}"
