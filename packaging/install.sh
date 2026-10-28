#!/bin/bash
# Install the Backup Chief agent from the Backup Chief package repository:
#
#   curl -fsSL __PACKAGE_BASE_URL__/install.sh | sudo bash
#
# This configures the signed apt or yum/dnf repository under the names the
# agent's updater looks for, then installs the backupchief package. It does
# not set up the server: run the `backupchief setup <token>` command from
# Backup Chief afterwards. Running it again is safe.
#
# bin/generate-install-sh.sh fills in the repository URL.

set -euo pipefail

PACKAGE_BASE_URL="__PACKAGE_BASE_URL__"

KEYRING=/etc/apt/keyrings/backupchief.gpg
LEGACY_KEYRING=/etc/apt/trusted.gpg.d/backupchief.gpg
APT_SOURCES=/etc/apt/sources.list.d/backupchief.sources
APT_LIST=/etc/apt/sources.list.d/backupchief.list
YUM_REPO=/etc/yum.repos.d/backupchief.repo

fail() {
    echo "Backup Chief install: $*" >&2
    exit 1
}

if [ "$(id -u)" -ne 0 ]; then
    fail "run this script as root, for example with sudo bash."
fi

if ! command -v curl >/dev/null 2>&1; then
    fail "curl is required."
fi

# Detect architecture
MACHINE_ARCH="$(uname -m)"
case "${MACHINE_ARCH}" in
    x86_64|amd64)  RPM_ARCH="x86_64"; DEB_ARCH="amd64" ;;
    aarch64|arm64) RPM_ARCH="aarch64"; DEB_ARCH="arm64" ;;
    *) fail "unsupported architecture ${MACHINE_ARCH}." ;;
esac

# Detect OS. Derivatives such as CloudLinux name their parent distributions
# in ID_LIKE, so fall back to that when ID is unknown.
if [ ! -r /etc/os-release ]; then
    fail "cannot detect the distribution: /etc/os-release is missing."
fi
# shellcheck source=/dev/null
OS_ID="$(. /etc/os-release && printf '%s' "${ID:-}")"
# shellcheck source=/dev/null
OS_ID_LIKE="$(. /etc/os-release && printf '%s' "${ID_LIKE:-}")"

OS_FAMILY=""
for CANDIDATE in ${OS_ID} ${OS_ID_LIKE}; do
    case "${CANDIDATE}" in
        ubuntu|debian) OS_FAMILY="debian"; break ;;
        centos|rhel|rocky|almalinux|cloudlinux|fedora) OS_FAMILY="rhel"; break ;;
    esac
done

# The binary form of the ASCII-armored key, without depending on gpg (which
# minimal Debian installs lack): the armor's base64 body minus its headers
# and checksum line. No POSIX character classes: Ubuntu 16.04 and older
# ship mawk 1.3.3, which lacks them.
dearmor() {
    awk '
        /^-----BEGIN PGP PUBLIC KEY BLOCK-----/ { body = 1; headers = 1; next }
        /^-----END PGP PUBLIC KEY BLOCK-----/ { body = 0 }
        body && headers { if ($0 ~ /^[ \t\r]*$/) headers = 0; next }
        body && !/^=/ { print }
    ' | base64 -d
}

# Write the repository source for apt: deb822 with signed-by from apt 1.1,
# a one-line .list with the key in trusted.gpg.d before that.
write_apt_source() {
    local url="$1"

    if dpkg --compare-versions "${APT_VERSION:-0}" ge 1.1; then
        # Add repository using deb822 format, trusting the key for it only
        rm -f "${APT_LIST}" "${LEGACY_KEYRING}"
        printf '%s\n' \
            'Types: deb' \
            "URIs: ${url}/deb" \
            'Suites: stable' \
            'Components: main' \
            "Architectures: ${DEB_ARCH}" \
            "Signed-By: ${KEYRING}" \
            > "${APT_SOURCES}"
        SOURCES="${APT_SOURCES}"
    else
        # apt 1.0 (Ubuntu 14.04) reads neither deb822 files nor signed-by,
        # so the key goes into apt-key's trusted.gpg.d. The agent cannot
        # update itself with this apt (it needs 1.1 or later).
        rm -f "${APT_SOURCES}"
        install -m 0644 "${KEYRING}" "${LEGACY_KEYRING}"
        printf 'deb [arch=%s] %s/deb stable main\n' "${DEB_ARCH}" "${url}" > "${APT_LIST}"
        SOURCES="${APT_LIST}"
    fi
}

# Refresh the Backup Chief repository only, the way the agent's updater
# does, so unrelated broken repositories cannot get in the way. Succeeds
# when apt then offers the backupchief package.
refresh_apt_source() {
    apt-get update \
        -o Dir::Etc::sourcelist="${SOURCES}" \
        -o Dir::Etc::sourceparts=- \
        -o APT::Get::List-Cleanup=0 || true

    local candidate
    candidate="$(apt-cache policy backupchief 2>/dev/null | awk '/Candidate:/ { print $2 }')"
    [ -n "${candidate}" ] && [ "${candidate}" != "(none)" ]
}

install_apt() {
    echo "Setting up the Backup Chief repository for Debian/Ubuntu..."

    # Keep apt and its hooks from prompting, as the agent's updater does.
    # Piped into bash, nobody can answer a dialog such as needrestart's
    # "Pending kernel upgrade", and reading stdin would eat this script.
    export DEBIAN_FRONTEND=noninteractive APT_LISTCHANGES_FRONTEND=none NEEDRESTART_SUSPEND=1

    APT_VERSION="$(dpkg-query -W -f='${Version}' apt 2>/dev/null || true)"
    REPOSITORY_URL="${PACKAGE_BASE_URL}"

    # apt before 1.5 needs apt-transport-https for HTTPS repositories.
    if [ "${REPOSITORY_URL#https:}" != "${REPOSITORY_URL}" ] && [ ! -e /usr/lib/apt/methods/https ]; then
        echo "Installing apt-transport-https..."
        if ! apt-get update; then
            echo "apt-get update reported errors; continuing because unrelated repositories may be broken."
        fi
        if ! apt-get install -y apt-transport-https ca-certificates < /dev/null; then
            echo "Could not install apt-transport-https; apt will use the repository over HTTP."
            REPOSITORY_URL="http://${PACKAGE_BASE_URL#https://}"
        fi
    fi

    # Add GPG key. curl brings its own TLS, which works where apt's may not.
    install -d -m 0755 /etc/apt/keyrings
    curl -fsSL "${PACKAGE_BASE_URL}/gpg.key" | dearmor > "${KEYRING}.tmp"
    if [ ! -s "${KEYRING}.tmp" ]; then
        rm -f "${KEYRING}.tmp"
        fail "could not download the repository signing key."
    fi
    chmod 0644 "${KEYRING}.tmp"
    mv -f "${KEYRING}.tmp" "${KEYRING}"

    write_apt_source "${REPOSITORY_URL}"

    if ! refresh_apt_source; then
        if [ "${REPOSITORY_URL#https:}" = "${REPOSITORY_URL}" ]; then
            fail "apt could not read the Backup Chief repository at ${REPOSITORY_URL}."
        fi

        # apt's HTTPS can be too old for the repository host: Ubuntu 14.04's
        # GnuTLS has none of its ciphers. The repository's metadata is signed
        # and checked by apt, so HTTP only gives up privacy, not integrity.
        echo "apt could not reach the Backup Chief repository over HTTPS; using HTTP instead. Packages stay signature-checked."
        REPOSITORY_URL="http://${PACKAGE_BASE_URL#https://}"
        write_apt_source "${REPOSITORY_URL}"

        if ! refresh_apt_source; then
            fail "apt could not read the Backup Chief repository over HTTPS or HTTP."
        fi
    fi

    apt-get install -y backupchief < /dev/null
}

install_rpm() {
    echo "Setting up the Backup Chief repository for RHEL/CentOS/Rocky/Alma/CloudLinux..."

    # Add repository
    cat > "${YUM_REPO}.tmp" << EOF
[backupchief]
name=Backup Chief
baseurl=${PACKAGE_BASE_URL}/rpm/stable/${RPM_ARCH}
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=${PACKAGE_BASE_URL}/gpg.key
EOF
    chmod 0644 "${YUM_REPO}.tmp"
    mv -f "${YUM_REPO}.tmp" "${YUM_REPO}"

    if command -v dnf >/dev/null 2>&1; then
        PACKAGE_MANAGER=dnf
    elif command -v yum >/dev/null 2>&1; then
        PACKAGE_MANAGER=yum
    else
        fail "neither dnf nor yum found."
    fi

    # The package has no dependencies, so install from the Backup Chief
    # repository only: EOL releases such as CentOS 7 often have unreachable
    # mirrors configured.
    "${PACKAGE_MANAGER}" -y --disablerepo='*' --enablerepo=backupchief install backupchief
}

case "${OS_FAMILY}" in
    debian) install_apt ;;
    rhel) install_rpm ;;
    *)
        echo "Unsupported OS: ${OS_ID:-unknown}" >&2
        echo "Please install manually from https://github.com/chieftools/backupchief-agent/releases" >&2
        exit 1
        ;;
esac

echo ""
echo "Backup Chief agent installed: $(backupchief version)"
echo "Set up this server with the setup command shown in https://backup.chief.app:"
echo "  sudo backupchief setup <token>"
echo "Successful setup enables and starts the backupchief service."
