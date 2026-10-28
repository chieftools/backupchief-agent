#!/bin/sh
# Shared by the Debian postinst ($1 = configure, $2 = the upgraded version) and
# the RPM %post ($1 = number of installed versions).
set -eu

if ! getent group backupchief >/dev/null; then
    groupadd --system backupchief
fi

if ! getent passwd backupchief >/dev/null; then
    useradd --system --gid backupchief --home-dir /var/lib/backupchief \
        --no-create-home --shell /sbin/nologin backupchief
fi

install -d -m 0750 -o root -g backupchief /etc/backupchief
chown root:backupchief /etc/backupchief/config.json
chmod 0640 /etc/backupchief/config.json
install -d -m 0700 -o backupchief -g backupchief \
    /var/lib/backupchief /var/log/backupchief

upgrade=false
case "${1:-}" in
    configure)
        if [ -n "${2:-}" ]; then
            upgrade=true
        fi
        ;;
    2|3|4|5|6|7|8|9)
        upgrade=true
        ;;
esac

# Pick the init system at runtime; the package does not depend on systemd.
if [ -d /run/systemd/system ]; then
    # systemd before 229 (such as CentOS 7's 219) has no ambient capabilities,
    # so the agent could not read the files it backs up: run it as root there.
    override=/etc/systemd/system/backupchief.service
    version="$(systemctl --version 2>/dev/null | awk 'NR == 1 { print $2 }')"
    case "$version" in ''|*[!0-9]*) version=0 ;; esac
    if [ "$version" -lt 229 ]; then
        install -m 0644 /usr/share/backupchief/backupchief-root.service "$override"
    elif [ -f "$override" ] && grep -q '^# backupchief-root-override:' "$override"; then
        rm -f "$override"
    fi

    systemctl daemon-reload
    systemctl enable --now backupchief-updater.path

    if [ "$upgrade" = true ]; then
        systemctl try-restart backupchief.service
    fi
elif [ -x /etc/init.d/backupchief ]; then
    # Without systemd the agent runs as root with the init script, and starts
    # the updater itself.
    if [ "$upgrade" = true ]; then
        /etc/init.d/backupchief try-restart || true
    fi
fi

echo 'Backup Chief agent installed. Run sudo backupchief setup <token> to connect this server, or configure /etc/backupchief/config.json. An inactive service remains inactive.'
