#!/bin/sh
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

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload

    case "${1:-}" in
        configure)
            if [ -n "${2:-}" ]; then
                systemctl try-restart backupchief.service
            fi
            ;;
        2|3|4|5|6|7|8|9)
            systemctl try-restart backupchief.service
            ;;
    esac
fi

echo 'Backup Chief agent installed. Run sudo backupchief setup <token> to connect this server, or configure /etc/backupchief/config.json. An inactive service remains inactive.'
