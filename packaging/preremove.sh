#!/bin/sh
# Shared by the Debian prerm ($1 = remove | upgrade | deconfigure) and the RPM
# %preun ($1 = number of versions left after the transaction).
set -eu

case "${1:-}" in
    remove|0)
        ;;
    *)
        exit 0
        ;;
esac

if [ -d /run/systemd/system ]; then
    systemctl disable --now backupchief-updater.path
    systemctl stop backupchief-updater.service
    systemctl stop backupchief.service
    systemctl disable backupchief.service
else
    if [ -x /etc/init.d/backupchief ]; then
        /etc/init.d/backupchief stop || true
    fi
    if command -v update-rc.d >/dev/null 2>&1; then
        update-rc.d -f backupchief remove >/dev/null 2>&1 || true
    elif command -v chkconfig >/dev/null 2>&1; then
        chkconfig --del backupchief >/dev/null 2>&1 || true
    fi
fi
