#!/bin/sh
# Shared by the Debian postrm ($1 = remove | purge | upgrade | …) and the RPM
# %postun ($1 = number of versions left after the transaction).
set -eu

case "${1:-}" in
    remove|purge|0)
        # The root unit the package installed, never one of the operator's
        override=/etc/systemd/system/backupchief.service
        if [ -f "$override" ] && grep -q '^# backupchief-root-override:' "$override"; then
            rm -f "$override"
        fi
        ;;
esac

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
fi
