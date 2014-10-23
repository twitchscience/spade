#!/bin/bash --
set -e -u -o pipefail

BASEDIR="/opt/science"
SPADEDIR="${BASEDIR}/spade"
CONFDIR="${SPADEDIR}/config"
UPSTARTDIR="/etc/init"
PKGDIR="/tmp/pkg"

mv ${PKGDIR}/deploy ${SPADEDIR}

# Setup upstart
mv ${CONFDIR}/mount_spade_volumes.conf ${UPSTARTDIR}/mount_spade_volumes.conf
mv ${CONFDIR}/spade.conf ${UPSTARTDIR}/spade.conf
