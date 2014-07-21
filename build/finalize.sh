#!/bin/bash --
set -e -u -o pipefail

BASEDIR="/opt/science"
SPADEDIR="${BASEDIR}/spade"
CONFDIR="${SPADEDIR}/config"
UPSTARTDIR="/etc/init"
PKGDIR="/tmp/pkg"

mv ${PKGDIR}/deploy ${SPADEDIR}
ln -s /mnt ${SPADEDIR}/data

# Setup upstart
mv ${CONFDIR}/spade.conf ${UPSTARTDIR}/spade.conf
