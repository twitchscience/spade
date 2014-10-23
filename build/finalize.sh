#!/bin/bash --
set -e -u -o pipefail

BASEDIR="/opt/science"
SPADEDIR="${BASEDIR}/spade"
CONFDIR="${SPADEDIR}/config"
UPSTARTDIR="/etc/init"
PKGDIR="/tmp/pkg"

mv ${PKGDIR}/deploy ${SPADEDIR}

# Setup upstart
mv ${CONFDIR}/spade.conf ${UPSTARTDIR}/spade.conf
