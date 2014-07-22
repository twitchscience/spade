#!/bin/bash --
set -e -u -o pipefail

export GOPATH="/home/vagrant/go"
export SRCDIR="${GOPATH}/src/github.com/twitchscience/spade"
export PKGDIR="/tmp/pkg"
export DEPLOYDIR="${PKGDIR}/deploy"
export PATH=${PATH}:${GOPATH}/bin

mkdir -p ${DEPLOYDIR}/{bin,log}
cp ${SRCDIR}/build/scripts/* ${DEPLOYDIR}/bin
cp -R ${SRCDIR}/build/config ${DEPLOYDIR}

cd ${SRCDIR}
GOBIN=${DEPLOYDIR}/bin godep go install ./...

${DEPLOYDIR}/bin/transformer_dumper -outFile="${DEPLOYDIR}/config/transforms_available.json"

