#!/bin/bash --
set -e -u -o pipefail

SPADE_DIR="/opt/science/spade"
source "${SPADE_DIR}/bin/run_spade_base.sh"

exec ${SPADE_DIR}/bin/spade -spade_dir ${SPADE_DATA_DIR} \
  -config "${SPADE_DIR}/config/conf.json" \
  -stat_prefix "${STATSD_PREFIX}"
