#!/bin/bash
set -e -u -o pipefail

RUN_TAG=$1 && shift

SPADE_DIR="/opt/science/replay"
set -a
source "${SPADE_DIR}/bin/run_spade_base.sh"
set +a

exec ${SPADE_DIR}/bin/spade -replay \
  -run_tag "${RUN_TAG}" \
  -spade_dir ${SPADE_DATA_DIR} \
  -config "${SPADE_DIR}/config/conf.json" \
  -stat_prefix "${STATSD_PREFIX}"
