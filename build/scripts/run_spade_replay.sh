#!/bin/bash
set -e -u -o pipefail

RUN_TAG="$1"

SPADE_DIR="/opt/science/spade"
source "${SPADE_DIR}/bin/run_spade_base.sh"

exec ${SPADE_DIR}/bin/spade -replay \
  -run_tag "${RUN_TAG}" \
  -spade_dir ${SPADE_DATA_DIR} \
  -config "${SPADE_DIR}/config/conf.json" \
  -audit_log_dir "${SPADE_LOG_DIR}" \
  -stat_prefix "${STATSD_PREFIX}"
