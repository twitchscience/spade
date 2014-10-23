#!/bin/bash --
set -e -u -o pipefail

SPADE_DIR="/opt/science/spade"

/usr/sbin/ebslvm vgebs lvebs /mnt
ln -s /mnt ${SPADE_DIR}/data

export GOMAXPROCS="3" # we need 3 or more for spade to run well..

export GEO_IP_DB="${SPADE_DIR}/config/GeoLiteCity.dat"
export ASN_IP_DB="${SPADE_DIR}/config/GeoIPASNum.dat"
SPADE_DATA_DIR="${SPADE_DIR}/data"
SPADE_LOG_DIR="${SPADE_DIR}/log"
export STATSD_HOSTPORT="localhost:8125"

eval "$(curl 169.254.169.254/latest/user-data/)"

export HOST="$(curl 169.254.169.254/latest/meta-data/hostname)"

STATSD_PREFIX="${CLOUD_APP}.${CLOUD_DEV_PHASE:-${CLOUD_ENVIRONMENT}}.${EC2_REGION}.${CLOUD_AUTO_SCALE_GROUP##*-}"
mkdir -p ${SPADE_DATA_DIR}/spade_logging ${SPADE_DATA_DIR}/events ${SPADE_DATA_DIR}/upload

exec ${SPADE_DIR}/bin/spade -gzipped -spade_dir ${SPADE_DATA_DIR} \
  -config_url ${BLUEPRINT_URL} \
  -audit_log_dir ${SPADE_LOG_DIR} \
  -stat_prefix ${STATSD_PREFIX}
