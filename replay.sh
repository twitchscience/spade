#!/bin/bash --
set -euo pipefail

MASTER_IP=$1 && shift
REDSHIFT_TARGET=$1 && shift
START_TIME=$1 && shift
END_TIME=$1 && shift

SPARK_COMMAND=$(cat <<SPARK_COMMAND
  /opt/spark/bin/spark-submit
  --master spark://${MASTER_IP}:7077
  /opt/science/replay/bin/replay.py
  "${START_TIME}" "${END_TIME}"
  $@
  --rsurl="${REDSHIFT_TARGET}"
  2>&1 | tee -a replay.log
SPARK_COMMAND
)

exec ssh -q ${MASTER_IP} ${SPARK_COMMAND}
