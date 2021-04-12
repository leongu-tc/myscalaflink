#!/usr/bin/env bash

# some Java parameters
if [ "$FLINK_YARN_HOME" != "" ]; then
  #echo "run flink in $FLINK_HOME"
  FLINK_YARN_HOME=$FLINK_YARN_HOME
fi

$FLINK_YARN_HOME/bin/flink run \
-d -m yarn-cluster -p 2 \
-ynm sdp_flink_kafka_console \
-yjm 2g -ys 1 -ytm 2g \
-j myscalaflink-0.0.1.jar \
-c leongu.business.SqlSubmit --sqlfile