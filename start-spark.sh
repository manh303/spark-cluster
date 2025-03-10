#!/bin/bash

if [ "$SPARK_MODE" == "master" ]; then
    $SPARK_HOME/sbin/start-master.sh
elif [ "$SPARK_MODE" == "worker" ]; then
    $SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER
fi

tail -f $SPARK_HOME/logs/*