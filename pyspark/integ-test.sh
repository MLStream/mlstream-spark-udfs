#!/bin/bash

set -eu

export PYTHONPATH=`pwd`

# TODO: get correctly the jar pth
# JAR="$(python mlstream_spark_udfs/main.py)"
JAR="/github/workspace/spark/target/scala-2.11/mlstream-spark-udfs_2.11-0.1-SNAPSHOT.jar"

spark-submit --jars "$JAR" \
--num-executors 1 \
--driver-memory 6g \
--executor-memory 6g \
tests/test_package.py
