#!/usr/bin/env bash
# Shared configuration for all pipeline scripts.
# Override any of these from your shell before sourcing, e.g.
#   HDFS_BASE=hdfs:///user/$USER/enron ./scripts/02_pagerank_basic.sh

export SPARK_HOME="${SPARK_HOME:-/opt/spark-3.0.0-bin-hadoop3.2}"
export HDFS_BASE="${HDFS_BASE:-hdfs:///user/$USER/enron}"
export LOCAL_DATA="${LOCAL_DATA:-./data}"
export JAR="${JAR:-target/enron-graph-analytics.jar}"
export ITERATIONS="${ITERATIONS:-10}"
export MASTER="${MASTER:-yarn}"

# Remove an HDFS output directory if it already exists (Spark refuses to overwrite).
hdfs_clean() {
  hdfs dfs -rm -r -f "$1" > /dev/null 2>&1 || true
}
