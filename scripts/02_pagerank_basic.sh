#!/usr/bin/env bash
# Stage 2 — Unweighted PageRank over the sender -> recipient graph.
# Output: $HDFS_BASE/pagerank_basic_top20
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

hdfs_clean "$HDFS_BASE/pagerank_basic_top20"

"$SPARK_HOME/bin/spark-submit" \
  --class com.enron.pagerank.PageRankBasic \
  --master "$MASTER" \
  "$JAR" \
  "$HDFS_BASE/clean_edges.tsv" \
  "$HDFS_BASE/pagerank_basic_top20" \
  "$ITERATIONS"
