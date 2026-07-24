#!/usr/bin/env bash
# Stage 4 — Temporal PageRank: the graph is sliced by calendar quarter and
# PageRank is run independently on each slice, so influence can be tracked over
# time rather than collapsed into a single static score.
#
# Requires stage 2 output: the global top-20 defines which people to track.
# Outputs: ..._tracked      (top-5 global figures, score per quarter)
#          ..._quartertop   (whoever ranked highest within each quarter)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

hdfs_clean "$HDFS_BASE/pagerank_temporal_tracked"
hdfs_clean "$HDFS_BASE/pagerank_temporal_quartertop"

"$SPARK_HOME/bin/spark-submit" \
  --class com.enron.pagerank.PageRankTemporal \
  --master "$MASTER" \
  "$JAR" \
  "$HDFS_BASE/clean_edges.tsv" \
  "$HDFS_BASE/pagerank_basic_top20" \
  "$HDFS_BASE/pagerank_temporal" \
  "$ITERATIONS"
