#!/usr/bin/env bash
# Stage 3 — Weighted PageRank. Rank mass is split across out-edges in
# proportion to how often the pair actually corresponded, instead of uniformly.
#
# Two weighting modes are run for comparison:
#   log  — weight = 1 + ln(message_count), dampens high-volume mailing traffic
#   freq — weight = raw message_count
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

for MODE in log freq; do
  hdfs_clean "$HDFS_BASE/pagerank_weighted_${MODE}_top20"
  "$SPARK_HOME/bin/spark-submit" \
    --class com.enron.pagerank.PageRankWeighted \
    --master "$MASTER" \
    "$JAR" \
    "$HDFS_BASE/clean_edges.tsv" \
    "$HDFS_BASE/pagerank_weighted_${MODE}_top20" \
    "$ITERATIONS" \
    "$MODE"
done
