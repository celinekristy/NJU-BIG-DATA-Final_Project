#!/usr/bin/env bash
# Stage 5 — Behavioural clustering.
#   5a. Extract 7 per-person features from the raw corpus (single node).
#   5b. Cluster them with a hand-rolled Lloyd's-algorithm K-Means on Spark.
#
# Features: recvCount, outDegree, inDegree, nightRatio,
#           sendRecvRatio, activeMonths, avgSentPerMonth
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

java -cp "$JAR" com.enron.preprocess.FeatureExtract \
  "$LOCAL_DATA/emails.csv" \
  "$LOCAL_DATA/person_features.tsv"

hdfs dfs -put -f "$LOCAL_DATA/person_features.tsv" "$HDFS_BASE/"

for K in 3 4; do
  hdfs_clean "$HDFS_BASE/kmeans_k${K}"
  "$SPARK_HOME/bin/spark-submit" \
    --class com.enron.clustering.KMeans \
    --master "$MASTER" \
    "$JAR" \
    "$HDFS_BASE/person_features.tsv" \
    "$HDFS_BASE/kmeans_k${K}" \
    "$K" 20
done
