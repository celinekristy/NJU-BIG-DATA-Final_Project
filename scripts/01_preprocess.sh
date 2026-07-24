#!/usr/bin/env bash
# Stage 1 — Parse the raw Enron CSV into a graph edge list.
#
# Input :  data/emails.csv        (Kaggle: wcukierski/enron-email-dataset)
# Output:  data/clean_edges.tsv   sender <TAB> recipient <TAB> type(to|cc) <TAB> yyyy-MM
#          data/persons.txt       one unique email address per line
#
# Runs on a single node — the CSV is read sequentially because message bodies
# contain embedded newlines and quotes that a naive line split would corrupt.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

java -cp "$JAR" com.enron.preprocess.Preprocess \
  "$LOCAL_DATA/emails.csv" \
  "$LOCAL_DATA/clean_edges.tsv" \
  "$LOCAL_DATA/persons.txt"

hdfs dfs -mkdir -p "$HDFS_BASE"
hdfs dfs -put -f "$LOCAL_DATA/clean_edges.tsv" "$HDFS_BASE/"
