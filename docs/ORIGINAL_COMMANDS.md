# Original run commands

Preserved for the record. These are the commands as used on the university's
Hadoop/YARN teaching cluster, where the project was actually executed. They are
kept for provenance — **use `scripts/` instead**, which does the same work
without hardcoded cluster paths.

Environment: Spark 3.0.0 (`/home/workspace/spark-3.0.0-bin-hadoop3.2`),
Hadoop 3.2.1, Commons CSV 1.0 taken from the Hadoop distribution's
timelineservice libs.

> Note: these will not run verbatim against the source files as submitted.
> Each file was given a `new_` filename prefix while the class inside kept its
> unprefixed name, so `javac new_PageRankTask1.java` and `--class new_PageRankTask1`
> both fail. See NOTES.md.

## Preprocessing

```bash
cd ~/final_project
CSVJAR=/home/workspace/hadoop-3.2.1/share/hadoop/yarn/timelineservice/lib/commons-csv-1.0.jar

javac -encoding UTF-8 -cp $CSVJAR -d classes new_Preprocess.java
java -cp classes:$CSVJAR new_Preprocess emails.csv new_clean_edges.tsv new_persons.txt
```

## Task 1 — standard PageRank

```bash
SPARK_HOME=/home/workspace/spark-3.0.0-bin-hadoop3.2

javac -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d classes new_PageRankTask1.java
cd classes && jar cf ../new_task1_pagerank.jar . && cd ~/final_project

hdfs dfs -rm -r /user/225220002a/final_project/task1_pagerank_top20

$SPARK_HOME/bin/spark-submit \
  --class new_PageRankTask1 --master yarn \
  new_task1_pagerank.jar \
  hdfs:///user/225220002a/final_project/new_clean_edges.tsv \
  hdfs:///user/225220002a/final_project/task1_pagerank_top20 \
  10
```

## Task 2 — weighted PageRank (log and freq)

```bash
javac -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d classes new_PageRankTask2.java
cd classes && jar cf ../new_task2_pagerank.jar . && cd ~/final_project

# log smoothing (default)
hdfs dfs -rm -r /user/225220002a/final_project/task2_log_top20
$SPARK_HOME/bin/spark-submit --class new_PageRankTask2 --master yarn \
  new_task2_pagerank.jar \
  hdfs:///user/225220002a/final_project/new_clean_edges.tsv \
  hdfs:///user/225220002a/final_project/task2_log_top20 \
  10 log

# raw frequency
hdfs dfs -rm -r /user/225220002a/final_project/task2_freq_top20
$SPARK_HOME/bin/spark-submit --class new_PageRankTask2 --master yarn \
  new_task2_pagerank.jar \
  hdfs:///user/225220002a/final_project/new_clean_edges.tsv \
  hdfs:///user/225220002a/final_project/task2_freq_top20 \
  10 freq
```

## Task 3 — temporal PageRank

Takes the Task 1 output as a second input: the global top-20 defines who to track.

```bash
javac -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d classes new_PageRankTask3.java
cd classes && jar cf ../new_task3_pagerank.jar . && cd ~/final_project

hdfs dfs -rm -r /user/225220002a/final_project/task3_timeseries_tracked
hdfs dfs -rm -r /user/225220002a/final_project/task3_timeseries_quartertop

$SPARK_HOME/bin/spark-submit --class new_PageRankTask3 --master yarn \
  new_task3_pagerank.jar \
  hdfs:///user/225220002a/final_project/new_clean_edges.tsv \
  hdfs:///user/225220002a/final_project/task1_pagerank_top20 \
  hdfs:///user/225220002a/final_project/task3_timeseries \
  10
```

Output suffixes `_tracked` and `_quartertop` are appended by the program.

## Task 4 — feature extraction and K-Means

```bash
CSVJAR=/home/workspace/hadoop-3.2.1/share/hadoop/yarn/timelineservice/lib/commons-csv-1.0.jar
javac -encoding UTF-8 -cp $CSVJAR -d classes new_FeatureExtract.java
java -cp classes:$CSVJAR new_FeatureExtract emails.csv person_features.tsv

hdfs dfs -put -f person_features.tsv /user/225220002a/final_project/

javac -encoding UTF-8 -cp "$SPARK_HOME/jars/*" -d classes new_KMeansTask4.java
cd classes && jar cf ../new_kmeans.jar . && cd ~/final_project

# K=3
hdfs dfs -rm -r /user/225220002a/final_project/task4_k3
$SPARK_HOME/bin/spark-submit --class new_KMeansTask4 --master yarn new_kmeans.jar \
  hdfs:///user/225220002a/final_project/person_features.tsv \
  hdfs:///user/225220002a/final_project/task4_k3 3 20

# K=4
hdfs dfs -rm -r /user/225220002a/final_project/task4_k4
$SPARK_HOME/bin/spark-submit --class new_KMeansTask4 --master yarn new_kmeans.jar \
  hdfs:///user/225220002a/final_project/person_features.tsv \
  hdfs:///user/225220002a/final_project/task4_k4 4 20
```
