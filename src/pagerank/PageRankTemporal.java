package com.enron.pagerank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PageRankTemporal
{
    private static String parseToEmail(String line)
    {
        String[] parts = line.split("\t");
        if(parts.length >= 2)
        {
            return parts[1].trim();
        }
        return "";
    }

    private static String toQuarter(String ym)
    {
        try {
            String[] parts = ym.split("-");
            int month = Integer.parseInt(parts[1].trim());
            int quarter = (month - 1) / 3 + 1;
            return parts[0] + "-Q" + quarter;
        }
        catch(Exception e) {
            return "";
        }
    }

    private static JavaPairRDD<String, Double> runPageRank(JavaPairRDD<String, String> quarterEdges, int iterations)
    {
        final double d = 0.85; // damping factor

        // -------------------------- 获取全部节点 nodes ----------------------------
        JavaRDD<String> nodes = quarterEdges
            .keys()
            .union(quarterEdges.values())
            .distinct()
            .cache();
        long nodeCount = nodes.count();

        // ------------------------- 构建当前季度邻接表 -----------------------------
        // sender -> [receiver1, receiver2, ...]
        JavaPairRDD<String, Iterable<String>> links = quarterEdges
            .groupByKey()
            .cache();

        // -------------------------- 标记有出边的节点，用于识别 Dangling Node ----------------
        JavaPairRDD<String, Integer> hasOutLinks = links.keys()
            .mapToPair(node -> new Tuple2<>(node, 1))
            .cache();

        // -------------------------- 初始化 PageRank --------------------------------------
        JavaPairRDD<String, Double> ranks = nodes
            .mapToPair(node -> new Tuple2<>(node, 1.0 / nodeCount))
            .cache();
        ranks.count(); // 这个操作是为了可以让 ranks 里面立即生效，使得  ranks 可以先被缓存起来

        final double baseScore = (1.0 - d) / nodeCount;

        // -------------------------- 迭代计算 PageRank ------------------------------------
        for(int i = 0; i < iterations; ++i)
        {
            // ---------------------- 计算没有出边的 rank 总和 --------------------------
            double danglingMass = ranks
                .subtractByKey(hasOutLinks)
                .values()
                .fold(0.0, (a,b) -> a + b);

            final double danglingShare = danglingMass / nodeCount;

            // --------------------- 计算普通节点的贡献（ contributions ）-------------------
            JavaPairRDD<String, Double> contributions = links
                .join(ranks)
                .flatMapToPair(item -> {
                    Iterable<String> outLinks = item._2._1;
                    double rank = item._2._2;

                    List<String> outList = new ArrayList<>();
                    for(String receiver : outLinks)
                    {
                        outList.add(receiver);
                    }
                    
                    List<Tuple2<String, Double>> result = new ArrayList<>();
                    if(outList.size() > 0)
                    {
                        double share = rank / outList.size();
                        
                        for(String receiver : outList)
                        {
                            result.add(new Tuple2<>(receiver, share));
                        }
                    }
                    return result.iterator();
                })
                .reduceByKey((a, b) -> a + b);
            
            // -------------------------- 更新每一个节点的 PageRank -------------------------
            JavaPairRDD<String, Double> oldranks = ranks;
            ranks = nodes
                    .mapToPair(node -> new Tuple2<>(node, baseScore + d * danglingShare))
                    .leftOuterJoin(contributions)
                    .mapToPair(item -> {
                        String currentNode = item._1;
                        Double baseRankPart = item._2._1;
                        Optional<Double> receivedContribution = item._2._2;

                        double contribution = 0.0;
                        if(receivedContribution.isPresent())
                        {
                            contribution = receivedContribution.get();
                        }
                        else
                        {
                            contribution = 0.0;
                        }
                        double updatedPageRank = baseRankPart + d * contribution;
                        return new Tuple2<>(currentNode, updatedPageRank);
                    })
                    .cache();
            
            ranks.count();
            oldranks.unpersist();
            System.out.println("Iteration" + (i + 1) + "finished.");
        }
        nodes.unpersist();
        links.unpersist();
        hasOutLinks.unpersist();

        return ranks;
    }
    public static void main(String[] args)
    {
        if(args.length < 4)
        {
            System.err.println("Usage: PageRankTemporal <input_edges> <task1_result_path> <output_path> [iterations]");
            System.exit(1);
        }

        String inputEdgePath = args[0];
        String task1ResultPath = args[1];
        String outputPath = args[2];

        int iterations = args.length >= 4 ? Integer.parseInt(args[3]) : 10;

        SparkConf conf = new SparkConf().setAppName("Task3 Dynamic PageRank By Quarter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> task1Lines = sc.textFile(task1ResultPath); // 获取 Task1 Result 中的 Top5 节点
        List<String> top5Nodes = task1Lines
            .map(line -> parseToEmail(line))
            .filter(email -> email.length() > 0)
            .take(5);
        
        if(top5Nodes.size() == 0)
        {
            System.err.println("No Top 5 nodes found from Task1 result.");
            System.exit(1);
        }
        System.out.println("Tracked Top5 Nodes: ");
        for(String node : top5Nodes)
        {
            System.out.println(node);
        }

        // -------------- 读取 clean_edges.tsv，构造 quarter -> (sender, receiver) -----------------
        JavaPairRDD<String, Tuple2<String, String>> edgeWithQuarter = sc
            .textFile(inputEdgePath)
            .mapToPair(line -> {
                String[] parts = line.split("\t");
                String src = parts[0].trim();
                String dst = parts[1].trim();
                String quarter = toQuarter(parts[3].trim());

                return new Tuple2<>(quarter, new Tuple2<>(src, dst));
            })
            .filter(element -> !element._2._1.equals(element._2._2)
                                && element._1.length() > 0)
            .cache();
        
        // ---------------------- collecting quarters and sort ------------------------------
        List<String> quarters = new ArrayList<>(edgeWithQuarter.keys().distinct().collect());

        java.util.Collections.sort(quarters);
        System.out.println("所有季度窗口： " + quarters);

        // --------------------------------- 准备输出内容 -------------------------------------
        List<String> trackedSeriesLines = new ArrayList<>(); // trackedSeriesLines： 任务一 Top5 邮箱在每个季度的 PageRank，用于画折线图。
        trackedSeriesLines.add("quarter\temail\tpagerank");

        List<String> quarterTopLines = new ArrayList<>(); // quarterTopLines: 每个季度自己的 PageRank Top20，用于报告分析和检查。
        quarterTopLines.add("quarter\trank\temail\tpagerank");

        // ----------------------------对每个季度单独计算 PageRank -----------------------------
        for(String quarter: quarters)
        {
            final String currentQuarter = quarter;

            JavaPairRDD<String, String> quarterEdges = edgeWithQuarter
                .filter(element -> currentQuarter.equals(element._1))
                .mapToPair(element -> element._2())
                .distinct()
                .cache();
            
            long edgeCount = quarterEdges.count();

            if (edgeCount == 0) // 检查当前季度有没有边
            {
                quarterEdges.unpersist();
                continue;
            }

            System.out.println("Computing PageRank for quarter: " + currentQuarter + ", edgeCount = " + edgeCount);

            // -------------------------- 当前季度单独运行标准 PageRank ----------------------------
            JavaPairRDD<String, Double> ranks = runPageRank(quarterEdges, iterations);

            // --------------------------- 把当前季度 PageRank 结果收集成 Map ----------------------
            // ranks: email -> pagerank
            // prMap: {email = pagerank}
            Map<String, Double> prMap = ranks.collectAsMap();

            //  填追踪序列：top5 在这个季度的 PR（没出现记 0）
            for (String person : top5Nodes) 
            {
                double pr = prMap.containsKey(person) ? prMap.get(person) : 0.0;
                trackedSeriesLines.add(currentQuarter + "\t" + person + "\t" + pr);
            }

            // -------------------------- 输出 Top 20 影响力人物 -----------------------------
            List<Tuple2<Double, String>> top20 = ranks
                .mapToPair(item -> new Tuple2<>(item._2, item._1)) // (人,分值) → (分值,人)，方便按分值排序
                .sortByKey(false) // false = 降序
                .take(20);

            System.out.println("===== Top 20 影响力人物 =====");
            int rankNo = 1;
            for (Tuple2<Double, String> element : top20)
            {
                // element._2 = 人, element._1 = 分值
                quarterTopLines.add(currentQuarter + "\t" + rankNo + "\t" + element._2 + "\t" + element._1);
                rankNo++;
            }

            System.out.println("季度 " + currentQuarter + " 完成");
        }

        // ============ 改动3：两份分别写到不同目录 ============
        sc.parallelize(trackedSeriesLines, 1).saveAsTextFile(outputPath + "_tracked");
        sc.parallelize(quarterTopLines, 1).saveAsTextFile(outputPath + "_quartertop");

        System.out.println("Task3 finished.");
        System.out.println("  追踪序列: " + outputPath + "_tracked");
        System.out.println("  各季度Top20: " + outputPath + "_quartertop");
        sc.stop();
    }
}