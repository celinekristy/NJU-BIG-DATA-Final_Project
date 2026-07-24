package com.enron.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KMeans 
{
    static final int DIM = 7; // 7 个特征

    // 欧氏距离平方（比较大小用平方即可，省一次开方）
    static double dist2(double[] a, double[] b) 
    {
        double s = 0;
        for (int i = 0; i < DIM; i++) 
        { 
            double d = a[i] - b[i]; s += d * d; 
        }
        return s;
    }

    // 找最近中心的下标
    static int nearest(double[] p, List<double[]> centers) 
    {
        int best = 0; double bd = Double.MAX_VALUE;
        for (int c = 0; c < centers.size(); c++) 
        {
            double d = dist2(p, centers.get(c));
            if (d < bd) { bd = d; best = c; }
        }
        return best;
    }

    public static void main(String[] args)
    {
        if(args.length < 2)
        {
            System.err.println("Usage: KMeans <features.tsv> <output> [K] [iterations]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int K = args.length >= 3 ? Integer.parseInt(args[2]) : 3;
        int iterations = args.length >= 4 ? Integer.parseInt(args[3]) : 20;

        SparkConf conf = new SparkConf().setAppName("KMeans");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ---------------------- 读取特征表 person_features.tsv --------------------------
        JavaPairRDD<String, double[]> rawData = sc.textFile(inputPath)
            .filter(line -> !line.startsWith("email"))
            .mapToPair(line -> {
                String[] parts = line.split("\t");
                double[] features = new double[DIM];

                for(int i = 0; i < DIM; ++i)
                {
                    features[i] = Double.parseDouble(parts[i+1]);
                }
                return new Tuple2<>(parts[0], features);
            })
            .cache();

        long n = rawData.count(); // 统计人数一共多少人

        // ------------------- z-score 归一化：先算每列均值、标准差 --------------------------
        // 标准化后的值 = (原来的值 - 平均值) / 标准差
        final double[] sum = new double[DIM]; // 用来保存每一列的总和。
        final double[] sumSq = new double[DIM]; // 每一列“平方后的总和”。
        List<double[]> allFeats = rawData.values().collect();
        for (double[] f : allFeats)
        {
            for (int i = 0; i < DIM; i++) 
            {
                sum[i] += f[i]; sumSq[i] += f[i] * f[i];
            }    
        }
        
        final double[] mean = new double[DIM]; // mean 存每列平均值
        final double[] std = new double[DIM]; // std 存每列标准差

        for (int i = 0; i < DIM; i++) 
        {
            mean[i] = sum[i] / n;
            double var = sumSq[i] / n - mean[i] * mean[i]; // 方差 = 平方的平均值 - 平均值的平方
            std[i] = var > 1e-9 ? Math.sqrt(var) : 1.0; // 如果方差不是 0，就正常开方； 如果方差太小，说明这一列几乎一样，就把标准差设为 1，避免后面除以 0。

        }

        // ----------------- 对原始数据做转换，生成标准化后的数据 -------------------
        // email -> 标准化后的数据
        JavaPairRDD<String, double[]> data = rawData
            .mapToPair(element -> {
                double[] f = element._2;
                double[] z = new double[DIM];
                
                for(int i = 0; i < DIM; ++i)
                {
                    z[i] = (f[i] - mean[i]) / std[i]; // 标准化后的值 = (原始值 - 平均值) / 标准差
                }
                return new Tuple2<>(element._1, z);
            })
            .cache();
        
        // ------------------- 初始化 K 个中心：随机取 K 个点 ----------------------
        List<double[]> centers = new ArrayList<>(data.values().takeSample(false, K, 42));   // 42是随机种子，结果可复现

        // ----------------------- 迭代 --------------------------
        for(int iter = 0; iter < iterations; ++iter)
        {
            final List<double[]> curCenters = centers;

            // 分配 + 累加：(簇号, (向量和, 计数))
            JavaPairRDD<Integer, Tuple2<double[], Integer>> assigned = data
                .mapToPair(t -> {
                    int c = nearest(t._2, curCenters);
                    return new Tuple2<>(c, new Tuple2<>(t._2, 1));
                });

            JavaPairRDD<Integer, Tuple2<double[], Integer>> reduced = assigned
                .reduceByKey((a, b) -> {
                    double[] s = new double[DIM];
                    for (int i = 0; i < DIM; i++) 
                    {
                        s[i] = a._1[i] + b._1[i];
                    }
                    return new Tuple2<>(s, a._2 + b._2);
                });
            
            // 算新中心
            List<Tuple2<Integer, Tuple2<double[], Integer>>> res = reduced.collect();
            List<double[]> newCenters = new ArrayList<>(curCenters);   // 拷贝旧的，防止某簇空了没人更新
            for (Tuple2<Integer, Tuple2<double[], Integer>> e : res) {
                int c = e._1;
                double[] s = e._2._1; int cnt = e._2._2;
                double[] nc = new double[DIM];
                for (int i = 0; i < DIM; i++) 
                {
                    nc[i] = s[i] / cnt;
                }
                newCenters.set(c, nc);
            }

            // 算中心移动量，收敛就停
            double shift = 0;
            for (int c = 0; c < K; c++) 
            {
                shift += dist2(centers.get(c), newCenters.get(c));
            }
            centers = newCenters;
            System.out.println("KMeans iter " + (iter + 1) + " center shift=" + shift);
            if (shift < 1e-6) break;
        }

        // ---- 输出每个人的簇号 ----
        final List<double[]> finalCenters = centers;
        JavaPairRDD<String, Integer> result = data
            .mapToPair(t -> new Tuple2<>(t._1, nearest(t._2, finalCenters)));

        // 拼成输出行：email \t 簇号
        List<String> lines = new ArrayList<>();
        lines.add("email\tcluster");
        for (Tuple2<String, Integer> t : result.collect())
        {
            lines.add(t._1 + "\t" + t._2);
        }
            
        sc.parallelize(lines, 1).saveAsTextFile(outputPath);

        // ---- 顺便打印每个簇的规模 + 中心（反归一化回原始量纲，方便解读）----
        System.out.println("===== 各簇规模 =====");
        for (Tuple2<Integer, Long> t : result.values()
                .mapToPair(c -> new Tuple2<>(c, 1L)).reduceByKey((a,b)->a+b).collect()) {
            System.out.println("簇 " + t._1 + ": " + t._2 + " 人");
        }
        System.out.println("===== 各簇中心（还原到原始量纲）=====");
        for (int c = 0; c < K; c++) 
        {
            double[] z = finalCenters.get(c);
            StringBuilder sb = new StringBuilder("簇 " + c + ": ");
            for (int i = 0; i < DIM; i++) sb.append(String.format("%.2f ", z[i] * std[i] + mean[i]));
            System.out.println(sb);
        }
        sc.stop();   
    }
}