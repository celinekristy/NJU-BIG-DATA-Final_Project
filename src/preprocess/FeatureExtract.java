package com.enron.preprocess;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class FeatureExtract {
    private static final Pattern TZ = Pattern.compile("\\s*\\([^)]*\\)\\s*$");

    public static void main(String[] args) throws Exception {
        String inPath  = args.length > 0 ? args[0] : "emails.csv";
        String outPath = args.length > 1 ? args[1] : "person_features.tsv";

        // 各种累加器
        Map<String,Integer> sent = new HashMap<>();      // 发信量
        Map<String,Integer> recv = new HashMap<>();      // 收信量
        Map<String,Set<String>> outSet = new HashMap<>();// 发给过的人
        Map<String,Set<String>> inSet  = new HashMap<>();// 收到过谁的
        Map<String,Integer> night = new HashMap<>();     // 深夜发信
        Map<String,Set<String>> months = new HashMap<>();// 活跃月份

        try (Reader reader = new InputStreamReader(new FileInputStream(inPath), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.parse(reader)) {

            boolean first = true;
            for (CSVRecord rec : parser) {
                if (first) { first = false; continue; }   // 跳表头 file,message

                // emails.csv 只有两列：第0列 file，第1列 message（整封邮件全文）
                String message = safe(rec, 1);
                if (message.isEmpty()) continue;

                // 从邮件头里抠出 From / To / Cc / Date（支持跨行收件人）
                String from = cleanAddr(extractHeader(message, "From:")).toLowerCase();
                String to   = extractHeader(message, "To:");
                String cc   = extractHeader(message, "Cc:");
                String date = extractHeader(message, "Date:");

                if (from.isEmpty() || to.trim().isEmpty()) continue;
                if (!isValidEmail(from)) continue;        // 发件人必须是有效邮箱

                int hour = parseHour(date);       // -1 表示解析失败
                String ym = parseYearMonth(date); // NA 表示失败

                // 收集本封邮件的所有收件人（过滤非邮箱）
                List<String> dsts = new ArrayList<>();
                for (String field : new String[]{to, cc}) {
                    if (field == null || field.trim().isEmpty()) continue;
                    for (String raw : field.split("[,;]")) {
                        String d = cleanAddr(raw.trim()).toLowerCase();
                        if (!d.isEmpty() && !d.equals(from) && isValidEmail(d)) dsts.add(d);
                    }
                }

                // 发件人累加
                sent.merge(from, dsts.size(), Integer::sum);
                if (!"NA".equals(ym)) months.computeIfAbsent(from, k -> new HashSet<>()).add(ym);
                if (hour >= 0 && hour < 6) night.merge(from, dsts.size(), Integer::sum);

                // 收件人累加
                for (String d : dsts) {
                    recv.merge(d, 1, Integer::sum);
                    outSet.computeIfAbsent(from, k -> new HashSet<>()).add(d);
                    inSet.computeIfAbsent(d, k -> new HashSet<>()).add(from);
                }
            }
        }

        // 全部人 = 发过或收过的
        Set<String> allPeople = new HashSet<>();
        allPeople.addAll(sent.keySet());
        allPeople.addAll(recv.keySet());

        // 写特征表
        try (BufferedWriter w = new BufferedWriter(new FileWriter(outPath))) {
            // 表头
            w.write("email\trecvCount\toutDegree\tinDegree\tnightRatio\tsendRecvRatio\tactiveMonths\tavgSentPerMonth");
            w.newLine();
            for (String p : allPeople) {
                int sc = sent.getOrDefault(p, 0);
                int rc = recv.getOrDefault(p, 0);
                int od = outSet.getOrDefault(p, Collections.emptySet()).size();
                int idg = inSet.getOrDefault(p, Collections.emptySet()).size();
                int ni = night.getOrDefault(p, 0);
                int am = months.getOrDefault(p, Collections.emptySet()).size();

                double nightRatio    = sc > 0 ? (double) ni / sc : 0.0;
                double sendRecvRatio = (sc + rc) > 0 ? (double) sc / (sc + rc) : 0.0;
                double avgSentPerMonth = am > 0 ? (double) sc / am : 0.0;

                w.write(p + "\t" + rc + "\t" + od + "\t" + idg + "\t"
                        + nightRatio + "\t" + sendRecvRatio + "\t" + am + "\t" + avgSentPerMonth);
                w.newLine();
            }
        }
        System.out.println("总人数 = " + allPeople.size());
        System.out.println("特征表已写出: " + outPath);
    }

    // 从邮件全文里找 headerName 开头的行，并把它的“接续行”(以空格/Tab开头)拼回来
    private static String extractHeader(String message, String headerName) {
        String[] lines = message.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String l = stripCR(lines[i]);
            if (l.startsWith(headerName)) {
                StringBuilder sb = new StringBuilder(l.substring(headerName.length()).trim());
                for (int j = i + 1; j < lines.length; j++) {
                    String next = stripCR(lines[j]);
                    if (next.startsWith(" ") || next.startsWith("\t")) {
                        sb.append(" ").append(next.trim());
                    } else {
                        break;
                    }
                }
                return sb.toString().trim();
            }
        }
        return "";
    }

    private static String stripCR(String s) {
        return s.endsWith("\r") ? s.substring(0, s.length() - 1) : s;
    }

    // 清理 "Name <foo@bar.com>"，只取尖括号里的邮箱
    private static String cleanAddr(String s) {
        if (s == null) return "";
        int lt = s.indexOf('<'), gt = s.indexOf('>');
        if (lt >= 0 && gt > lt) return s.substring(lt + 1, gt).trim();
        return s.trim();
    }

    // 判断有效邮箱：恰好一个@、不在首尾、不含空格/冒号/尖括号、@后有点
    private static boolean isValidEmail(String s) {
        if (s == null) return false;
        int at = s.indexOf('@');
        if (at <= 0 || at != s.lastIndexOf('@') || at == s.length() - 1) return false;
        if (s.contains(" ") || s.contains(":") || s.contains("<") || s.contains(">")) return false;
        if (s.indexOf('.', at) < 0) return false;
        return true;
    }

    private static String safe(CSVRecord rec, int idx) {
        if (idx >= rec.size()) return "";
        String v = rec.get(idx);
        return v == null ? "" : v.trim();
    }

    private static int parseHour(String date) {
        if (date == null || date.trim().isEmpty()) return -1;
        String c = TZ.matcher(date.trim()).replaceAll("");
        try { return new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US).parse(c).getHours(); }
        catch (Exception e) { return -1; }
    }

    private static String parseYearMonth(String date) {
        if (date == null || date.trim().isEmpty()) return "NA";
        String c = TZ.matcher(date.trim()).replaceAll("");
        try {
            Date d = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US).parse(c);
            String ym = new SimpleDateFormat("yyyy-MM", Locale.US).format(d);
            int year = Integer.parseInt(ym.substring(0, 4));
            if (year < 1998 || year > 2002) return "NA";   // 范围检查，和 Preprocess 一致
            return ym;
        } catch (Exception e) { return "NA"; }
    }
}