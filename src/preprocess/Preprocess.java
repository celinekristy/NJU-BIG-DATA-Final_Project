package com.enron.preprocess;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public class Preprocess {

    private static final Pattern TZ_PAREN = Pattern.compile("\\s*\\([^)]*\\)\\s*$");

    public static void main(String[] args) throws Exception {
        String inPath  = args.length > 0 ? args[0] : "emails.csv";
        String edgeOut = args.length > 1 ? args[1] : "new_clean_edges.tsv";
        String nodeOut = args.length > 2 ? args[2] : "new_persons.txt";

        Set<String> persons = new HashSet<>();
        int emailCount = 0, edgeCount = 0, skipped = 0, naDate = 0;

        try (Reader reader = new InputStreamReader(
                     new FileInputStream(inPath), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.parse(reader);   // 1.0版：能正确处理 message 里的多行/引号
             BufferedWriter ew = new BufferedWriter(new FileWriter(edgeOut))) {

            boolean first = true;
            for (CSVRecord rec : parser) {
                if (first) { first = false; continue; }   // 跳过表头行 file,message

                // emails.csv 只有两列：第0列 file（路径），第1列 message（整封邮件全文）
                String message = safe(rec, 1);
                if (message.isEmpty()) { skipped++; continue; }

                // 从邮件头里抠出 From / To / Cc / Date（支持跨行收件人）
                String from = cleanAddr(extractHeader(message, "From:")).toLowerCase();
                String to   = extractHeader(message, "To:");
                String cc   = extractHeader(message, "Cc:");
                String date = extractHeader(message, "Date:");

                if (from.isEmpty() || to.trim().isEmpty()) { skipped++; continue; }
                if (!isValidEmail(from)) { skipped++; continue; }   // 发件人必须是有效邮箱
                emailCount++;

                String ym = parseYearMonth(date);
                if ("NA".equals(ym)) naDate++;
                persons.add(from);

                edgeCount += writeEdges(ew, from, to, "to", ym, persons);
                edgeCount += writeEdges(ew, from, cc, "cc", ym, persons);
            }
        }

        try (BufferedWriter nw = new BufferedWriter(new FileWriter(nodeOut))) {
            for (String p : persons) { nw.write(p); nw.newLine(); }
        }

        System.out.println("有效邮件数 emailCount = " + emailCount);
        System.out.println("跳过(缺From/To/空/非法) = " + skipped);
        System.out.println("生成边数 edgeCount    = " + edgeCount);
        System.out.println("去重人数(节点)        = " + persons.size());
        System.out.println("日期解析失败(NA)      = " + naDate);
    }

    // 从邮件全文里找 headerName 开头的行，并把它的“接续行”(以空格/Tab开头)拼回来
    private static String extractHeader(String message, String headerName) {
        String[] lines = message.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String l = stripCR(lines[i]);
            if (l.startsWith(headerName)) {
                StringBuilder sb = new StringBuilder(l.substring(headerName.length()).trim());
                // 收件人/抄送常跨多行：下一行若以空格或Tab开头，说明是上一行的延续
                for (int j = i + 1; j < lines.length; j++) {
                    String next = stripCR(lines[j]);
                    if (next.startsWith(" ") || next.startsWith("\t")) {
                        sb.append(" ").append(next.trim());
                    } else {
                        break;   // 遇到不是接续行的，停止
                    }
                }
                return sb.toString().trim();
            }
        }
        return "";
    }

    // 去掉行尾的 \r（Windows换行残留）
    private static String stripCR(String s) {
        return s.endsWith("\r") ? s.substring(0, s.length() - 1) : s;
    }

    // 清理形如 "ENA Legal <foo@bar.com>" 的地址，只取尖括号里的邮箱；没有尖括号就原样返回
    private static String cleanAddr(String s) {
        if (s == null) return "";
        int lt = s.indexOf('<'), gt = s.indexOf('>');
        if (lt >= 0 && gt > lt) {
            return s.substring(lt + 1, gt).trim();
        }
        return s.trim();
    }

    // 判断是不是有效邮箱：恰好一个@、@不在首尾、不含空格/冒号/尖括号、@后有点(域名)
    private static boolean isValidEmail(String s) {
        if (s == null) return false;
        int at = s.indexOf('@');
        if (at <= 0 || at != s.lastIndexOf('@') || at == s.length() - 1) return false;
        if (s.contains(" ") || s.contains(":") || s.contains("<") || s.contains(">")) return false;
        if (s.indexOf('.', at) < 0) return false;   // @后面要有点(域名)
        return true;
    }

    private static int writeEdges(BufferedWriter w, String from, String field,
                                  String type, String ym, Set<String> persons)
            throws IOException {
        if (field == null || field.trim().isEmpty()) return 0;
        int n = 0;
        for (String raw : field.split("[,;]")) {
            String dst = cleanAddr(raw.trim()).toLowerCase();
            if (dst.isEmpty() || dst.equals(from)) continue;
            if (!isValidEmail(dst)) continue;   // 收件人必须是有效邮箱，过滤掉 "department: ena legal" 这种
            persons.add(dst);
            w.write(from + "\t" + dst + "\t" + type + "\t" + ym);
            w.newLine();
            n++;
        }
        return n;
    }

    private static String safe(CSVRecord rec, int idx) {
        if (idx >= rec.size()) return "";
        String v = rec.get(idx);
        return v == null ? "" : v.trim();
    }

    private static String parseYearMonth(String date) {
        if (date == null || date.trim().isEmpty()) return "NA";
        String cleaned = TZ_PAREN.matcher(date.trim()).replaceAll("");
        SimpleDateFormat in  = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US);
        SimpleDateFormat out = new SimpleDateFormat("yyyy-MM", Locale.US);
        try {
            Date d = in.parse(cleaned);
            String ym = out.format(d);
            // 只保留合理范围 1998~2002，超出的当成 NA（源数据日期填错）
            int year = Integer.parseInt(ym.substring(0, 4));
            if (year < 1998 || year > 2002) return "NA";
            return ym;
        } catch (Exception e) {
            return "NA";
        }
    }
}