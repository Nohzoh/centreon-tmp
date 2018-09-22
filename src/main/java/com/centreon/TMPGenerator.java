/**
 * WARNING WARNING WARNING
 * 
 * THIS CLASS IS NOS NOW DEPRECATED,
 * IT HAS BEEN WRITTEN FOR A FORMER INPUT FORMAT NOW UNUSED
 */

package com.centreon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

@Deprecated
public class TMPGenerator {

    private static Logger log = Logger.getLogger(TMPGenerator.class.getName());
    private static int granularity = 300_000;

    private static Map<String, String> metrics = new HashMap<>();
    private static List<String> orderedMetrics = new ArrayList<>();
    private static Map<String, Long> metricsUsage = new HashMap<>();

    /**
     * Expected arguments :
     * <li>- either "finalize", {metrics definition file}, {sorted file}, {result
     * file}; then we wille produce the final TMP file from the previously filtered
     * and sorted data<br>
     * <li>- or {metrics definition file}, {databin file}, {filtered file}, {sorted
     * file}; then we will produce filterd and sorted data from databin
     */
    public static void main(String[] args) {
        if (args.length > 0 && "finalize".equals(args[0])) {
            finalize(args);
            return;
        }

        String metricsFile = args.length > 0 ? args[0] : "../qlikview.csv";
        String dataFile = args.length > 1 ? args[1] : "../databin";
        String filteredFile = args.length > 2 ? args[2] : "../filteredFile.work";
        String sortedFile = args.length > 3 ? args[3] : "../sortedFile.work";
        log.info("Metrics file: " + metricsFile);
        log.info("Data file: " + dataFile);
        log.info("Filtered file: " + filteredFile);
        log.info("Sorted file: " + sortedFile);

        readMetrics(metricsFile);

        log.info("Filtering ....");
        filterData(dataFile, filteredFile);
        log.info("Sorting ....");
        sortData(filteredFile, sortedFile);
        log.info("THE END");
    }

    private static void finalize(String[] args) {
        String metricsFile = args.length > 1 ? args[1] : "../qlikview.csv";
        String sortedFile = args.length > 2 ? args[2] : "../sortedFile.work";
        String resultFile = args.length > 3 ? args[3] : "../result.csv";
        log.info("Metrics file: " + metricsFile);
        log.info("Sorted file: " + sortedFile);
        log.info("Result file: " + resultFile);
        readMetrics(metricsFile);

        consolidateDataAndWriteResult(sortedFile, resultFile);

        for (String metric : orderedMetrics) {
            Long usage = metricsUsage.get(metric);
            log.info("Metric usage: " + metric + " => " + (usage == null ? "Unused" : usage));
        }
    }

    private static void consolidateDataAndWriteResult(String sortedFile, String resultFile) {
        try (PrintStream os = new PrintStream(resultFile)) {
            StringBuilder resultHeader = new StringBuilder("Timestamp");
            for (String metric : orderedMetrics) {
                resultHeader.append(';').append(metrics.get(metric));
            }
            os.println(resultHeader.toString());
            long lineNumber = 0;
            try (Scanner scanner = new Scanner(new File(sortedFile))) {
                Map<String, String> currentTimestampCollumns = null;
                long currentTimestamp = 0;
                while (scanner.hasNext()) {
                    String line = scanner.nextLine();
                    String[] cells = line.split("\t");
                    long lineTimestamp = granularizeTimestamp(Long.parseLong(cells[0]) * 1000);
                    String metricId = cells[1];
                    String value = cells[2];
                    if (lineTimestamp != currentTimestamp) {
                        if (currentTimestamp != 0) {
                            writeToFile(os, currentTimestampCollumns, currentTimestamp);
                            lineNumber++;
                            if (lineNumber % 1000 == 0) {
                                os.flush();
                            }
                        }
                        currentTimestamp = lineTimestamp;
                        currentTimestampCollumns = new HashMap<>();
                    }
                    String v = currentTimestampCollumns.putIfAbsent(metricId, value);
                    if (v != null) {
                        log.fine("Multiple values for single metric/date: " + metricId + "|" + currentTimestamp);
                    }
                }
                if (currentTimestamp != 0) {
                    writeToFile(os, currentTimestampCollumns, currentTimestamp);
                }
            }
        } catch (FileNotFoundException fnf) {
            throw new IllegalArgumentException("Failed to read sorted file [" + sortedFile + "]");
        }
    }

    private static void writeToFile(PrintStream os, Map<String, String> collumns, long timestamp) {
        StringBuilder resultRow = new StringBuilder();
        resultRow.append(Instant.ofEpochMilli(timestamp).toString());
        for (String metric : orderedMetrics) {
            String metricValue = collumns.get(metric);
            countUsage(metric, metricValue);
            resultRow.append(';').append(metricValue == null ? "" : metricValue);
        }
        os.println(resultRow.toString());
    }

    private static void countUsage(String metric, String metricValue) {
        if (metricValue != null) {
            Long usage = metricsUsage.get(metric);
            if (usage == null) {
                usage = 0L;
            }
            usage++;
            metricsUsage.put(metric, usage);
        }
    }

    private static void sortData(String filteredFile, String sortedFile) {
        long t0 = System.currentTimeMillis();
        try (PrintStream os = new PrintStream(sortedFile)) {
            Process proc = Runtime.getRuntime().exec("sort -u " + filteredFile);
            long lineNumber = 0;
            try (Scanner scanner = new Scanner(proc.getInputStream())) {
                while (scanner.hasNext()) {
                    if (System.currentTimeMillis() - t0 > 10_000) {
                        t0 = System.currentTimeMillis();
                        log.info("Filtered read: " + lineNumber);
                    }
                    os.println(scanner.nextLine());
                    lineNumber++;
                    if (lineNumber % 1000 == 0) {
                        os.flush();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void filterData(String dataFile, String filteredFile) {
        try (PrintStream os = new PrintStream(filteredFile)) {
            long t0 = System.currentTimeMillis();
            try (Scanner scanner = new Scanner(new File(dataFile))) {
                long lineNumber = 0;
                while (scanner.hasNext()) {
                    if (System.currentTimeMillis() - t0 > 10_000) {
                        t0 = System.currentTimeMillis();
                        log.info("Databin read: " + lineNumber);
                    }
                    String row = scanner.nextLine();
                    String[] cells = row.split("\t");
                    if (metrics.containsKey(cells[0])) {
                        os.println(cells[1] + '\t' + cells[0] + '\t' + cells[2]);
                    }
                    lineNumber++;
                    if (lineNumber % 1000 == 0) {
                        os.flush();
                    }
                }
                os.flush();
                log.info("Rows read: " + lineNumber);
            }
        } catch (FileNotFoundException fnf) {
            throw new IllegalArgumentException("Failed to read data file [" + dataFile + "]");
        }
    }

    private static void readMetrics(String metricsFile) {
        try (Scanner scanner = new Scanner(new File(metricsFile))) {
            while (scanner.hasNext()) {
                String row = scanner.nextLine();
                String[] cells = row.split("\t");
                String v = metrics.putIfAbsent(cells[4], cells[0] + ':' + cells[2] + ':' + cells[5]);
                if (v != cells[5]) {
                    log.fine("Metric ID already existing: " + cells[4]);
                }
            }
            orderedMetrics.addAll(metrics.keySet());
        } catch (FileNotFoundException fnf) {
            throw new IllegalArgumentException("Failed to read metrics file [" + metricsFile + "]");
        }
        if (log.isLoggable(Level.FINE)) {
            for (Entry<String, String> metric : metrics.entrySet()) {
                log.fine("Metric: " + metric);
            }
        }
        log.info("Metrics count: " + metrics.size());
    }

    private static long granularizeTimestamp(long timestamp) {
        return timestamp - timestamp % granularity;
    }
}
