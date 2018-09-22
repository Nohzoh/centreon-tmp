package com.centreon;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;

public class TMPGenerator2 {

    private static Logger log = Logger.getLogger(TMPGenerator2.class.getName());
    private static int granularity = 300_000;

    // ordered list of expected metrics
    private static List<String> metrics = new ArrayList<>();
    private static MetricInfo[] metricsUsage;

    /**
     * Entry point of the program
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            log.warning(
                    "usage: java [JVM_ARGS] -jar generator.jar [metrics list file] [source data file] [result file]");
            return;
        }
        String metricsFile = args[0];
        String sourceFile = args[1];
        String resultFile = args[2];
        log.info("Metrics file: " + metricsFile);
        log.info("Source file: " + sourceFile);
        log.info("Result file: " + resultFile);

        // Read the metrics file
        readMetrics(metricsFile);

        // Read the data file, build the result in memory and write the output
        consolidateDataAndWriteResult(sourceFile, resultFile);

        // Display statistics about metrics in data provided
        int idx = 0;
        for (String metric : metrics) {
            log.info(metric + " occurences: " + metricsUsage[idx].toString());
        }
    }

    private static void consolidateDataAndWriteResult(String sourceFile, String resultFile) {
        
        // result data will be written in resultData during source file read
        Map<Long, String[]> resultData = new HashMap<>();

        // for logging purpose
        int lineNumber = 0;

        // a java.util.Scanner is used to read source file row by row
        try (Scanner scanner = new Scanner(new File(sourceFile))) {
            while (scanner.hasNext()) {
                // log progress each 10 000 lines
                logProgress(lineNumber++);

                String line = scanner.nextLine();

                // we exclude source header (data file might contain several ones)
                if (line.startsWith("host_id ")) {
                    continue;
                }
                String[] cells = toArray(line);

                long rowTimestamp = granularizeTimestamp(Long.parseLong(cells[6]) * 1000);
                String metricName = cells[1] + ":" + cells[3] + ":" + cells[5];
                int metricIndex = metrics.indexOf(metricName);
                if (metricIndex == -1) {
                    continue;
                }
                String[] rowData = resultData.get(rowTimestamp);
                if (rowData == null) {
                    rowData = new String[metrics.size()];
                    resultData.put(rowTimestamp, rowData);
                }
                MetricInfo info = metricsUsage[metricIndex];
                if (info == null) {
                    metricsUsage[metricIndex] = MetricInfo.from(rowTimestamp);
                } else {
                    info.consider(rowTimestamp);
                }

                rowData[metricIndex] = cells[7];
            }
        } catch (FileNotFoundException fnf) {
            throw new IllegalArgumentException("Failed to read source file [" + sourceFile + "]");
        }

        try (PrintStream os = new PrintStream(resultFile)) {
            StringBuilder resultHeader = new StringBuilder("Timestamp");
            for (String metric : metrics) {
                resultHeader.append(';').append(metric);
            }
            os.println(resultHeader.toString());

            List<Long> sorted = new ArrayList<>(resultData.keySet());
            Collections.sort(sorted);
            for (Long timestamp : sorted) {
                String[] values = resultData.get(timestamp);
                StringBuilder resultRow = new StringBuilder();
                resultRow.append(Instant.ofEpochMilli(timestamp).toString());
                for (String value : values) {
                    resultRow.append(';').append(value == null ? "" : value);
                }
                os.println(resultRow.toString());
                os.flush();
            }
            os.flush();
        } catch (FileNotFoundException fnf) {
            throw new IllegalArgumentException("Failed to read sorted file [" + sourceFile + "]");
        }
    }

    private static String[] toArray(String line) {
        return line.replaceAll(" +", " ").split(" ");
    }

    private static void readMetrics(String metricsFile) {
        try (Scanner scanner = new Scanner(new File(metricsFile))) {
            while (scanner.hasNext()) {
                metrics.add(scanner.nextLine());
            }
        } catch (FileNotFoundException fnf) {
            throw new IllegalArgumentException("Failed to read metrics file [" + metricsFile + "]");
        }
        metricsUsage = new MetricInfo[metrics.size()];
        log.info("Metrics count: " + metrics.size());
    }

    private static long granularizeTimestamp(long timestamp) {
        return timestamp - timestamp % granularity;
    }

    private static void logProgress(int lineNumber) {
        if (lineNumber % 10000 == 0) {
            log.info("reading row " + lineNumber);
        }
    }

    /**
     * This inner class is used to get usage informations of each metrics in the
     * data file
     */
    static class MetricInfo {
        static MetricInfo from(long dt) {
            MetricInfo result = new MetricInfo();
            result.count = 1;
            result.dtMax = dt;
            result.dtMin = dt;
            return result;
        }

        void consider(long dt) {
            count++;
            dtMin = Math.min(dt, this.dtMin);
            dtMax = Math.max(dt, this.dtMax);
        }

        int count;
        long dtMin;
        long dtMax;

        @Override
        public String toString() {
            return "Occurrences: " + count + ", dtMin: " + Instant.ofEpochMilli(dtMin) + ", dtMax: "
                    + Instant.ofEpochMilli(dtMax);
        }
    }
}
