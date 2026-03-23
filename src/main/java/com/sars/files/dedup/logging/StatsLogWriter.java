package com.sars.files.dedup.logging;

import com.sars.files.dedup.batch.JobCompletionValidationListener.TableStatsView;
import com.sars.files.dedup.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Map;

@Service
public class StatsLogWriter {

    private static final Logger log = LoggerFactory.getLogger(StatsLogWriter.class);
    private static final String DEFAULT_RUN_ID = "default";

    private final AppProperties properties;

    public StatsLogWriter(AppProperties properties) {
        this.properties = properties;
    }

    public void writeStatsLog(JobExecution jobExecution, Map<String, TableStatsView> statsByTable) {
        LocalDate runDate = properties.getRunDate();
        String runId = resolveRunId(jobExecution);

        Path statsFile = Path.of("logs")
                .resolve(runDate.toString())
                .resolve("stats-" + sanitizeFilePart(runId) + ".log");

        long grandTotal = statsByTable.values().stream()
                .mapToLong(TableStatsView::totalProcessed)
                .sum();

        Duration overallDuration = jobDuration(jobExecution);

        StringBuilder sb = new StringBuilder();
        sb.append("Run ID: ").append(runId).append(System.lineSeparator());
        sb.append("Run Date: ").append(runDate).append(System.lineSeparator());
        sb.append(System.lineSeparator());
        sb.append("Data Processed Summary:").append(System.lineSeparator());

        statsByTable.values().stream()
                .sorted(Comparator.comparing(TableStatsView::table, String.CASE_INSENSITIVE_ORDER))
                .forEach(stats -> {
                    Path outputFile = properties.outputDirPath()
                            .resolve(stats.table())
                            .resolve(runDate.toString())
                            .resolve(stats.outputFileName());

                    sb.append(stats.table()).append(":").append(System.lineSeparator());
                    sb.append("\tTotal Records processed: ").append(stats.totalProcessed()).append(System.lineSeparator());
                    sb.append("\tTotal Rejected: ").append(stats.totalRejected()).append(System.lineSeparator());
                    sb.append("\tTotal Duplicates: ").append(stats.totalDuplicates()).append(System.lineSeparator());
                    sb.append("\tFiles: ").append(outputFile).append(System.lineSeparator());
                    sb.append("\tDuration: ").append(formatDuration(stats.duration())).append(System.lineSeparator());
                });

        sb.append("------------------------------------------------------------------------")
                .append(System.lineSeparator());
        sb.append("Grand Total: ").append(grandTotal).append(System.lineSeparator());
        sb.append("Duration: ").append(formatDuration(overallDuration)).append(System.lineSeparator());

        try {
            Files.createDirectories(statsFile.getParent());
            Files.writeString(statsFile, sb.toString(), StandardCharsets.UTF_8);
            log.info("event=stats_log_written runDate={} runId={} file={}", runDate, runId, statsFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write stats log: " + statsFile, e);
        }
    }

    private String resolveRunId(JobExecution jobExecution) {
        String rerunId = jobExecution.getJobParameters().getString("rerunId");
        if (rerunId != null && !rerunId.isBlank()) {
            return rerunId;
        }

        String configuredRerunId = properties.getRerunId();
        if (configuredRerunId != null && !configuredRerunId.isBlank()) {
            return configuredRerunId;
        }

        return DEFAULT_RUN_ID;
    }

    private String sanitizeFilePart(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_RUN_ID;
        }
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private Duration jobDuration(JobExecution jobExecution) {
        LocalDateTime start = jobExecution.getStartTime();
        LocalDateTime end = jobExecution.getEndTime();
        if (start == null || end == null) {
            return Duration.ZERO;
        }
        return Duration.between(start, end);
    }

    private String formatDuration(Duration duration) {
        long totalSeconds = duration.getSeconds();
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        long millis = duration.toMillisPart();

        if (hours > 0) {
            return String.format("%dh %dm %d.%03ds", hours, minutes, seconds, millis);
        }
        if (minutes > 0) {
            return String.format("%dm %d.%03ds", minutes, seconds, millis);
        }
        return String.format("%d.%03ds", seconds, millis);
    }
}