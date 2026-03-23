package com.sars.files.dedup.logging;

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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Service
public class ExtendedStatsLogWriter {

    private static final Logger log = LoggerFactory.getLogger(ExtendedStatsLogWriter.class);
    private static final String DEFAULT_RUN_ID = "default";

    private final AppProperties properties;

    public ExtendedStatsLogWriter(AppProperties properties) {
        this.properties = properties;
    }

    public void writeStatsLog(JobExecution jobExecution, ExtendedStatsCollector collector) {
        LocalDate runDate = properties.getRunDate();
        String runId = resolveRunId(jobExecution);

        Path statsFile = Path.of("logs")
                .resolve(runDate.toString())
                .resolve("stats-detailed-" + sanitizeFilePart(runId) + ".log");

        List<String> lines = new ArrayList<>();
        //lines.addAll(collector.duplicateEntries());
        lines.addAll(collector.getDuplicateEntries());
        if (!collector.getDuplicateEntries().isEmpty() && !collector.getDuplicateEntries().isEmpty()) {
            lines.add("");
        }
        lines.addAll(collector.getRejectedEntries());

        try {
            Files.createDirectories(statsFile.getParent());
            Files.write(statsFile, lines, StandardCharsets.UTF_8);
            log.info("event=extended_stats_log_written runDate={} runId={} file={}", runDate, runId, statsFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write extended stats log: " + statsFile, e);
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
}
