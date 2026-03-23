package com.sars.files.dedup.batch;

import com.sars.files.dedup.config.AppProperties;
import com.sars.files.dedup.logging.ExtendedStatsCollector;
import com.sars.files.dedup.logging.ExtendedStatsLogWriter;
import com.sars.files.dedup.logging.StatsLogWriter;
import com.sars.files.dedup.service.FileDiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Component
public class JobCompletionValidationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionValidationListener.class);

    private static final String TODAY_WORKER_STEP_PREFIX = "dedupeTodayWorkerStep:";

    private final FileDiscoveryService fileDiscoveryService;
    private final AppProperties properties;
    private final StatsLogWriter statsLogWriter;
    private final ExtendedStatsCollector extendedStatsCollector;
    private final ExtendedStatsLogWriter extendedStatsLogWriter;

    public JobCompletionValidationListener(FileDiscoveryService fileDiscoveryService,
                                           AppProperties properties,
                                           StatsLogWriter statsLogWriter,
                                           ExtendedStatsCollector extendedStatsCollector,
                                           ExtendedStatsLogWriter extendedStatsLogWriter) {
        this.fileDiscoveryService = fileDiscoveryService;
        this.properties = properties;
        this.statsLogWriter = statsLogWriter;
        this.extendedStatsCollector = extendedStatsCollector;
        this.extendedStatsLogWriter = extendedStatsLogWriter;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        extendedStatsCollector.clear();
        if (fileDiscoveryService.discoverTodayFiles().isEmpty()) {
            throw new IllegalStateException(
                    "No input files found for run date " + jobExecution.getJobParameters().getString("runDate")
            );
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long starting = jobExecution.getStepExecutions().stream()
                .filter(se -> se.getStepName().startsWith(TODAY_WORKER_STEP_PREFIX))
                .filter(se -> se.getStatus() == BatchStatus.STARTING)
                .count();

        long started = jobExecution.getStepExecutions().stream()
                .filter(se -> se.getStepName().startsWith(TODAY_WORKER_STEP_PREFIX))
                .filter(se -> se.getStatus() == BatchStatus.STARTED)
                .count();

        long failed = jobExecution.getStepExecutions().stream()
                .filter(se -> se.getStepName().startsWith(TODAY_WORKER_STEP_PREFIX))
                .filter(se -> se.getStatus() == BatchStatus.FAILED)
                .count();

        long completed = jobExecution.getStepExecutions().stream()
                .filter(se -> se.getStepName().startsWith(TODAY_WORKER_STEP_PREFIX))
                .filter(se -> se.getStatus() == BatchStatus.COMPLETED)
                .count();

        log.info("event=partition_step_summary jobExecutionId={} completed={} failed={} starting={} started={}",
                jobExecution.getId(), completed, failed, starting, started);

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            if (stepExecution.getStatus() != BatchStatus.COMPLETED ||
                    !"COMPLETED".equals(stepExecution.getExitStatus().getExitCode())) {

                log.error(
                        "event=step_failure_detail step={} status={} exitCode={} readCount={} writeCount={} filterCount={} skipCount={} rollbackCount={} commitCount={} failureExceptions={}",
                        stepExecution.getStepName(),
                        stepExecution.getStatus(),
                        stepExecution.getExitStatus().getExitCode(),
                        stepExecution.getReadCount(),
                        stepExecution.getWriteCount(),
                        stepExecution.getFilterCount(),
                        stepExecution.getSkipCount(),
                        stepExecution.getRollbackCount(),
                        stepExecution.getCommitCount(),
                        stepExecution.getFailureExceptions().stream()
                                .map(Throwable::toString)
                                .toList()
                );

                for (Throwable t : stepExecution.getFailureExceptions()) {
                    log.error("event=step_failure_stacktrace step={}", stepExecution.getStepName(), t);
                }
            }
        }

        if (jobExecution.getStatus() != BatchStatus.COMPLETED) {
            log.warn("event=completion_marker_skipped job={} runDate={} status={} exitStatus={}",
                    jobExecution.getJobInstance().getJobName(),
                    properties.getRunDate(),
                    jobExecution.getStatus(),
                    jobExecution.getExitStatus().getExitCode());
            return;
        }

        Map<String, TableStats> statsByTable = aggregateTodayStepStats(jobExecution);
        writeCompletionMarker(jobExecution, statsByTable);
        logTableSummaries(jobExecution, statsByTable);

        Map<String, TableStatsView> statsView = statsByTable.entrySet().stream()
                .collect(LinkedHashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue().toView()),
                        LinkedHashMap::putAll);

        statsLogWriter.writeStatsLog(jobExecution, statsView);
        extendedStatsLogWriter.writeStatsLog(jobExecution, extendedStatsCollector);
    }

    private Map<String, TableStats> aggregateTodayStepStats(JobExecution jobExecution) {
        Map<String, TableStats> statsByTable = new LinkedHashMap<>();

        jobExecution.getStepExecutions().stream()
                .filter(this::isTodayWorkerStep)
                .sorted(Comparator.comparing(StepExecution::getStepName).thenComparing(StepExecution::getId))
                .forEach(stepExecution -> {
                    ExecutionContext context = stepExecution.getExecutionContext();
                    String table = context.getString("table", "unknown");

                    TableStats stats = statsByTable.computeIfAbsent(table, ignored -> new TableStats(table));
                    stats.totalProcessed += stepExecution.getReadCount();
                    stats.totalDuplicates += stepExecution.getFilterCount();
                    stats.totalRejected += stepExecution.getReadSkipCount()
                            + stepExecution.getProcessSkipCount()
                            + stepExecution.getWriteSkipCount();
                    stats.outputFileName = mergedFileName(table, properties.getRunDate());
                    stats.duration = stats.duration.plus(stepDuration(stepExecution));
                });

        statsByTable.values().forEach(stats ->
                stats.outputCount = countMergedOutputRecords(stats.table, stats.outputFileName));

        return statsByTable;
    }

    private boolean isTodayWorkerStep(StepExecution stepExecution) {
        return stepExecution.getStepName() != null
                && stepExecution.getStepName().startsWith(TODAY_WORKER_STEP_PREFIX);
    }

    private void writeCompletionMarker(JobExecution jobExecution, Map<String, TableStats> statsByTable) {
        LocalDate runDate = properties.getRunDate();

        Path completionFile = properties.outputDirPath()
                .resolve(properties.getCompletedFileFolder())
                .resolve(runDate + properties.normalizedCompletedFileExtension());

        try {
            Files.createDirectories(completionFile.getParent());

            List<String> lines = statsByTable.values().stream()
                    .sorted(Comparator.comparing(stats -> stats.table, String.CASE_INSENSITIVE_ORDER))
                    .map(stats -> stats.table + "," + stats.outputFileName + "," + stats.outputCount)
                    .toList();

            Files.write(completionFile, lines, StandardCharsets.UTF_8);

            log.info("event=completion_marker_written job={} runDate={} file={} tableCount={}",
                    jobExecution.getJobInstance().getJobName(), runDate, completionFile, lines.size());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write completion marker file: " + completionFile, e);
        }
    }

    private void logTableSummaries(JobExecution jobExecution, Map<String, TableStats> statsByTable) {
        statsByTable.values().stream()
                .sorted(Comparator.comparing(stats -> stats.table, String.CASE_INSENSITIVE_ORDER))
                .forEach(stats -> log.info(
                        "event=dedupe_table_summary job={} runDate={} table={} processedCount={} duplicatesRemoved={} outputCount={} skippedCount={} outputFile={} duration={}",
                        jobExecution.getJobInstance().getJobName(),
                        properties.getRunDate(),
                        stats.table,
                        stats.totalProcessed,
                        stats.totalDuplicates,
                        stats.outputCount,
                        stats.totalRejected,
                        stats.outputFileName,
                        formatDuration(stats.duration)
                ));

        long totalProcessed = statsByTable.values().stream().mapToLong(stats -> stats.totalProcessed).sum();
        long totalDuplicatesRemoved = statsByTable.values().stream().mapToLong(stats -> stats.totalDuplicates).sum();
        long totalOutputCount = statsByTable.values().stream().mapToLong(stats -> stats.outputCount).sum();
        long totalRejected = statsByTable.values().stream().mapToLong(stats -> stats.totalRejected).sum();

        log.info("event=dedupe_job_summary job={} runDate={} tableCount={} processedCount={} duplicatesRemoved={} outputCount={} rejectedCount={} status={} exitStatus={}",
                jobExecution.getJobInstance().getJobName(),
                properties.getRunDate(),
                statsByTable.size(),
                totalProcessed,
                totalDuplicatesRemoved,
                totalOutputCount,
                totalRejected,
                jobExecution.getStatus(),
                jobExecution.getExitStatus().getExitCode());
    }

    private long countMergedOutputRecords(String table, String outputFileName) {
        Path mergedFile = properties.outputDirPath()
                .resolve(table)
                .resolve(properties.getRunDate().toString())
                .resolve(outputFileName);

        if (!Files.exists(mergedFile)) {
            return 0L;
        }

        try (Stream<String> lines = Files.lines(mergedFile, StandardCharsets.UTF_8)) {
            return lines.count();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to count merged records in " + mergedFile, e);
        }
    }

    private String mergedFileName(String table, LocalDate runDate) {
        return "MERGED=" + table + "-" + runDate + ".json";
    }

    private Duration stepDuration(StepExecution stepExecution) {
        LocalDateTime start = stepExecution.getStartTime();
        LocalDateTime end = stepExecution.getEndTime();
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

    private static final class TableStats {
        private final String table;
        private long totalProcessed;
        private long totalDuplicates;
        private long totalRejected;
        private long outputCount;
        private String outputFileName;
        private Duration duration = Duration.ZERO;

        private TableStats(String table) {
            this.table = table;
        }

        private TableStatsView toView() {
            return new TableStatsView(
                    table,
                    totalProcessed,
                    totalRejected,
                    totalDuplicates,
                    outputCount,
                    outputFileName,
                    duration
            );
        }
    }

    public record TableStatsView(
            String table,
            long totalProcessed,
            long totalRejected,
            long totalDuplicates,
            long outputCount,
            String outputFileName,
            Duration duration
    ) {
    }
}