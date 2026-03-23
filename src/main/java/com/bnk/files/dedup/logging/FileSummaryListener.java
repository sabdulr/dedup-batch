package com.bnk.files.dedup.logging;

import com.bnk.files.dedup.io.RestartableNdjsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
//import org.springframework.batch.core.StepExecution;
//import org.springframework.batch.core.StepExecutionListener;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

public class FileSummaryListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(FileSummaryListener.class);

    private final Path file;
    private final RestartableNdjsonWriter writer;
    private Instant start;

    public FileSummaryListener(Path file, RestartableNdjsonWriter writer) {
        this.file = file;
        this.writer = writer;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.start = Instant.now();
        log.info("event=file_step_start step={} partition={} file={} executionId={}",
                stepExecution.getStepName(), stepExecution.getStepName(), file, stepExecution.getId());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        Duration elapsed = Duration.between(start, Instant.now());
        long read = stepExecution.getReadCount();
        long written = stepExecution.getWriteCount();
        long filtered = stepExecution.getFilterCount();
        long skipped = stepExecution.getProcessSkipCount() + stepExecution.getReadSkipCount() + stepExecution.getWriteSkipCount();
        double seconds = Math.max(1.0d, elapsed.toMillis() / 1000.0d);
        double throughput = read / seconds;
        log.info("event=file_step_end status={} step={} file={} tempOutput={} readCount={} writeCount={} filteredCount={} skippedCount={} elapsedMs={} throughputPerSec={}",
                stepExecution.getStatus(), stepExecution.getStepName(), file,
                writer == null ? "n/a" : writer.getPartitionTempFile(),
                read, written, filtered, skipped, elapsed.toMillis(), String.format("%.2f", throughput));
        return stepExecution.getExitStatus();
    }
}
