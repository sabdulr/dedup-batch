package com.bnk.files.dedup.batch;

import com.bnk.files.dedup.config.AppProperties;
import com.bnk.files.dedup.store.RocksDbKeyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Locale;
import java.util.stream.Stream;

@Service
public class JobLaunchService {

    private static final Logger log = LoggerFactory.getLogger(JobLaunchService.class);

    private final JobOperator jobOperator;
    private final Job ndjsonDedupeJob;
    private final AppProperties properties;
    private final RocksDbKeyStore rocksDbKeyStore;

    public JobLaunchService(JobOperator jobOperator,
                            Job ndjsonDedupeJob,
                            AppProperties properties,
                            RocksDbKeyStore rocksDbKeyStore) {
        this.jobOperator = jobOperator;
        this.ndjsonDedupeJob = ndjsonDedupeJob;
        this.properties = properties;
        this.rocksDbKeyStore = rocksDbKeyStore;
    }

    public void launch() throws Exception {
        long startNanos = System.nanoTime();
        String runMode = normalizeRunMode(properties.getRunMode());
        boolean runningFileCreated = false;

        log.info("event=job_launch_start job={} runDate={} runMode={} rerunId={}",
                ndjsonDedupeJob.getName(),
                properties.getRunDate(),
                runMode,
                properties.getRerunId());

        try {
            renameReadyFileToRunning();
            runningFileCreated = true;

            if ("rerun".equals(runMode)) {
                prepareForRerun();
            }

            JobParametersBuilder builder = new JobParametersBuilder()
                    .addString("runDate", properties.getRunDate().toString());

            if ("rerun".equals(runMode)) {
                String rerunId = properties.getRerunId();
                if (rerunId == null || rerunId.isBlank()) {
                    rerunId = String.valueOf(System.currentTimeMillis());
                }
                builder.addString("rerunId", rerunId);
            }

            JobParameters parameters = builder.toJobParameters();

            log.info("event=job_launch job={} runDate={} runMode={} rerunId={}",
                    ndjsonDedupeJob.getName(),
                    properties.getRunDate(),
                    runMode,
                    parameters.getString("rerunId"));

            JobExecution execution = jobOperator.start(ndjsonDedupeJob, parameters);

            long elapsedMs = elapsedMillis(startNanos);
            double elapsedSec = elapsedMs / 1000.0;

            log.info("event=job_complete job={} status={} exitStatus={} executionId={} elapsedMs={} elapsedSec={}",
                    ndjsonDedupeJob.getName(),
                    execution.getStatus(),
                    execution.getExitStatus().getExitCode(),
                    execution.getId(),
                    elapsedMs,
                    elapsedSec);

            if (!BatchStatus.COMPLETED.equals(execution.getStatus())) {
                throw new IllegalStateException(
                        "Job did not complete successfully: status=" + execution.getStatus()
                                + ", exitStatus=" + execution.getExitStatus().getExitCode()
                                + ", executionId=" + execution.getId()
                                + ", toString=" + execution.toString()
                );
            }

            renameRunningFileToSuccess();
        } catch (Exception e) {
            long elapsedMs = elapsedMillis(startNanos);
            double elapsedSec = elapsedMs / 1000.0;

            if (runningFileCreated) {
                tryRenameRunningFileToFail(e);
            }

            log.error("event=job_failed job={} runDate={} runMode={} elapsedMs={} elapsedSec={} message={}",
                    ndjsonDedupeJob.getName(),
                    properties.getRunDate(),
                    runMode,
                    elapsedMs,
                    elapsedSec,
                    e.getMessage(),
                    e);

            throw e;
        }
    }

    private void renameReadyFileToRunning() {
        Path readyFile = properties.readyFilePath();
        Path runningFile = properties.runningFilePath();

        if (!Files.exists(readyFile) || !Files.isRegularFile(readyFile)) {
            throw new IllegalStateException("Ready file not found before launch: " + readyFile);
        }

        moveFile(readyFile, runningFile, "ready_file_renamed_to_running");
    }

    private void renameRunningFileToSuccess() {
        Path runningFile = properties.runningFilePath();
        Path successFile = properties.successFilePath();

        if (!Files.exists(runningFile) || !Files.isRegularFile(runningFile)) {
            throw new IllegalStateException("Running file not found after successful completion: " + runningFile);
        }

        moveFile(runningFile, successFile, "running_file_renamed_to_success");
    }

    private void tryRenameRunningFileToFail(Exception originalException) {
        Path runningFile = properties.runningFilePath();
        Path failFile = properties.failFilePath();

        if (!Files.exists(runningFile) || !Files.isRegularFile(runningFile)) {
            log.warn("event=running_file_missing_for_fail_rename runDate={} runningFile={} originalMessage={}",
                    properties.getRunDate(),
                    runningFile,
                    originalException.getMessage());
            return;
        }

        try {
            Files.move(runningFile, failFile, StandardCopyOption.REPLACE_EXISTING);
            log.info("event=running_file_renamed_to_fail runDate={} source={} target={}",
                    properties.getRunDate(),
                    runningFile,
                    failFile);
        } catch (IOException renameException) {
            log.error("event=running_file_rename_to_fail_failed runDate={} source={} target={} originalMessage={} renameMessage={}",
                    properties.getRunDate(),
                    runningFile,
                    failFile,
                    originalException.getMessage(),
                    renameException.getMessage(),
                    renameException);
        }
    }

    private void moveFile(Path source, Path target, String eventName) {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);

            log.info("event={} runDate={} source={} target={}",
                    eventName,
                    properties.getRunDate(),
                    source,
                    target);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to rename file. source=" + source + ", target=" + target,
                    e
            );
        }
    }

    private long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    private String normalizeRunMode(String value) {
        if (value == null || value.isBlank()) {
            return "restart";
        }

        String normalized = value.toLowerCase(Locale.ROOT);
        if (!normalized.equals("restart") && !normalized.equals("rerun")) {
            throw new IllegalArgumentException("app.run-mode must be 'restart' or 'rerun'");
        }
        return normalized;
    }

    private void prepareForRerun() {
        Path baselineOutputDir = properties.outputDirPath().resolve(properties.getRunDate().toString());

        log.info("event=rerun_prepare_start runDate={} tempDir={} rocksDbDir={} baselineOutputDir={} overwriteOutput={}",
                properties.getRunDate(),
                properties.tempOutputDir(),
                properties.rocksDbDir(),
                baselineOutputDir,
                properties.isOverwriteOutput());

        rocksDbKeyStore.close();

        deleteDirectoryIfExists(properties.tempOutputDir());
        deleteDirectoryIfExists(properties.rocksDbDir());

        if (properties.isOverwriteOutput()) {
            log.info("event=rerun_output_preserved runDate={} outputDir={} reason=output_dir_used_as_dedupe_baseline",
                    properties.getRunDate(),
                    baselineOutputDir);
        }

        log.info("event=rerun_prepare_complete runDate={} tempDir={} rocksDbDir={} baselineOutputDir={}",
                properties.getRunDate(),
                properties.tempOutputDir(),
                properties.rocksDbDir(),
                baselineOutputDir);
    }

    private void deleteDirectoryIfExists(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }

        try (Stream<Path> stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new IllegalStateException("Failed to delete path during rerun cleanup: " + path, e);
                        }
                    });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to clean directory during rerun: " + dir, e);
        }
    }
}