package com.bnk.files.dedup.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Validated
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    @NotNull
    private Path inputDir;

    @NotNull
    private Path outputDir;

    @NotNull
    private Path workDir;

    @NotNull
    private LocalDate runDate;

    @Min(0)
    private int lookbackDays = 3;

    @Min(1)
    private int chunkSize = 1000;

    @Min(1)
    private int gridSize = 8;

    @Min(1)
    private int taskQueueCapacity = 1000;

    @Min(1)
    private int progressLogInterval = 100000;

    private boolean skipMalformedJson = true;

    @NotBlank
    private String runMode = "restart";

    private boolean overwriteOutput = true;

    private List<String> uniquenessFields = List.of(
            "ChangeData.id",
            "Source.Table",
            "ChangeData.ChangeDateTime",
            "Data._creationdate"
    );

    private String rerunId;

    private Map<String, List<String>> tableUniquenessFields = new HashMap<>();

    private Path tablesFile;

    private List<String> tables = List.of(
            "account",
            "accountaccesstype",
            "accountbalance",
            "accountdormancystatus",
            "accounthistory",
            "accountsegment",
            "activitylogcategory",
            "activitylogtype",
            "bnk_plastichistory",
            "award"
    );

    /**
     * Configurable extension for the input ready file.
     * Example: ".ready"
     */
    @NotBlank
    private String readyFileExtension = ".ready";

    /**
     * Kept as "extention" intentionally to match the requested YAML key:
     * app.running-file-extention
     */
    @NotBlank
    private String runningFileExtention = ".loading";

    @NotBlank
    private String successFileExtension = ".loaded";

    @NotBlank
    private String failFileExtension = ".fail";

    /**
     * Configurable extension for the completion marker file.
     * Examples: ".completed", ".done", ".trg"
     */
    @NotBlank
    private String completedFileExtension = ".completed";

    private String completedFileFolder = "triggerFile";

    public Path getInputDir() {
        return inputDir;
    }

    public void setInputDir(Path inputDir) {
        this.inputDir = inputDir;
    }

    public Path getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(Path outputDir) {
        this.outputDir = outputDir;
    }

    public Path getWorkDir() {
        return workDir;
    }

    public void setWorkDir(Path workDir) {
        this.workDir = workDir;
    }

    public LocalDate getRunDate() {
        return runDate;
    }

    public void setRunDate(LocalDate runDate) {
        this.runDate = runDate;
    }

    public int getLookbackDays() {
        return lookbackDays;
    }

    public void setLookbackDays(int lookbackDays) {
        this.lookbackDays = lookbackDays;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getGridSize() {
        return gridSize;
    }

    public void setGridSize(int gridSize) {
        this.gridSize = gridSize;
    }

    public int getTaskQueueCapacity() {
        return taskQueueCapacity;
    }

    public void setTaskQueueCapacity(int taskQueueCapacity) {
        this.taskQueueCapacity = taskQueueCapacity;
    }

    public int getProgressLogInterval() {
        return progressLogInterval;
    }

    public void setProgressLogInterval(int progressLogInterval) {
        this.progressLogInterval = progressLogInterval;
    }

    public boolean isSkipMalformedJson() {
        return skipMalformedJson;
    }

    public void setSkipMalformedJson(boolean skipMalformedJson) {
        this.skipMalformedJson = skipMalformedJson;
    }

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }

    public boolean isOverwriteOutput() {
        return overwriteOutput;
    }

    public void setOverwriteOutput(boolean overwriteOutput) {
        this.overwriteOutput = overwriteOutput;
    }

    public List<String> getUniquenessFields() {
        return uniquenessFields;
    }

    public void setUniquenessFields(List<String> uniquenessFields) {
        this.uniquenessFields = uniquenessFields;
    }

    public String getRerunId() {
        return rerunId;
    }

    public void setRerunId(String rerunId) {
        this.rerunId = rerunId;
    }

    public Map<String, List<String>> getTableUniquenessFields() {
        return tableUniquenessFields;
    }

    public void setTableUniquenessFields(Map<String, List<String>> tableUniquenessFields) {
        this.tableUniquenessFields = tableUniquenessFields;
    }

    public Path getTablesFile() {
        return tablesFile;
    }

    public void setTablesFile(Path tablesFile) {
        this.tablesFile = tablesFile;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public String getReadyFileExtension() {
        return readyFileExtension;
    }

    public void setReadyFileExtension(String readyFileExtension) {
        this.readyFileExtension = readyFileExtension;
    }

    public String getRunningFileExtention() {
        return runningFileExtention;
    }

    public void setRunningFileExtention(String runningFileExtention) {
        this.runningFileExtention = runningFileExtention;
    }

    public String getSuccessFileExtension() {
        return successFileExtension;
    }

    public void setSuccessFileExtension(String successFileExtension) {
        this.successFileExtension = successFileExtension;
    }

    public String getFailFileExtension() {
        return failFileExtension;
    }

    public void setFailFileExtension(String failFileExtension) {
        this.failFileExtension = failFileExtension;
    }

    public String getCompletedFileExtension() {
        return completedFileExtension;
    }

    public void setCompletedFileExtension(String completedFileExtension) {
        this.completedFileExtension = completedFileExtension;
    }

    public Path inputDirPath() {
        return inputDir;
    }

    public Path outputDirPath() {
        return outputDir;
    }

    public Path workDirPath() {
        return workDir;
    }

    public Path runDateInputDir() {
        return inputDirPath().resolve(runDate.toString());
    }

    public Path tempOutputDir() {
        return workDirPath().resolve("temp-output").resolve(runDate.toString());
    }

    public Path rocksDbDir() {
        return workDirPath().resolve("rocksdb").resolve(runDate.toString());
    }

    public Path rocksDbDir(String table) {
        return rocksDbDir().resolve(table);
    }

    public Path readyFilePath() {
        return runDateInputDir().resolve(runDate + normalizedReadyFileExtension());
    }

    public Path runningFilePath() {
        return runDateInputDir().resolve(runDate + normalizedRunningFileExtention());
    }

    public Path successFilePath() {
        return runDateInputDir().resolve(runDate + normalizedSuccessFileExtension());
    }

    public Path failFilePath() {
        return runDateInputDir().resolve(runDate + normalizedFailFileExtension());
    }

    public Path completionFilePath() {
        return outputDirPath()
                .resolve(completedFileFolder)
                .resolve(runDate + normalizedCompletedFileExtension());
    }

    public String getCompletedFileFolder() {
        return completedFileFolder;
    }

    public void setCompletedFileFolder(String completedFileFolder) {
        this.completedFileFolder = completedFileFolder;
    }

    public String normalizedReadyFileExtension() {
        return normalizeExtension(readyFileExtension);
    }

    public String normalizedRunningFileExtention() {
        return normalizeExtension(runningFileExtention);
    }

    public String normalizedSuccessFileExtension() {
        return normalizeExtension(successFileExtension);
    }

    public String normalizedFailFileExtension() {
        return normalizeExtension(failFileExtension);
    }

    public String normalizedCompletedFileExtension() {
        return normalizeExtension(completedFileExtension);
    }

    private String normalizeExtension(String extension) {
        if (extension == null || extension.isBlank()) {
            throw new IllegalArgumentException("File extension must not be blank");
        }

        String trimmed = extension.trim();
        return trimmed.startsWith(".") ? trimmed : "." + trimmed;
    }
}