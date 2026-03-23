package com.bnk.files.dedup.service;

import com.bnk.files.dedup.config.AppProperties;
import com.bnk.files.dedup.domain.FileDescriptor;
import com.bnk.files.dedup.domain.FileRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

@Service
public class FileDiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(FileDiscoveryService.class);

    private final AppProperties properties;
    private final FileNameParser parser;
    private final TableListService tableListService;

    public FileDiscoveryService(AppProperties properties,
                                FileNameParser parser,
                                TableListService tableListService) {
        this.properties = properties;
        this.parser = parser;
        this.tableListService = tableListService;
    }

    public List<FileDescriptor> discoverTodayFiles() {
        return discoverForTodayInput(null);
    }

    public List<FileDescriptor> discoverTodayFiles(String table) {
        return discoverForTodayInput(table);
    }

    public List<FileDescriptor> discoverPriorFiles() {
        return discoverPriorFiles(null);
    }

    public List<FileDescriptor> discoverPriorFiles(String table) {
        LocalDate runDate = properties.getRunDate();
        Path outputRootDir = properties.outputDirPath();

        Set<String> allowedTables = tableListService.resolveAllowedTables();
        String requestedTable = normalizeTable(table);
        List<FileDescriptor> result = new ArrayList<>();

        if (!Files.exists(outputRootDir)) {
            logDiscovery(FileRole.PRIOR, 0, outputRootDir, runDate, allowedTables, requestedTable);
            return result;
        }

        if (!Files.isDirectory(outputRootDir)) {
            throw new IllegalStateException("Output directory does not exist: " + outputRootDir);
        }

        try (Stream<Path> stream = Files.walk(outputRootDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .forEach(path -> {
                        OutputLocation outputLocation = extractOutputLocation(outputRootDir, path);
                        if (outputLocation == null) {
                            return;
                        }

                        String normalizedDiscoveredTable = normalizeTable(outputLocation.table());
                        if (!allowedTables.contains(normalizedDiscoveredTable)) {
                            return;
                        }
                        if (requestedTable != null && !requestedTable.equals(normalizedDiscoveredTable)) {
                            return;
                        }
                        if (!runDate.equals(outputLocation.runDate())) {
                            return;
                        }

                        result.add(new FileDescriptor(
                                outputLocation.table(),
                                runDate,
                                1,
                                path,
                                path.getFileName().toString(),
                                FileRole.PRIOR
                        ));
                    });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan output directory " + outputRootDir, e);
        }

        result.sort(Comparator.comparing(FileDescriptor::table, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(FileDescriptor::path));

        logDiscovery(FileRole.PRIOR, result.size(), outputRootDir, runDate, allowedTables, requestedTable);
        return result;
    }

    private List<FileDescriptor> discoverForTodayInput(String table) {
        Path inputDir = properties.inputDirPath();
        if (!Files.isDirectory(inputDir)) {
            throw new IllegalStateException("Input directory does not exist: " + inputDir);
        }

        Set<String> allowedTables = tableListService.resolveAllowedTables();
        String requestedTable = normalizeTable(table);
        List<FileDescriptor> result = new ArrayList<>();
        LocalDate runDate = properties.getRunDate();

        try (Stream<Path> stream = Files.walk(inputDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .forEach(path -> parser.parse(path, FileRole.TODAY)
                            .filter(fd -> allowedTables.contains(normalizeTable(fd.table())))
                            .filter(fd -> requestedTable == null || requestedTable.equals(normalizeTable(fd.table())))
                            .filter(fd -> runDate.equals(fd.fileDate()))
                            .ifPresent(result::add));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to scan input directory " + inputDir, e);
        }

        result.sort(Comparator.comparing(FileDescriptor::table, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(FileDescriptor::fileDate)
                .thenComparingInt(FileDescriptor::sequence));

        logDiscovery(FileRole.TODAY, result.size(), inputDir, runDate, allowedTables, requestedTable);
        return result;
    }

    private void logDiscovery(FileRole role, int count, Path dir, LocalDate runDate, Set<String> allowedTables, String requestedTable) {
        if (requestedTable == null) {
            log.info("event=file_discovery role={} count={} dir={} runDate={} tables={}",
                    role, count, dir, runDate, allowedTables);
        } else {
            log.info("event=file_discovery role={} count={} dir={} runDate={} tables={} requestedTable={}",
                    role, count, dir, runDate, allowedTables, requestedTable);
        }
    }

    private OutputLocation extractOutputLocation(Path outputRootDir, Path file) {
        Path relative = outputRootDir.relativize(file);

        // New format: <table>/<yyyy-mm-dd>/<file>
        if (relative.getNameCount() < 3) {
            return null;
        }

        String table = relative.getName(0).toString();
        String runDateText = relative.getName(1).toString();

        try {
            LocalDate runDate = LocalDate.parse(runDateText);
            return new OutputLocation(table, runDate);
        } catch (Exception e) {
            return null;
        }
    }

    private String normalizeTable(String table) {
        return table == null ? null : table.toLowerCase(Locale.ROOT);
    }

    private record OutputLocation(String table, LocalDate runDate) {
    }
}