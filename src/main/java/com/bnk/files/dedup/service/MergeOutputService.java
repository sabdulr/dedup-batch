package com.bnk.files.dedup.service;

import com.bnk.files.dedup.config.AppProperties;
import com.bnk.files.dedup.domain.FileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class MergeOutputService {

    private static final Logger log = LoggerFactory.getLogger(MergeOutputService.class);

    private final AppProperties properties;
    private final FileDiscoveryService discoveryService;

    public MergeOutputService(AppProperties properties, FileDiscoveryService discoveryService) {
        this.properties = properties;
        this.discoveryService = discoveryService;
    }

    public void mergeAllTodayTables() {
        List<FileDescriptor> todayFiles = discoveryService.discoverTodayFiles();
        Map<String, List<FileDescriptor>> byTable = todayFiles.stream()
                .collect(Collectors.groupingBy(FileDescriptor::table));
        byTable.forEach(this::mergeTable);
    }

    public void mergeTable(String table) {
        mergeTable(table, discoveryService.discoverTodayFiles(table));
    }

    private void mergeTable(String requestedTable, List<FileDescriptor> files) {
        if (files == null || files.isEmpty()) {
            log.info("event=merge_skipped_no_input_files table={} runDate={}", requestedTable, properties.getRunDate());
            return;
        }

        List<FileDescriptor> ordered = files.stream()
                .sorted(Comparator.comparingInt(FileDescriptor::sequence))
                .toList();

        String actualTable = ordered.getFirst().table();
        LocalDate runDate = properties.getRunDate();

        // New format: output/<table>/<run-date>/
        Path finalDir = properties.outputDirPath()
                .resolve(actualTable)
                .resolve(runDate.toString());

        Path tempDir = properties.tempOutputDir()
                .resolve("_merged")
                .resolve(actualTable);

        String mergedFileName = "MERGED=" + actualTable + "-" + runDate + ".json";
        Path finalFile = finalDir.resolve(mergedFileName);
        Path tempFile = tempDir.resolve(mergedFileName + ".part");

        List<Path> existingPartitionTemps = ordered.stream()
                .map(file -> properties.tempOutputDir()
                        .resolve(file.table())
                        .resolve(String.format("%02d-%s.part", file.sequence(), file.originalFileName())))
                .filter(Files::exists)
                .toList();

        if (existingPartitionTemps.isEmpty()) {
            log.info("event=merge_skipped_no_data requestedTable={} actualTable={} runDate={}",
                    requestedTable, actualTable, runDate);
            return;
        }

        try {
            Files.createDirectories(finalDir);
            Files.createDirectories(tempDir);

            try (BufferedWriter writer = Files.newBufferedWriter(
                    tempFile,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                for (Path partitionTemp : existingPartitionTemps) {
                    try (Stream<String> lines = Files.lines(partitionTemp, StandardCharsets.UTF_8)) {
                        var iterator = lines.iterator();
                        while (iterator.hasNext()) {
                            writer.write(iterator.next());
                            writer.newLine();
                        }
                    }
                }
            }

            moveAtomically(tempFile, finalFile);

            log.info("event=merge_complete requestedTable={} actualTable={} runDate={} finalFile={} sourceFileCount={}",
                    requestedTable, actualTable, runDate, finalFile, existingPartitionTemps.size());

        } catch (IOException e) {
            throw new IllegalStateException("Failed to merge outputs for requestedTable=" + requestedTable
                    + ", actualTable=" + actualTable, e);
        }
    }

    private void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}