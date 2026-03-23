package com.sars.files.dedup.service;

import com.sars.files.dedup.config.AppProperties;
import com.sars.files.dedup.domain.FileDescriptor;
import com.sars.files.dedup.domain.FileRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

class MergeOutputServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void mergeAllTodayTables_mergesPartitionFilesInSequenceOrder() throws Exception {
        AppProperties properties = new AppProperties();
        properties.setRunDate(LocalDate.of(2026, 3, 11));
        properties.setInputDir(tempDir.resolve("input"));
        properties.setOutputDir(tempDir.resolve("output"));
        properties.setWorkDir(tempDir.resolve("work"));

        FileDescriptor seq2 = descriptor("account", 2, "account-2026-03-11-2.json");
        FileDescriptor seq1 = descriptor("account", 1, "account-2026-03-11-1.json");
        FileDescriptor award = descriptor("award", 1, "award-2026-03-11-1.json");

        writePartitionTemp(properties, seq1, List.of("a1", "a2"));
        writePartitionTemp(properties, seq2, List.of("a3"));
        writePartitionTemp(properties, award, List.of("w1"));

        FileDiscoveryService discoveryService = Mockito.mock(FileDiscoveryService.class);
        when(discoveryService.discoverTodayFiles()).thenReturn(List.of(seq2, award, seq1));
        when(discoveryService.discoverTodayFiles("account")).thenReturn(List.of(seq2, seq1));
        when(discoveryService.discoverTodayFiles("award")).thenReturn(List.of(award));

        MergeOutputService service = new MergeOutputService(properties, discoveryService);

        service.mergeAllTodayTables();

        Path accountOutput = properties.getOutputDir()
                .resolve("account")
                .resolve("2026-03-11")
                .resolve("MERGED=account-2026-03-11.json");
        Path awardOutput = properties.getOutputDir()
                .resolve("award")
                .resolve("2026-03-11")
                .resolve("MERGED=award-2026-03-11.json");

        assertEquals(List.of("a1", "a2", "a3"), Files.readAllLines(accountOutput));
        assertEquals(List.of("w1"), Files.readAllLines(awardOutput));
    }

    @Test
    void mergeAllTodayTables_skipsMissingPartitionTempFile() throws Exception {
        AppProperties properties = new AppProperties();
        properties.setRunDate(LocalDate.of(2026, 3, 11));
        properties.setInputDir(tempDir.resolve("input"));
        properties.setOutputDir(tempDir.resolve("output"));
        properties.setWorkDir(tempDir.resolve("work"));

        FileDescriptor seq1 = descriptor("account", 1, "account-2026-03-11-1.json");
        FileDescriptor seq2 = descriptor("account", 2, "account-2026-03-11-2.json");
        writePartitionTemp(properties, seq1, List.of("a1"));

        FileDiscoveryService discoveryService = Mockito.mock(FileDiscoveryService.class);
        when(discoveryService.discoverTodayFiles()).thenReturn(List.of(seq1, seq2));
        when(discoveryService.discoverTodayFiles("account")).thenReturn(List.of(seq1, seq2));

        MergeOutputService service = new MergeOutputService(properties, discoveryService);

        service.mergeAllTodayTables();

        Path accountOutput = properties.getOutputDir()
                .resolve("account")
                .resolve("2026-03-11")
                .resolve("MERGED=account-2026-03-11.json");

        assertEquals(List.of("a1"), Files.readAllLines(accountOutput));
    }

    @Test
    void mergeAllTodayTables_createsSeparateMergedFilesPerTable() throws Exception {
        AppProperties properties = new AppProperties();
        properties.setRunDate(LocalDate.of(2026, 3, 11));
        properties.setInputDir(tempDir.resolve("input"));
        properties.setOutputDir(tempDir.resolve("output"));
        properties.setWorkDir(tempDir.resolve("work"));

        FileDescriptor account = descriptor("account", 1, "account-2026-03-11-1.json");
        FileDescriptor award = descriptor("award", 1, "award-2026-03-11-1.json");

        writePartitionTemp(properties, account, List.of("a1"));
        writePartitionTemp(properties, award, List.of("w1", "w2"));

        FileDiscoveryService discoveryService = Mockito.mock(FileDiscoveryService.class);
        when(discoveryService.discoverTodayFiles()).thenReturn(List.of(account, award));
        when(discoveryService.discoverTodayFiles("account")).thenReturn(List.of(account));
        when(discoveryService.discoverTodayFiles("award")).thenReturn(List.of(award));

        MergeOutputService service = new MergeOutputService(properties, discoveryService);

        service.mergeAllTodayTables();

        Path accountOutput = properties.getOutputDir()
                .resolve("account")
                .resolve("2026-03-11")
                .resolve("MERGED=account-2026-03-11.json");
        Path awardOutput = properties.getOutputDir()
                .resolve("award")
                .resolve("2026-03-11")
                .resolve("MERGED=award-2026-03-11.json");

        assertEquals(List.of("a1"), Files.readAllLines(accountOutput));
        assertEquals(List.of("w1", "w2"), Files.readAllLines(awardOutput));
    }

    private FileDescriptor descriptor(String table, int sequence, String fileName) {
        return new FileDescriptor(
                table,
                LocalDate.of(2026, 3, 11),
                sequence,
                tempDir.resolve("input").resolve("2026-03-11").resolve(table).resolve(fileName),
                fileName,
                FileRole.TODAY
        );
    }

    private void writePartitionTemp(AppProperties properties, FileDescriptor descriptor, List<String> lines) throws Exception {
        Path partitionTemp = properties.tempOutputDir()
                .resolve(descriptor.table())
                .resolve(String.format("%02d-%s.part", descriptor.sequence(), descriptor.originalFileName()));
        Files.createDirectories(partitionTemp.getParent());
        Files.write(partitionTemp, lines);
    }
}