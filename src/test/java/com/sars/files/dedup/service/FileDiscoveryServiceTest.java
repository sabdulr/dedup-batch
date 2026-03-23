package com.sars.files.dedup.service;

import com.sars.files.dedup.config.AppProperties;
import com.sars.files.dedup.domain.FileDescriptor;
import com.sars.files.dedup.domain.FileRole;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileDiscoveryServiceTest {

    @TempDir
    Path tempDir;

    @Mock
    private AppProperties properties;

    @Mock
    private TableListService tableListService;

    private FileDiscoveryService service;

    @BeforeEach
    void setUp() {
        service = new FileDiscoveryService(
                properties,
                new FileNameParser(),
                tableListService
        );
    }

    @Test
    void discoverTodayFiles_filtersByRunDateAndAllowedTables_andSortsBySequence() throws Exception {
        Path inputDir = tempDir.resolve("input");

        createInputFile(inputDir, "2026-03-11", "account", "account-2026-03-11-2.json");
        createInputFile(inputDir, "2026-03-11", "account", "account-2026-03-11-1.json");
        createInputFile(inputDir, "2026-03-11", "award", "award-2026-03-11-1.json");
        createInputFile(inputDir, "2026-03-10", "account", "account-2026-03-10-1.json");
        createInputFile(inputDir, "2026-03-11", "ignored", "ignored-2026-03-11-1.json");

        when(properties.getRunDate()).thenReturn(LocalDate.of(2026, 3, 11));
        when(properties.inputDirPath()).thenReturn(inputDir);
        when(tableListService.resolveAllowedTables()).thenReturn(Set.of("account", "award"));

        List<FileDescriptor> result = service.discoverTodayFiles();

        assertEquals(3, result.size());
        assertEquals(List.of("account", "account", "award"), result.stream().map(FileDescriptor::table).toList());
        assertEquals(List.of(1, 2, 1), result.stream().map(FileDescriptor::sequence).toList());
        assertTrue(result.stream().allMatch(fd -> fd.fileDate().equals(LocalDate.of(2026, 3, 11))));
        assertTrue(result.stream().allMatch(fd -> fd.role() == FileRole.TODAY));
        assertEquals(
                List.of(
                        "account-2026-03-11-1.json",
                        "account-2026-03-11-2.json",
                        "award-2026-03-11-1.json"
                ),
                result.stream().map(FileDescriptor::originalFileName).toList()
        );
    }

    @Test
    void discoverTodayFiles_throwsWhenInputDirDoesNotExist() {
        Path inputDir = tempDir.resolve("missing-input");

        when(properties.inputDirPath()).thenReturn(inputDir);

        IllegalStateException ex = assertThrows(IllegalStateException.class, service::discoverTodayFiles);
        assertTrue(ex.getMessage().contains("Input directory does not exist"));
    }

    @Test
    void discoverPriorFiles_readsBaselineFilesFromOutputRunDateDir() throws Exception {
        Path outputDir = tempDir.resolve("output");

        createOutputFile(outputDir, "2026-03-11", "account", "MERGED=account-2026-03-11.json");
        createOutputFile(outputDir, "2026-03-11", "award", "MERGED=award-2026-03-11.json");
        createOutputFile(outputDir, "2026-03-10", "account", "MERGED=account-2026-03-10.json");

        when(properties.getRunDate()).thenReturn(LocalDate.of(2026, 3, 11));
        when(properties.outputDirPath()).thenReturn(outputDir);
        when(tableListService.resolveAllowedTables()).thenReturn(Set.of("account", "award"));

        List<FileDescriptor> result = service.discoverPriorFiles();

        assertEquals(2, result.size());
        assertEquals(List.of("account", "award"), result.stream().map(FileDescriptor::table).toList());
        assertTrue(result.stream().allMatch(fd -> fd.fileDate().equals(LocalDate.of(2026, 3, 11))));
        assertTrue(result.stream().allMatch(fd -> fd.role() == FileRole.PRIOR));
        assertEquals(
                List.of("MERGED=account-2026-03-11.json", "MERGED=award-2026-03-11.json"),
                result.stream().map(FileDescriptor::originalFileName).toList()
        );
        assertTrue(result.stream().anyMatch(fd -> fd.path().startsWith(outputDir.resolve("account").resolve("2026-03-11"))));
        assertTrue(result.stream().anyMatch(fd -> fd.path().startsWith(outputDir.resolve("award").resolve("2026-03-11"))));
        assertEquals(List.of(1, 1), result.stream().map(FileDescriptor::sequence).toList());
    }

    @Test
    void discoverPriorFiles_returnsEmptyWhenBaselineOutputDirDoesNotExist() {
        Path outputDir = tempDir.resolve("output");

        when(properties.getRunDate()).thenReturn(LocalDate.of(2026, 3, 11));
        when(properties.outputDirPath()).thenReturn(outputDir);
        when(tableListService.resolveAllowedTables()).thenReturn(Set.of("account"));

        List<FileDescriptor> result = service.discoverPriorFiles();

        assertTrue(result.isEmpty());
    }

    @Test
    void discoverPriorFiles_ignoresFilesOutsideAllowedTables() throws Exception {
        Path outputDir = tempDir.resolve("output");

        createOutputFile(outputDir, "2026-03-11", "account", "MERGED=account-2026-03-11.json");
        createOutputFile(outputDir, "2026-03-11", "ignored", "MERGED=ignored-2026-03-11.json");

        when(properties.getRunDate()).thenReturn(LocalDate.of(2026, 3, 11));
        when(properties.outputDirPath()).thenReturn(outputDir);
        when(tableListService.resolveAllowedTables()).thenReturn(Set.of("account"));

        List<FileDescriptor> result = service.discoverPriorFiles();

        assertEquals(1, result.size());
        assertEquals("account", result.getFirst().table());
        assertEquals("MERGED=account-2026-03-11.json", result.getFirst().originalFileName());
        assertTrue(result.getFirst().path().startsWith(outputDir.resolve("account").resolve("2026-03-11")));
    }

    @Test
    void discoverPriorFiles_ignoresFilesNotUnderTableDateLayout() throws Exception {
        Path outputDir = tempDir.resolve("output");
        Path strayFile = outputDir.resolve("2026-03-11").resolve("MERGED=account-2026-03-11.json");
        Files.createDirectories(strayFile.getParent());
        Files.writeString(strayFile, "{}\n");

        when(properties.getRunDate()).thenReturn(LocalDate.of(2026, 3, 11));
        when(properties.outputDirPath()).thenReturn(outputDir);
        when(tableListService.resolveAllowedTables()).thenReturn(Set.of("account"));

        List<FileDescriptor> result = service.discoverPriorFiles();

        assertTrue(result.isEmpty());
    }

    @Test
    void discoverPriorFiles_ignoresNonJsonFiles() throws Exception {
        Path outputDir = tempDir.resolve("output");
        Path nonJson = outputDir.resolve("account").resolve("2026-03-11").resolve("MERGED=account-2026-03-11.txt");
        Files.createDirectories(nonJson.getParent());
        Files.writeString(nonJson, "ignored\n");

        when(properties.getRunDate()).thenReturn(LocalDate.of(2026, 3, 11));
        when(properties.outputDirPath()).thenReturn(outputDir);
        when(tableListService.resolveAllowedTables()).thenReturn(Set.of("account"));

        List<FileDescriptor> result = service.discoverPriorFiles();

        assertTrue(result.isEmpty());
    }

    private void createInputFile(Path inputDir, String date, String table, String fileName) throws Exception {
        Path file = inputDir.resolve(date).resolve(table).resolve(fileName);
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{}\n");
    }

    private void createOutputFile(Path outputDir, String date, String table, String fileName) throws Exception {
        Path file = outputDir.resolve(table).resolve(date).resolve(fileName);
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{}\n");
    }
}