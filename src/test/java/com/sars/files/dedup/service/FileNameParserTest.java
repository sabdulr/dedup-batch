package com.sars.files.dedup.service;

import com.sars.files.dedup.domain.FileDescriptor;
import com.sars.files.dedup.domain.FileRole;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FileNameParserTest {

    private final FileNameParser parser = new FileNameParser();

    @Test
    void parse_validFileName_returnsDescriptor() {
        Path path = Path.of("/tmp/input/2026-03-11/account/account-2026-03-11-2.json");

        Optional<FileDescriptor> result = parser.parse(path, FileRole.TODAY);

        assertTrue(result.isPresent());
        FileDescriptor descriptor = result.get();
        assertEquals("account", descriptor.table());
        assertEquals(LocalDate.of(2026, 3, 11), descriptor.fileDate());
        assertEquals(2, descriptor.sequence());
        assertEquals(path, descriptor.path());
        assertEquals("account-2026-03-11-2.json", descriptor.originalFileName());
        assertEquals(FileRole.TODAY, descriptor.role());
    }

    @Test
    void parse_sequenceLessThanOne_returnsEmpty() {
        assertTrue(parser.parse(Path.of("account-2026-03-11-0.json"), FileRole.TODAY).isEmpty());
    }

    @Test
    void parse_sequenceAbove24_isAccepted() {
        Optional<FileDescriptor> result =
                parser.parse(Path.of("account-2026-03-11-25.json"), FileRole.TODAY);

        assertTrue(result.isPresent());
        assertEquals(25, result.get().sequence());
        assertEquals("account", result.get().table());
        assertEquals(LocalDate.of(2026, 3, 11), result.get().fileDate());
    }

    @Test
    void parse_invalidExtensionOrDate_returnsEmpty() {
        assertTrue(parser.parse(Path.of("account-2026-03-11-1.txt"), FileRole.TODAY).isEmpty());
        assertTrue(parser.parse(Path.of("account-2026-13-11-1.json"), FileRole.TODAY).isEmpty());
    }

    @Test
    void parse_tableNameWithMixedCaseAndUnderscore_isAccepted() {
        Optional<FileDescriptor> result = parser.parse(
                Path.of("sars_PlasticHistory-2026-03-11-1.json"),
                FileRole.PRIOR
        );

        assertTrue(result.isPresent());
        assertEquals("sars_PlasticHistory", result.get().table());
        assertEquals(FileRole.PRIOR, result.get().role());
    }

    @Test
    void parse_missingSequence_returnsEmpty() {
        assertTrue(parser.parse(Path.of("account-2026-03-11.json"), FileRole.TODAY).isEmpty());
    }

    @Test
    void parse_nonJsonFile_returnsEmpty() {
        assertTrue(parser.parse(Path.of("account-2026-03-11-1.csv"), FileRole.TODAY).isEmpty());
    }
}