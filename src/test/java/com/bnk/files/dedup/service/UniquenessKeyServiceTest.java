package com.bnk.files.dedup.service;

import com.bnk.files.dedup.config.AppProperties;
import com.bnk.files.dedup.domain.FileDescriptor;
import com.bnk.files.dedup.domain.FileRole;
import com.bnk.files.dedup.domain.RecordEnvelope;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UniquenessKeyServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void key_sameLogicalRecord_producesSameHash() throws Exception {
        AppProperties properties = baseProperties();
        UniquenessKeyService service = new UniquenessKeyService(properties);

        RecordEnvelope left = envelope("""
                {"Source":{"Table":"account"},"ChangeData":{"ChangeDateTime":"2026-03-11T10:00:00Z"},"Data":{"_creationdate":"2026-03-01"}}
                """);
        RecordEnvelope right = envelope("""
                {"Source":{"Table":"account"},"ChangeData":{"ChangeDateTime":"2026-03-11T10:00:00Z"},"Data":{"_creationdate":"2026-03-01"}}
                """);

        assertArrayEquals(service.key(left), service.key(right));
    }

    @Test
    void key_differentConfiguredValue_producesDifferentHash() throws Exception {
        AppProperties properties = baseProperties();
        UniquenessKeyService service = new UniquenessKeyService(properties);

        RecordEnvelope left = envelope("""
                {"Source":{"Table":"account"},"ChangeData":{"ChangeDateTime":"2026-03-11T10:00:00Z"},"Data":{"_creationdate":"2026-03-01"}}
                """);
        RecordEnvelope right = envelope("""
                {"Source":{"Table":"account"},"ChangeData":{"ChangeDateTime":"2026-03-11T10:30:00Z"},"Data":{"_creationdate":"2026-03-01"}}
                """);

        assertFalse(Arrays.equals(service.key(left), service.key(right)));
    }

    @Test
    void resolvePaths_usesTableSpecificOverrideWhenPresent() {
        AppProperties properties = baseProperties();
        properties.setTableUniquenessFields(Map.of("award", List.of("Data.id")));

        UniquenessKeyService service = new UniquenessKeyService(properties);

        assertEquals(List.of("Data.id"), service.resolvePaths("award"));
        assertEquals(properties.getUniquenessFields(), service.resolvePaths("account"));
    }

    @Test
    void resolveTable_fallsBackToFileDescriptorTableWhenJsonFieldMissing() throws Exception {
        AppProperties properties = baseProperties();
        UniquenessKeyService service = new UniquenessKeyService(properties);

        RecordEnvelope envelope = new RecordEnvelope(
                new FileDescriptor(
                        "award",
                        LocalDate.of(2026, 3, 11),
                        1,
                        Path.of("award-2026-03-11-1.json"),
                        "award-2026-03-11-1.json",
                        FileRole.TODAY
                ),
                1,
                "{}",
                objectMapper.readTree("{}")
        );

        assertEquals("award", service.resolveTable(envelope));
    }

    @Test
    void key_usesTableSpecificOverrideWhenPresent() throws Exception {
        AppProperties properties = baseProperties();
        properties.setTableUniquenessFields(Map.of("award", List.of("Data.id")));

        UniquenessKeyService service = new UniquenessKeyService(properties);

        RecordEnvelope left = new RecordEnvelope(
                new FileDescriptor(
                        "award",
                        LocalDate.of(2026, 3, 11),
                        1,
                        Path.of("award-2026-03-11-1.json"),
                        "award-2026-03-11-1.json",
                        FileRole.TODAY
                ),
                1,
                "{\"Data\":{\"id\":\"100\",\"other\":\"left\"}}",
                objectMapper.readTree("{\"Data\":{\"id\":\"100\",\"other\":\"left\"}}")
        );

        RecordEnvelope right = new RecordEnvelope(
                new FileDescriptor(
                        "award",
                        LocalDate.of(2026, 3, 11),
                        2,
                        Path.of("award-2026-03-11-2.json"),
                        "award-2026-03-11-2.json",
                        FileRole.TODAY
                ),
                1,
                "{\"Data\":{\"id\":\"100\",\"other\":\"right\"}}",
                objectMapper.readTree("{\"Data\":{\"id\":\"100\",\"other\":\"right\"}}")
        );

        assertArrayEquals(service.key(left), service.key(right));
    }

    private AppProperties baseProperties() {
        AppProperties properties = new AppProperties();
        properties.setUniquenessFields(List.of("Source.Table", "ChangeData.ChangeDateTime", "Data._creationdate"));
        return properties;
    }

    private RecordEnvelope envelope(String json) throws Exception {
        return new RecordEnvelope(
                new FileDescriptor(
                        "account",
                        LocalDate.of(2026, 3, 11),
                        1,
                        Path.of("account-2026-03-11-1.json"),
                        "account-2026-03-11-1.json",
                        FileRole.TODAY
                ),
                1,
                json,
                objectMapper.readTree(json)
        );
    }
}