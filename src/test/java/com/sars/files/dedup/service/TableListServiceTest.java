package com.sars.files.dedup.service;

import com.sars.files.dedup.config.AppProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TableListServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void resolveAllowedTables_prefersTablesFileAndNormalizesCase() throws Exception {
        Path tablesFile = tempDir.resolve("tables.txt");
        Files.writeString(tablesFile, "account\n# comment\n award \nsars_PlasticHistory\naccount\n\n");

        AppProperties properties = new AppProperties();
        properties.setTables(List.of("ignored"));
        properties.setTablesFile(tablesFile);

        TableListService service = new TableListService(properties);

        Set<String> result = service.resolveAllowedTables();

        assertEquals(Set.of("account", "award", "sars_plastichistory"), result);
    }

    @Test
    void resolveAllowedTables_usesConfiguredListWhenNoFilePresent() {
        AppProperties properties = new AppProperties();
        properties.setTables(List.of(" account ", "Award", "", "award"));

        TableListService service = new TableListService(properties);

        Set<String> result = service.resolveAllowedTables();

        assertEquals(Set.of("account", "award"), result);
    }

    @Test
    void resolveAllowedTables_throwsWhenNoUsableTablesConfigured() {
        AppProperties properties = new AppProperties();
        properties.setTables(List.of(" ", "  "));

        TableListService service = new TableListService(properties);

        IllegalStateException ex = assertThrows(IllegalStateException.class, service::resolveAllowedTables);
        assertTrue(ex.getMessage().contains("No tables configured"));
    }

    @Test
    void resolveAllowedTables_throwsWhenTablesFileCannotBeRead() {
        AppProperties properties = new AppProperties();
        properties.setTablesFile(tempDir.resolve("missing.txt"));

        TableListService service = new TableListService(properties);

        IllegalStateException ex = assertThrows(IllegalStateException.class, service::resolveAllowedTables);
        assertTrue(ex.getMessage().contains("Failed to read tables file"));
    }

    @Test
    void resolveAllowedTables_ignoresBlankAndCommentOnlyFileLines() throws Exception {
        Path tablesFile = tempDir.resolve("tables.txt");
        Files.writeString(tablesFile, "\n# comment one\n   \n# comment two\naccount\n");

        AppProperties properties = new AppProperties();
        properties.setTablesFile(tablesFile);

        TableListService service = new TableListService(properties);

        Set<String> result = service.resolveAllowedTables();

        assertEquals(Set.of("account"), result);
    }
}