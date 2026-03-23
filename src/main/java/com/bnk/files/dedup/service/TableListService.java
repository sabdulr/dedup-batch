package com.bnk.files.dedup.service;

import com.bnk.files.dedup.config.AppProperties;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class TableListService {

    private final AppProperties properties;

    public TableListService(AppProperties properties) {
        this.properties = properties;
    }

    public Set<String> resolveAllowedTables() {
        Set<String> tables;

        if (properties.getTablesFile() != null) {
            try {
                tables = Files.readAllLines(properties.getTablesFile()).stream()
                        .map(String::trim)
                        .filter(s -> !s.isBlank())
                        .filter(s -> !s.startsWith("#"))
                        .map(s -> s.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read tables file: " + properties.getTablesFile(), e);
            }
        } else {
            List<String> configuredTables = properties.getTables();
            if (configuredTables == null) {
                configuredTables = List.of();
            }

            tables = configuredTables.stream()
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .map(s -> s.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }

        if (tables.isEmpty()) {
            throw new IllegalStateException("No tables configured. Provide app.tables or app.tables-file.");
        }

        return tables;
    }

    public List<String> resolveAllowedTablesOrdered() {
        return List.copyOf(resolveAllowedTables());
    }
}