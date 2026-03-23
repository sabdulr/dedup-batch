package com.sars.files.dedup.service;

import com.sars.files.dedup.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

@Service
public class InputReadyFileValidator {

    private static final Logger log = LoggerFactory.getLogger(InputReadyFileValidator.class);

    private final AppProperties properties;

    public InputReadyFileValidator(AppProperties properties) {
        this.properties = properties;
    }

    public void validateReadyFileExists() {
        Path runDateInputDir = properties.runDateInputDir();
        LocalDate runDate = properties.getRunDate();

        Path readyFile = properties.readyFilePath();

        if (!Files.isDirectory(runDateInputDir)) {
            throw new IllegalStateException("Run-date input directory does not exist: " + runDateInputDir);
        }

        if (!Files.exists(readyFile) || !Files.isRegularFile(readyFile)) {
            throw new IllegalStateException("Required ready file does not exist: " + readyFile);
        }

        log.info("event=input_ready_file_found runDate={} readyFile={}", runDate, readyFile);
    }
}