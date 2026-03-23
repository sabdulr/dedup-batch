package com.sars.files.dedup.service;

import com.sars.files.dedup.domain.FileDescriptor;
import com.sars.files.dedup.domain.FileRole;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class FileNameParser {

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("^([A-Za-z0-9_]+)-(\\d{4}-\\d{2}-\\d{2})-(\\d+)\\.json$");

    public Optional<FileDescriptor> parse(Path path, FileRole role) {
        if (path == null || path.getFileName() == null) {
            return Optional.empty();
        }

        String fileName = path.getFileName().toString();
        Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return Optional.empty();
        }

        String table = matcher.group(1);

        LocalDate fileDate;
        try {
            fileDate = LocalDate.parse(matcher.group(2));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }

        int sequence;
        try {
            sequence = Integer.parseInt(matcher.group(3));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }

        if (sequence < 1) {
            return Optional.empty();
        }

        return Optional.of(new FileDescriptor(
                table,
                fileDate,
                sequence,
                path,
                fileName,
                role
        ));
    }
}