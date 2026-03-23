package com.bnk.files.dedup.domain;

import java.nio.file.Path;
import java.time.LocalDate;

public record FileDescriptor(String table,
                             LocalDate fileDate,
                             int sequence,
                             Path path,
                             String originalFileName,
                             FileRole role) {

    public String mergedOutputFileName() {
        return "MERGED=" + table + "-" + fileDate + ".json";
    }

    public String partitionName() {
        return role.name().toLowerCase() + "-" + table + "-" + sequence;
    }
}
