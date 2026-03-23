package com.bnk.files.dedup.logging;

import com.bnk.files.dedup.domain.SeenRecord;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class ExtendedStatsCollector {

    private final List<String> duplicateEntries =
            Collections.synchronizedList(new ArrayList<>());

    private final List<String> rejectedEntries =
            Collections.synchronizedList(new ArrayList<>());

    public void addDuplicate(long currentLineNumber,
                             String currentFile,
                             SeenRecord firstSeen) {
        duplicateEntries.add(
                "Line " + currentLineNumber + " in " + currentFile
                        + " is a duplicate of line " + firstSeen.lineNumber()
                        + " in " + firstSeen.fileName()
        );
    }

    public void addRejected(long lineNumber, String inputFile) {
        addRejected(lineNumber, inputFile, "invalid data/format");
    }

    public void addRejected(long lineNumber, String inputFile, String reason) {
        String finalReason = (reason == null || reason.isBlank())
                ? "invalid data/format"
                : reason;

        rejectedEntries.add(
                "Line " + lineNumber + " in " + inputFile
                        + " has been rejected due to " + finalReason
        );
    }

    public List<String> getDuplicateEntries() {
        return List.copyOf(duplicateEntries);
    }

    public List<String> getRejectedEntries() {
        return List.copyOf(rejectedEntries);
    }

    public void clear() {
        duplicateEntries.clear();
        rejectedEntries.clear();
    }
}