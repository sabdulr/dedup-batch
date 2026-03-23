package com.bnk.files.dedup.io;

import java.nio.file.Path;

public class MalformedJsonException extends RuntimeException {

    private final Path file;
    private final long lineNumber;

    public MalformedJsonException(Path file, long lineNumber, String message, Throwable cause) {
        super(message, cause);
        this.file = file;
        this.lineNumber = lineNumber;
    }

    public Path getFile() {
        return file;
    }

    public long getLineNumber() {
        return lineNumber;
    }
}
