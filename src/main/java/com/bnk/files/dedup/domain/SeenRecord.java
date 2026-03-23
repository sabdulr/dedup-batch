package com.bnk.files.dedup.domain;

import java.nio.charset.StandardCharsets;

public record SeenRecord(String fileName, long lineNumber) {

    private static final String SEPARATOR = "|";

    public byte[] encode() {
        return (escape(fileName) + SEPARATOR + lineNumber).getBytes(StandardCharsets.UTF_8);
    }

    public static SeenRecord decode(byte[] bytes) {
        String raw = new String(bytes, StandardCharsets.UTF_8);
        int separatorIndex = findSeparator(raw);
        if (separatorIndex < 0) {
            throw new IllegalArgumentException("Invalid SeenRecord payload");
        }

        String fileName = unescape(raw.substring(0, separatorIndex));
        long lineNumber = Long.parseLong(raw.substring(separatorIndex + 1));
        return new SeenRecord(fileName, lineNumber);
    }

    private static int findSeparator(String raw) {
        boolean escaped = false;
        for (int i = 0; i < raw.length(); i++) {
            char current = raw.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (current == '\\') {
                escaped = true;
                continue;
            }
            if (current == '|') {
                return i;
            }
        }
        return -1;
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace(SEPARATOR, "\\|");
    }

    private static String unescape(String value) {
        StringBuilder result = new StringBuilder(value.length());
        boolean escaped = false;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (escaped) {
                result.append(current);
                escaped = false;
            } else if (current == '\\') {
                escaped = true;
            } else {
                result.append(current);
            }
        }
        if (escaped) {
            result.append('\\');
        }
        return result.toString();
    }
}
