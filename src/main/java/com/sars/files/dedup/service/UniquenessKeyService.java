package com.sars.files.dedup.service;

import com.sars.files.dedup.config.AppProperties;
import com.sars.files.dedup.domain.RecordEnvelope;
//import tools.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

@Service
public class UniquenessKeyService {

    private static final byte SEP = 0x1F;

    private final AppProperties properties;

    public UniquenessKeyService(AppProperties properties) {
        this.properties = properties;
    }

    public byte[] key(RecordEnvelope envelope) {
        String table = resolveTable(envelope);
        List<String> paths = resolvePaths(table);
        return sha256(materialize(table, envelope.jsonNode(), paths));
    }

    public List<String> resolvePaths(String table) {
        Map<String, List<String>> perTable = properties.getTableUniquenessFields();
        return perTable.getOrDefault(table, properties.getUniquenessFields());
    }

    public String resolveTable(RecordEnvelope envelope) {
        JsonNode sourceTable = readPath(envelope.jsonNode(), "Source.Table");
        if (sourceTable != null && !sourceTable.isNull()) {
            //return sourceTable.asText();
            return sourceTable.asString();
        }
        return envelope.fileDescriptor().table();
    }

    private byte[] materialize(String table, JsonNode node, List<String> paths) {
        List<byte[]> parts = new ArrayList<>();
        parts.add(table.getBytes(StandardCharsets.UTF_8));
        for (String path : paths) {
            JsonNode value = readPath(node, path);
            parts.add(path.getBytes(StandardCharsets.UTF_8));
            parts.add(serialize(value).getBytes(StandardCharsets.UTF_8));
        }
        int total = parts.stream().mapToInt(b -> b.length + 1).sum();
        byte[] bytes = new byte[total];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, bytes, offset, part.length);
            offset += part.length;
            bytes[offset++] = SEP;
        }
        return bytes;
    }

    private String serialize(JsonNode node) {
        if (node == null || node.isNull()) {
            return "<null>";
        }
        if (node.isValueNode()) {
            //return node.asText();
            return node.asString();
        }
        return node.toString();
    }

    private JsonNode readPath(JsonNode root, String dottedPath) {
        JsonNode current = root;
        for (String segment : dottedPath.split("\\.")) {
            if (current == null) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }

    private byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    public String toHex(byte[] key) {
        return HexFormat.of().formatHex(key);
    }
}