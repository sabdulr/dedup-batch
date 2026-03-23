package com.bnk.files.dedup.io;


import com.bnk.files.dedup.domain.FileDescriptor;
import com.bnk.files.dedup.domain.RecordEnvelope;
//import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.util.Assert;

import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.infrastructure.item.file.separator.SimpleRecordSeparatorPolicy;


import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class RestartableNdjsonLineReader extends AbstractItemCountingItemStreamItemReader<RecordEnvelope> {

    private static final Logger log = LoggerFactory.getLogger(RestartableNdjsonLineReader.class);
    private static final String LINE_NUMBER_CONTEXT_KEY = "current.line.number";

    private final FileDescriptor fileDescriptor;
    private final ObjectMapper objectMapper;
    private final boolean strict;
    private final SimpleRecordSeparatorPolicy separatorPolicy = new SimpleRecordSeparatorPolicy();

    private BufferedReader reader;
    private long currentPhysicalLineNumber = 0L;

    public RestartableNdjsonLineReader(FileDescriptor fileDescriptor, ObjectMapper objectMapper, boolean strict) {
        this.fileDescriptor = fileDescriptor;
        this.objectMapper = objectMapper;
        this.strict = strict;
        setName("ndjsonReader-" + fileDescriptor.partitionName());
        setSaveState(true);
    }

    @Override
    protected void doOpen() throws Exception {
        if (strict) {
            Assert.isTrue(Files.exists(fileDescriptor.path()),
                    "Input file does not exist: " + fileDescriptor.path());
            Assert.isTrue(Files.isRegularFile(fileDescriptor.path()),
                    "Input path is not a regular file: " + fileDescriptor.path());
        }

        this.reader = Files.newBufferedReader(fileDescriptor.path(), StandardCharsets.UTF_8);
        this.currentPhysicalLineNumber = 0L;

        log.info("event=file_reader_open file={} role={} partition={}",
                fileDescriptor.path(), fileDescriptor.role(), fileDescriptor.partitionName());
    }

    @Override
    protected RecordEnvelope doRead() throws Exception {
        if (reader == null) {
            throw new IllegalStateException(
                    "Reader not opened for file " + fileDescriptor.path()
                            + ". Ensure the bean is declared as ItemStreamReader and wired as a step-scoped stream.");
        }

        String line;
        while ((line = reader.readLine()) != null) {
            currentPhysicalLineNumber++;

            if (line.isBlank()) {
                continue;
            }

            String record = separatorPolicy.postProcess(line);
            long lineNumber = currentPhysicalLineNumber;

            try {
                JsonNode jsonNode = objectMapper.readTree(record);
                return new RecordEnvelope(fileDescriptor, lineNumber, record, jsonNode);
            } catch (Exception ex) {
                throw new MalformedJsonException(
                        fileDescriptor.path(),
                        lineNumber,
                        "Malformed JSON at file=" + fileDescriptor.path() + " line=" + lineNumber,
                        ex);
            }
        }

        return null;
    }

    @Override
    protected void doClose() throws Exception {
        if (reader != null) {
            try {
                reader.close();
            } finally {
                reader = null;
            }
        }
    }

    @Override
    protected void jumpToItem(int itemIndex) throws Exception {
        if (reader == null) {
            throw new IllegalStateException(
                    "Reader not opened before restart jump for file " + fileDescriptor.path());
        }

        currentPhysicalLineNumber = 0L;

        int nonBlankItemsSkipped = 0;
        String line;
        while (nonBlankItemsSkipped < itemIndex && (line = reader.readLine()) != null) {
            currentPhysicalLineNumber++;
            if (!line.isBlank()) {
                nonBlankItemsSkipped++;
            }
        }

        log.info("event=file_reader_restart file={} partition={} resumeItem={} currentPhysicalLine={}",
                fileDescriptor.path(), fileDescriptor.partitionName(), itemIndex + 1L, currentPhysicalLineNumber);
    }
}