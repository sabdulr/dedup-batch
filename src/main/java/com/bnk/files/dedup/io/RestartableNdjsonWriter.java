package com.bnk.files.dedup.io;

//import com.example.ndjsondedupe.domain.FileDescriptor;
//import com.example.ndjsondedupe.domain.RecordEnvelope;
import com.bnk.files.dedup.domain.FileDescriptor;
import com.bnk.files.dedup.domain.RecordEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.ItemStreamWriter;
//import org.springframework.batch.item.Chunk;
//import org.springframework.batch.item.ExecutionContext;
//import org.springframework.batch.item.ItemStreamException;
//import org.springframework.batch.item.ItemStreamWriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class RestartableNdjsonWriter implements ItemStreamWriter<RecordEnvelope> {

    private static final Logger log = LoggerFactory.getLogger(RestartableNdjsonWriter.class);

    private final FileDescriptor fileDescriptor;
    private final Path partitionTempFile;
    private BufferedWriter writer;
    private long writtenCount;

    public RestartableNdjsonWriter(FileDescriptor fileDescriptor, Path tempOutputDir) {
        this.fileDescriptor = fileDescriptor;
        this.partitionTempFile = tempOutputDir.resolve(fileDescriptor.table())
                .resolve(String.format("%02d-%s.part", fileDescriptor.sequence(), fileDescriptor.originalFileName()));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.writtenCount = executionContext.getLong(contextKey("writtenCount"), 0L);

        if (writtenCount > 0L) {
            openWriterIfNeeded();
        }

        log.info("event=file_writer_open file={} tempFile={} writtenCount={}",
                fileDescriptor.path(), partitionTempFile, writtenCount);
    }

    @Override
    public void write(Chunk<? extends RecordEnvelope> chunk) throws Exception {
        if (chunk == null || chunk.isEmpty()) {
            return;
        }

        openWriterIfNeeded();

        for (RecordEnvelope item : chunk) {
            writer.write(item.rawLine());
            writer.newLine();
            writtenCount++;
        }
        writer.flush();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(contextKey("writtenCount"), writtenCount);
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Failed to close writer", e);
        }
    }

    public Path getPartitionTempFile() {
        return partitionTempFile;
    }

    private void openWriterIfNeeded() {
        if (writer != null) {
            return;
        }

        try {
            Files.createDirectories(partitionTempFile.getParent());
            writer = Files.newBufferedWriter(
                    partitionTempFile,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            throw new ItemStreamException("Failed to open writer for " + partitionTempFile, e);
        }
    }

    private String contextKey(String suffix) {
        return fileDescriptor.partitionName() + "." + suffix;
    }
}