package com.sars.files.dedup.batch;

import com.sars.files.dedup.domain.FileDescriptor;
//import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.Partitioner;
//import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FilePartitioner implements Partitioner {

    private final List<FileDescriptor> files;

    public FilePartitioner(List<FileDescriptor> files) {
        this.files = files;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
        int i = 0;
        for (FileDescriptor file : files) {
            ExecutionContext context = new ExecutionContext();
            context.putString("filePath", file.path().toString());
            context.putString("fileName", file.originalFileName());
            context.putString("table", file.table());
            context.putString("fileDate", file.fileDate().toString());
            context.putInt("sequence", file.sequence());
            context.putString("role", file.role().name());
            partitions.put(file.partitionName() + "-" + (++i), context);
        }
        return partitions;
    }
}
