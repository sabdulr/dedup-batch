package com.sars.files.dedup.batch;

import com.sars.files.dedup.domain.FileDescriptor;
import com.sars.files.dedup.domain.FileRole;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDate;

@Component
public class PartitionFileDescriptorFactory {

    public FileDescriptor current() {
        var context = StepSynchronizationManager.getContext();
        if (context == null) {
            throw new IllegalStateException("No step context available");
        }
        var executionContext = context.getStepExecution().getExecutionContext();
        return new FileDescriptor(
                executionContext.getString("table"),
                LocalDate.parse(executionContext.getString("fileDate")),
                executionContext.getInt("sequence"),
                Path.of(executionContext.getString("filePath")),
                executionContext.getString("fileName"),
                FileRole.valueOf(executionContext.getString("role"))
        );
    }
}
