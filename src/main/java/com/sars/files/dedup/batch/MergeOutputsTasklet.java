package com.sars.files.dedup.batch;

import com.sars.files.dedup.service.MergeOutputService;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

public class MergeOutputsTasklet implements Tasklet {

    private final MergeOutputService mergeOutputService;
    private final String table;

    public MergeOutputsTasklet(MergeOutputService mergeOutputService, String table) {
        this.mergeOutputService = mergeOutputService;
        this.table = table;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        mergeOutputService.mergeTable(table);
        return RepeatStatus.FINISHED;
    }
}
