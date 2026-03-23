package com.sars.files.dedup.config;

import com.sars.files.dedup.batch.*;
import com.sars.files.dedup.batch.*;
import com.sars.files.dedup.domain.RecordEnvelope;
import com.sars.files.dedup.io.MalformedJsonException;
import com.sars.files.dedup.io.RestartableNdjsonLineReader;
import com.sars.files.dedup.io.RestartableNdjsonWriter;
import com.sars.files.dedup.logging.ExtendedStatsCollector;
import com.sars.files.dedup.logging.FileSummaryListener;
import com.sars.files.dedup.logging.ProgressLoggingListener;
import com.sars.files.dedup.service.FileDiscoveryService;
import com.sars.files.dedup.service.MergeOutputService;
import com.sars.files.dedup.service.TableListService;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class BatchJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final AppProperties appProperties;

    public BatchJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            AppProperties appProperties
    ) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.appProperties = appProperties;
    }

    @Bean
    public Job ndjsonDedupeJob(JobRepository jobRepository,
                               TaskExecutor batchTaskExecutor,
                               FileDiscoveryService discoveryService,
                               MergeOutputService mergeOutputService,
                               TableListService tableListService,
                               PriorDayIndexingProcessor priorProcessor,
                               TodayDeduplicationProcessor todayProcessor,
                               @Qualifier("priorReader") ItemStreamReader<RecordEnvelope> priorReader,
                               @Qualifier("todayReader") ItemStreamReader<RecordEnvelope> todayReader,
                               @Qualifier("priorWriter") ItemWriter<RecordEnvelope> priorWriter,
                               @Qualifier("todayWriter") RestartableNdjsonWriter todayWriter,
                               @Qualifier("progressLoggingListener") ProgressLoggingListener progressListener,
                               @Qualifier("priorFileSummaryListener") FileSummaryListener priorSummaryListener,
                               @Qualifier("todayFileSummaryListener") FileSummaryListener todaySummaryListener,
                               JobCompletionValidationListener validationListener) {

        List<String> tables = tableListService.resolveAllowedTablesOrdered();
        if (tables.isEmpty()) {
            throw new IllegalStateException("No tables configured. Provide app.tables or app.tables-file.");
        }

        List<Step> orderedSteps = new ArrayList<>();
        for (String table : tables) {
            Step priorWorker = buildPriorIndexWorkerStep(table, priorReader, priorProcessor, progressListener, priorWriter, priorSummaryListener);
            Step priorMaster = buildPriorIndexMasterStep(table, batchTaskExecutor, discoveryService, priorWorker);
            Step todayWorker = dedupeTodayWorkerStep(table, todayReader, todayProcessor, progressListener, todayWriter, todaySummaryListener);
            Step todayMaster = dedupeTodayMasterStep(table, batchTaskExecutor, discoveryService, todayWorker);
            Step mergeStep = mergeOutputsStep(table, mergeOutputService);
            orderedSteps.add(priorMaster);
            orderedSteps.add(todayMaster);
            orderedSteps.add(mergeStep);
        }

        JobBuilder builder = new JobBuilder("ndjsonDedupeJob", jobRepository).listener(validationListener);
        var iterator = orderedSteps.iterator();
        SimpleJobBuilder current = builder.start(iterator.next());
        while (iterator.hasNext()) {
            current = current.next(iterator.next());
        }
        return current.build();
    }

    private Step buildPriorIndexMasterStep(String table,
                                           TaskExecutor batchTaskExecutor,
                                           FileDiscoveryService discoveryService,
                                           Step workerStep) {
        Partitioner partitioner = new FilePartitioner(discoveryService.discoverPriorFiles(table));
        return new StepBuilder("buildPriorIndexMasterStep:" + table, jobRepository)
                .partitioner(workerStep.getName(), partitioner)
                .step(workerStep)
                .gridSize(appProperties.getGridSize())
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    private Step dedupeTodayMasterStep(String table,
                                       TaskExecutor batchTaskExecutor,
                                       FileDiscoveryService discoveryService,
                                       Step workerStep) {
        Partitioner partitioner = new FilePartitioner(discoveryService.discoverTodayFiles(table));
        return new StepBuilder("dedupeTodayMasterStep:" + table, jobRepository)
                .partitioner(workerStep.getName(), partitioner)
                .step(workerStep)
                .gridSize(appProperties.getGridSize())
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    private Step mergeOutputsStep(String table, MergeOutputService mergeOutputService) {
        return new StepBuilder("mergeOutputsStep:" + table, jobRepository)
                .tasklet(new MergeOutputsTasklet(mergeOutputService, table), transactionManager)
                .build();
    }

    private Step buildPriorIndexWorkerStep(String table,
                                           ItemStreamReader<RecordEnvelope> reader,
                                           PriorDayIndexingProcessor processor,
                                           ProgressLoggingListener progressListener,
                                           ItemWriter<RecordEnvelope> writer,
                                           FileSummaryListener summaryListener) {
        return new StepBuilder("buildPriorIndexWorkerStep:" + table, jobRepository)
                .<RecordEnvelope, RecordEnvelope>chunk(appProperties.getChunkSize())
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skip(MalformedJsonException.class)
                .skipLimit(1000)
                .listener((org.springframework.batch.core.listener.ItemReadListener<RecordEnvelope>) progressListener)
                .listener((org.springframework.batch.core.listener.ItemWriteListener<RecordEnvelope>) progressListener)
                .listener((org.springframework.batch.core.listener.SkipListener<RecordEnvelope, RecordEnvelope>) progressListener)
                .listener((org.springframework.batch.core.listener.StepExecutionListener) summaryListener)
                .build();
    }

    private Step dedupeTodayWorkerStep(String table,
                                       ItemStreamReader<RecordEnvelope> reader,
                                       TodayDeduplicationProcessor processor,
                                       ProgressLoggingListener progressListener,
                                       RestartableNdjsonWriter writer,
                                       FileSummaryListener summaryListener) {
        return new StepBuilder("dedupeTodayWorkerStep:" + table, jobRepository)
                .<RecordEnvelope, RecordEnvelope>chunk(appProperties.getChunkSize())
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener((org.springframework.batch.core.listener.ItemReadListener<RecordEnvelope>) progressListener)
                .listener((org.springframework.batch.core.listener.ItemWriteListener<RecordEnvelope>) progressListener)
                .listener((org.springframework.batch.core.listener.SkipListener<RecordEnvelope, RecordEnvelope>) progressListener)
                .listener((org.springframework.batch.core.listener.StepExecutionListener) summaryListener)
                .faultTolerant()
                .skip(MalformedJsonException.class)
                .skipLimit(Integer.MAX_VALUE)
                .build();
    }

    @Bean
    @StepScope
    public RestartableNdjsonLineReader priorReader(PartitionFileDescriptorFactory factory,
                                                   ObjectMapper objectMapper) {
        return new RestartableNdjsonLineReader(factory.current(), objectMapper, true);
    }

    @Bean
    @StepScope
    public RestartableNdjsonLineReader todayReader(PartitionFileDescriptorFactory factory,
                                                   ObjectMapper objectMapper) {
        return new RestartableNdjsonLineReader(factory.current(), objectMapper, true);
    }

    @Bean
    @StepScope
    public ItemWriter<RecordEnvelope> priorWriter() {
        return new NoOpItemWriter<>();
    }

    @Bean
    @StepScope
    public RestartableNdjsonWriter todayWriter(PartitionFileDescriptorFactory factory, AppProperties properties) {
        return new RestartableNdjsonWriter(factory.current(), properties.tempOutputDir());
    }

    @Bean
    @StepScope
    public FileSummaryListener priorFileSummaryListener(PartitionFileDescriptorFactory factory) {
        return new FileSummaryListener(factory.current().path(), null);
    }

    @Bean
    @StepScope
    public FileSummaryListener todayFileSummaryListener(PartitionFileDescriptorFactory factory,
                                                        @Qualifier("todayWriter") RestartableNdjsonWriter writer) {
        return new FileSummaryListener(factory.current().path(), writer);
    }

    @Bean
    @StepScope
    public ProgressLoggingListener progressLoggingListener(AppProperties properties,
                                                         ExtendedStatsCollector extendedStatsCollector) {
        return new ProgressLoggingListener(properties.getProgressLogInterval(), extendedStatsCollector);
    }
}
