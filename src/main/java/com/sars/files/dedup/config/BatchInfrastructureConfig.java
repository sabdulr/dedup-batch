package com.sars.files.dedup.config;

import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableJdbcJobRepository
public class BatchInfrastructureConfig {

    @Bean
    public TaskExecutor batchTaskExecutor(AppProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getGridSize());
        executor.setMaxPoolSize(properties.getGridSize());
        //executor.setQueueCapacity(properties.getGridSize() * 2);
        executor.setQueueCapacity(properties.getTaskQueueCapacity());
        executor.setThreadNamePrefix("batch-");

        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(300);
        executor.setAcceptTasksAfterContextClose(false);

        executor.initialize();
        return executor;
    }
}