package com.sars.files.dedup;

import com.sars.files.dedup.batch.JobLaunchService;
import com.sars.files.dedup.config.AppProperties;
import com.sars.files.dedup.service.InputReadyFileValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

@Slf4j
@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
@RequiredArgsConstructor
public class DedupApplication implements ApplicationRunner {

    private final JobLaunchService jobLaunchService;
    private final InputReadyFileValidator inputReadyFileValidator;
    private final ConfigurableApplicationContext applicationContext;

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(DedupApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args);
    }

    @Override
    public void run(ApplicationArguments args) {
        int exitCode = 0;

        try {
            log.info("event=application_runner_start");
            inputReadyFileValidator.validateReadyFileExists();
            jobLaunchService.launch();
            log.info("event=application_exit status=SUCCESS");
        } catch (Exception e) {
            exitCode = 1;
            log.error("event=application_runner_failure message={}", e.getMessage(), e);
            log.info("event=application_exit status=FAILED");
        } finally {
            int finalExitCode = exitCode;
            int springExitCode = SpringApplication.exit(applicationContext, (ExitCodeGenerator) () -> finalExitCode);
            System.exit(springExitCode);
        }
    }
}