package com.bnk.files.dedup.logging;

import com.bnk.files.dedup.domain.RecordEnvelope;
import com.bnk.files.dedup.io.MalformedJsonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.ItemReadListener;
import org.springframework.batch.core.listener.ItemWriteListener;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;

import java.util.concurrent.atomic.AtomicLong;

public class ProgressLoggingListener implements ItemReadListener<RecordEnvelope>, ItemWriteListener<RecordEnvelope>, SkipListener<RecordEnvelope, RecordEnvelope> {

    private static final Logger log = LoggerFactory.getLogger(ProgressLoggingListener.class);

    private final long interval;
    private final AtomicLong readCounter = new AtomicLong();
    private final AtomicLong writeCounter = new AtomicLong();
    private final ExtendedStatsCollector extendedStatsCollector;

    public ProgressLoggingListener(long interval, ExtendedStatsCollector extendedStatsCollector) {
        this.interval = interval;
        this.extendedStatsCollector = extendedStatsCollector;
    }

    @Override
    public void afterRead(RecordEnvelope item) {
        long current = readCounter.incrementAndGet();
        if (current % interval == 0) {
            log.info("event=progress step={} partition={} readCount={} writeCount={}",
                    stepName(), partitionName(), current, writeCounter.get());
        }
    }

    @Override
    public void onReadError(Exception ex) {
        log.warn("event=read_error step={} partition={} errorType={} message={}",
                stepName(), partitionName(), ex.getClass().getSimpleName(), ex.getMessage());
    }

    @Override
    public void afterWrite(org.springframework.batch.infrastructure.item.Chunk<? extends RecordEnvelope> items) {
        writeCounter.addAndGet(items.size());
    }

    @Override
    public void onWriteError(Exception exception, org.springframework.batch.infrastructure.item.Chunk<? extends RecordEnvelope> items) {
        log.error("event=write_error step={} partition={} itemCount={} errorType={} message={}",
                stepName(), partitionName(), items.size(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
    }

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("event=skip_read step={} partition={} errorType={} message={}",
                stepName(), partitionName(), t.getClass().getSimpleName(), t.getMessage());

        if (t instanceof MalformedJsonException malformedJsonException) {
            extendedStatsCollector.addRejected(
                    malformedJsonException.getLineNumber(),
                    malformedJsonException.getFile().toString(),
                    "invalid data/format"
            );
        }
    }

    @Override
    public void onSkipInWrite(RecordEnvelope item, Throwable t) {
        log.warn("event=skip_write step={} partition={} file={} line={} errorType={} message={}",
                stepName(), partitionName(), item.fileDescriptor().path(), item.lineNumber(), t.getClass().getSimpleName(), t.getMessage());
        extendedStatsCollector.addRejected(item.lineNumber(), item.fileDescriptor().path().toString(), "invalid data/format");
    }

    @Override
    public void onSkipInProcess(RecordEnvelope item, Throwable t) {
        log.warn("event=skip_process step={} partition={} file={} line={} errorType={} message={}",
                stepName(), partitionName(), item.fileDescriptor().path(), item.lineNumber(), t.getClass().getSimpleName(), t.getMessage());
        extendedStatsCollector.addRejected(item.lineNumber(), item.fileDescriptor().path().toString(), "invalid data/format");
    }

    private String stepName() {
        return StepSynchronizationManager.getContext() == null ? "n/a" : StepSynchronizationManager.getContext().getStepExecution().getStepName();
    }

    private String partitionName() {
        return stepName();
    }
}
