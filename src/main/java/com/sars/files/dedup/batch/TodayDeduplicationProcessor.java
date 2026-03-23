package com.sars.files.dedup.batch;

import com.sars.files.dedup.domain.RecordEnvelope;
import com.sars.files.dedup.domain.SeenRecord;
import com.sars.files.dedup.logging.ExtendedStatsCollector;
import com.sars.files.dedup.service.UniquenessKeyService;
import com.sars.files.dedup.store.RocksDbKeyStore;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class TodayDeduplicationProcessor implements ItemProcessor<RecordEnvelope, RecordEnvelope> {

    private final UniquenessKeyService uniquenessKeyService;
    private final RocksDbKeyStore keyStore;
    private final ExtendedStatsCollector extendedStatsCollector;

    public TodayDeduplicationProcessor(UniquenessKeyService uniquenessKeyService,
                                       RocksDbKeyStore keyStore,
                                       ExtendedStatsCollector extendedStatsCollector) {
        this.uniquenessKeyService = uniquenessKeyService;
        this.keyStore = keyStore;
        this.extendedStatsCollector = extendedStatsCollector;
    }

    @Override
    public RecordEnvelope process(RecordEnvelope item) {
        byte[] key = uniquenessKeyService.key(item);
        byte[] existing = keyStore.putIfAbsent(
                key,
                new SeenRecord(item.fileDescriptor().path().toString(), item.lineNumber()).encode()
        );

        if (existing == null) {
            return item;
        }

        SeenRecord firstSeen = SeenRecord.decode(existing);
        extendedStatsCollector.addDuplicate(
                item.lineNumber(),
                item.fileDescriptor().path().toString(),
                firstSeen
        );
        return null;
    }
}