package com.sars.files.dedup.batch;

import com.sars.files.dedup.domain.RecordEnvelope;
import com.sars.files.dedup.domain.SeenRecord;
import com.sars.files.dedup.service.UniquenessKeyService;
import com.sars.files.dedup.store.RocksDbKeyStore;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class PriorDayIndexingProcessor implements ItemProcessor<RecordEnvelope, RecordEnvelope> {

    private final UniquenessKeyService uniquenessKeyService;
    private final RocksDbKeyStore keyStore;

    public PriorDayIndexingProcessor(UniquenessKeyService uniquenessKeyService, RocksDbKeyStore keyStore) {
        this.uniquenessKeyService = uniquenessKeyService;
        this.keyStore = keyStore;
    }

    @Override
    public RecordEnvelope process(RecordEnvelope item) {
        keyStore.putIfAbsent(
                uniquenessKeyService.key(item),
                new SeenRecord(item.fileDescriptor().path().toString(), item.lineNumber()).encode()
        );
        return item;
    }
}
