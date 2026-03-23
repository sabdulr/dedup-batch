package com.bnk.files.dedup.batch;

import com.bnk.files.dedup.domain.RecordEnvelope;
import com.bnk.files.dedup.domain.SeenRecord;
import com.bnk.files.dedup.service.UniquenessKeyService;
import com.bnk.files.dedup.store.RocksDbKeyStore;
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
