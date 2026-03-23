package com.sars.files.dedup.batch;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;

public class NoOpItemWriter<T> implements ItemWriter<T> {
    @Override
    public void write(Chunk<? extends T> chunk) {
        // Intentionally empty. The processor mutates the disk-backed index.
    }
}
