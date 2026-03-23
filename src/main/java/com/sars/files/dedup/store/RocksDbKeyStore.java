package com.sars.files.dedup.store;

import com.sars.files.dedup.config.AppProperties;
import jakarta.annotation.PreDestroy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Component
public class RocksDbKeyStore {

    private static final Logger log = LoggerFactory.getLogger(RocksDbKeyStore.class);

    static {
        RocksDB.loadLibrary();
    }

    private final AppProperties properties;

    private DBOptions dbOptions;
    private ColumnFamilyOptions columnFamilyOptions;
    private RocksDB db;
    private ColumnFamilyHandle defaultHandle;
    private String currentTable;

    public RocksDbKeyStore(AppProperties properties) {
        this.properties = properties;
    }

    public synchronized boolean putIfAbsent(byte[] key) {
        return putIfAbsent(key, new byte[]{1}) == null;
    }

    public synchronized byte[] putIfAbsent(byte[] key, byte[] value) {
        ensureOpen();
        try {
            byte[] existing = db.get(defaultHandle, key);
            if (existing != null) {
                return existing;
            }
            db.put(defaultHandle, key, value);
            return null;
        } catch (RocksDBException e) {
            throw new IllegalStateException("Failed to put key into RocksDB", e);
        }
    }

    public synchronized byte[] get(byte[] key) {
        ensureOpen();
        try {
            return db.get(defaultHandle, key);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Failed to read key from RocksDB", e);
        }
    }

    public synchronized boolean contains(byte[] key) {
        return get(key) != null;
    }

    public synchronized void clearAndReopen() {
        String table = resolveCurrentTable();
        close();
        Path targetDir = rocksDbDirFor(table);
        try {
            if (Files.exists(targetDir)) {
                try (var stream = Files.walk(targetDir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    throw new IllegalStateException("Failed deleting RocksDB path " + path, e);
                                }
                            });
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed clearing RocksDB dir " + targetDir, e);
        }
    }

    private synchronized void ensureOpen() {
        String table = resolveCurrentTable();
        if (db != null && table.equals(currentTable)) {
            return;
        }

        close();
        Path dbDir = rocksDbDirFor(table);

        try {
            Files.createDirectories(dbDir);

            dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

            columnFamilyOptions = new ColumnFamilyOptions();

            List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions)
            );
            List<ColumnFamilyHandle> handles = new ArrayList<>();

            db = RocksDB.open(
                    dbOptions,
                    dbDir.toString(),
                    cfDescriptors,
                    handles
            );
            defaultHandle = handles.get(0);
            currentTable = table;

            log.info("event=rocksdb_open path={} table={}", dbDir, table);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open RocksDB at " + dbDir, e);
        }
    }

    private Path rocksDbDirFor(String table) {
        return properties.rocksDbDir(table);
    }

    private String resolveCurrentTable() {
        var context = StepSynchronizationManager.getContext();
        if (context != null) {
            var executionContext = context.getStepExecution().getExecutionContext();
            String table = executionContext.getString("table", null);
            if (table != null && !table.isBlank()) {
                return table;
            }
        }
        if (currentTable != null) {
            return currentTable;
        }
        throw new IllegalStateException("No current table available for RocksDB operation");
    }

    @PreDestroy
    public synchronized void close() {
        if (defaultHandle != null) {
            defaultHandle.close();
            defaultHandle = null;
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (columnFamilyOptions != null) {
            columnFamilyOptions.close();
            columnFamilyOptions = null;
        }
        if (dbOptions != null) {
            dbOptions.close();
            dbOptions = null;
        }
        currentTable = null;
    }
}
