package com.unt1tledd.repository;

import com.unt1tledd.entity.KVEntry;
import io.tarantool.client.TarantoolClient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchingTarantoolIterable implements Iterable<KVEntry> {
    private final String keyTo;
    private String currentKey;
    private final TarantoolClient client;
    private final int BATCH_SIZE = 1000;

    public BatchingTarantoolIterable(String keySince, String keyTo, TarantoolClient client) {
        this.currentKey = keySince;
        this.keyTo = keyTo;
        this.client = client;
    }

    @Override
    public Iterator<KVEntry> iterator() {
        return new Iterator<>() {
            private List<List<?>> currentBatch = new ArrayList<>();
            private int index = 0;
            private boolean terminalReached = false;

            @Override
            public boolean hasNext() {
                if (index >= currentBatch.size() && !terminalReached) {
                    loadNextBatch();
                }
                return index < currentBatch.size();
            }

            @Override
            public KVEntry next() {
                List<?> tuple = currentBatch.get(index++);
                return new KVEntry((String) tuple.get(0), (byte[]) tuple.get(1));
            }

            private void loadNextBatch() {
                String lua = "return box.space.KV.index.primary:select(..., {iterator = 'GE', limit = " + BATCH_SIZE + "})";

                try {
                    var response = client.eval(lua, List.of(currentKey)).join();
                    List<?> result = response.get();

                    if (result == null || result.isEmpty()) {
                        terminalReached = true;
                        return;
                    }

                    Object firstResult = result.get(0);
                    if (!(firstResult instanceof List)) {
                        terminalReached = true;
                        return;
                    }

                    @SuppressWarnings("unchecked")
                    List<List<?>> batch = (List<List<?>>) firstResult;

                    currentBatch = new ArrayList<>();
                    for (List<?> tuple : batch) {
                        String key = tuple.get(0).toString();

                        if (key.compareTo(keyTo) > 0) {
                            terminalReached = true;
                            break;
                        }

                        if (key.equals(currentKey) && !currentBatch.isEmpty()) continue;

                        currentBatch.add(tuple);
                        currentKey = key;
                    }

                    index = 0;
                    if (currentBatch.isEmpty() || batch.size() < BATCH_SIZE) {
                        terminalReached = true;
                    } else {
                        currentKey = currentKey + "\0";
                    }
                } catch (Exception e) {
                    terminalReached = true;
                }
            }
        };
    }
}