package com.unt1tledd.repository;

import com.unt1tledd.entity.KVEntry;
import com.unt1tledd.exceptions.RepositoryException;
import io.tarantool.client.TarantoolClient;
import io.tarantool.pool.InstanceConnectionGroup;
import io.tarantool.client.factory.TarantoolFactory;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TarantoolRepository implements KVRepository {
    private final TarantoolClient client;
    private final String spaceName = "KV";

    public TarantoolRepository(String host, int port, String user, String password) {
        try {
            InstanceConnectionGroup conn = InstanceConnectionGroup.builder()
                    .withUser(user)
                    .withPassword(password)
                    .withHost(host)
                    .withPort(port)
                    .build();

            this.client = TarantoolFactory.box()
                    .withGroups(Collections.singletonList(conn))
                    .build();
        } catch (Exception e) {
            throw new RepositoryException("Failed to create client", e);
        }
    }

    @Override
    public void put(String key, byte[] value) {
        try {
            client.eval(
                    "return box.space." + spaceName + ":replace({...})",
                    Arrays.asList(key, value)
            ).join();
        } catch (Exception e) {
            throw new RepositoryException("Failed to put key: " + key, e);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        try {
            List<?> result = client.eval(
                    "local t = box.space." + spaceName + ":get(...) " +
                            "if t == nil then return nil end " +
                            "return t[1], t[2]",
                    Collections.singletonList(key),
                    Object.class
            ).join().get();

            if (result == null || result.isEmpty() || result.get(0) == null) {
                return Optional.empty();
            }

            if (result.size() > 1 && result.get(1) instanceof byte[]) {
                return Optional.of((byte[]) result.get(1));
            }
            return Optional.empty();
        } catch (Exception e) {
            throw new RepositoryException("Failed to get key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            client.eval(
                    "return box.space." + spaceName + ":delete(...)",
                    Collections.singletonList(key)
            ).join();
        } catch (Exception e) {
            throw new RepositoryException("Failed to delete key: " + key, e);
        }
    }

    @Override
    public long count() {
        try {
            List<?> result = client.eval(
                    "return box.space." + spaceName + ":count()",
                    Collections.emptyList(),
                    Object.class
            ).join().get();

            return result.isEmpty() ? 0 : ((Number) result.get(0)).longValue();
        } catch (Exception e) {
            throw new RepositoryException("Failed to count entries", e);
        }
    }

    @Override
    public Stream<KVEntry> getRange(String keySince, String keyTo) {
        try {
            return StreamSupport.stream(
                    new BatchingTarantoolIterable(keySince, keyTo, client).spliterator(),
                    false
            );
        } catch (Exception e) {
            throw new RepositoryException("Failed to get range", e);
        }
    }
}