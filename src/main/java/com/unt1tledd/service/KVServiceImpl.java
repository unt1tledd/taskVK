package com.unt1tledd.service;

import com.unt1tledd.entity.KVEntry;
import com.unt1tledd.exceptions.RepositoryException;
import com.unt1tledd.exceptions.ServiceException;
import com.unt1tledd.repository.KVRepository;

import java.util.Optional;
import java.util.stream.Stream;

public class KVServiceImpl implements KVService {
    private final KVRepository kvrepo;

    public KVServiceImpl(KVRepository kvrepo) {
        this.kvrepo = kvrepo;
    }

    @Override
    public void put(String key, byte[] value) throws IllegalArgumentException, ServiceException {
        if (!isValidKey(key)) {
            throw new IllegalArgumentException("key is empty");
        }
        try {
            kvrepo.put(key, value);
        } catch (RepositoryException e) {
            throw new ServiceException(
                    String.format("Failed to put with key=%s valueLength=%s", key,
                            (value == null ? "null" : value.length + " bytes")), e);
        }
    }

    @Override
    public byte[] get(String key) throws IllegalArgumentException {
        if (!isValidKey(key)) {
            throw new IllegalArgumentException("key is empty");
        }

        try {
            Optional<byte[]> value = kvrepo.get(key);
            return value.orElse(null);
        } catch (RepositoryException e) {
            throw new ServiceException(String.format("Failed to get with key=%s", key), e);
        }
    }

    @Override
    public void delete(String key) {
        if (!isValidKey(key)) {
            throw new IllegalArgumentException("key is empty");
        }

        try {
            kvrepo.delete(key);
        } catch (RepositoryException e) {
            throw new ServiceException(String.format("Failed to delete with key=%s", key), e);
        }
    }

    @Override
    public Stream<KVEntry> range(String keySince, String keyTo) throws IllegalArgumentException {
        if (!isValidKey(keySince)) {
            throw new IllegalArgumentException("keySince is empty");
        }

        if (!isValidKey(keyTo)) {
            throw new IllegalArgumentException("keyTo is empty");
        }

        if (kvrepo.get(keySince).isEmpty()) {
            throw new ServiceException("Start key '" + keySince + "' not found");
        }

        if (kvrepo.get(keyTo).isEmpty()) {
            throw new ServiceException("End key '" + keyTo + "' not found");
        }

        try {
            return kvrepo.getRange(keySince, keyTo);
        } catch (RepositoryException e) {
            throw new ServiceException(
                    String.format("Failed to range with keySince=%s, keyTo=%s", keySince, keyTo), e);
        }
    }

    @Override
    public long count() throws IllegalArgumentException{
        try {
            return kvrepo.count();
        } catch (RepositoryException e) {
            throw new ServiceException("Failed to count", e);
        }
    }

    private boolean isValidKey(String key) {
        return key != null && !key.isEmpty();
    }
}