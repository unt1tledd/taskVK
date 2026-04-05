package com.unt1tledd.repository;

import com.unt1tledd.entity.KVEntry;
import com.unt1tledd.exceptions.RepositoryException;

import java.util.Optional;
import java.util.stream.Stream;

public interface KVRepository {
    void put(String key, byte[] value) throws RepositoryException;
    Optional<byte[]> get(String key) throws RepositoryException;
    void delete(String key) throws RepositoryException;
    Stream<KVEntry> getRange(String keySince, String keyTo) throws RepositoryException;
    long count() throws RepositoryException;
}