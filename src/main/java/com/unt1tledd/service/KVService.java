package com.unt1tledd.service;

import com.unt1tledd.entity.KVEntry;

import java.util.stream.Stream;

public interface KVService {
    void put(String key, byte[] value) throws IllegalArgumentException;
    byte[] get(String key) throws IllegalArgumentException;
    void delete(String key) throws IllegalArgumentException;
    Stream<KVEntry> range(String keySince, String keyTo) throws IllegalArgumentException;
    long count();
}