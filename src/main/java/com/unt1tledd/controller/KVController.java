package com.unt1tledd.controller;

import api.kv.v1.KVServiceGrpc;
import api.kv.v1.Kvservice.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.unt1tledd.entity.KVEntry;
import com.unt1tledd.service.KVService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.Int64Value;

import java.util.stream.Stream;

public class KVController extends KVServiceGrpc.KVServiceImplBase {
    private final KVService kvs;

    public KVController(KVService kvs) {
        this.kvs = kvs;
    }

    @Override
    public void put(PutRequest request, StreamObserver<Empty> response) {
        try {
            byte[] value = request.getValue().isEmpty()
                    ? null
                    : request.getValue().toByteArray();
            kvs.put(request.getKey(), value);
            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("Error validation: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> response) {
        try {
            byte[] value = kvs.get(request.getKey());
            GetResponse.Builder respBuilder = GetResponse.newBuilder();
            if (value != null) {
                respBuilder.setValue(com.google.protobuf.ByteString.copyFrom(value)).setNotNull(true);
            } else {
                respBuilder.setNotNull(false);
            }
            response.onNext(respBuilder.build());
            response.onCompleted();
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("Error validation: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<Empty> response) {
        try {
            kvs.delete(request.getKey());
            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("Error validation: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> response) {
        try {
            Stream<KVEntry> kv = kvs.range(request.getKeySince(), request.getKeyTo());

            kv.forEach(el -> {
                RangeResponse.Builder builder = RangeResponse.newBuilder().setKey(el.key());
                if (el.value() != null) builder.setValue(ByteString.copyFrom(el.value()));
                response.onNext(builder.build());
            });

            response.onCompleted();
        } catch (IllegalArgumentException e) {
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription("Error validation: " + e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void count(Empty request, StreamObserver<CountResponse> response) {
        try {
            long count = kvs.count();
            CountResponse resp = CountResponse.newBuilder().setCount(Int64Value.of(count)).build();
            response.onNext(resp);
            response.onCompleted();
        } catch (Exception e) {
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}